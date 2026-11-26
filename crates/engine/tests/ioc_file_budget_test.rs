//! Selected IOC CSVs must be bounded before they can replace saved rules.

use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
};

use sha2::{Digest, Sha256};
use trivium_engine::{
    error::AppError,
    ioc::{prepare_ioc_entries, save_ioc_entries},
    models::IocEntry,
    ops::{
        iocs::{import_iocs, save_iocs, ImportIocsPayload, SaveIocsPayload},
        projects::{create_project, CreateProjectPayload},
    },
    storage::load_setting_bytes,
    store::ProjectsStore,
};
use uuid::Uuid;

const LARGE_FIELD_BYTES: u64 = 5 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_RULES: usize = 10_000;
const EVIDENCE: &[u8] = b"command\nneedle\nother\n";

struct Fixture {
    root: tempfile::TempDir,
    store: ProjectsStore,
    id: Uuid,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let evidence = root.path().join("evidence.csv");
        fs::write(&evidence, EVIDENCE).unwrap();
        let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
        let id = create_project(
            &store,
            CreateProjectPayload {
                path: evidence.to_string_lossy().into(),
                description: None,
                delimiter: None,
                restore_annotations: None,
                import_options: None,
            },
        )
        .unwrap()
        .meta
        .id;
        save_iocs(
            &store,
            SaveIocsPayload {
                project_id: id,
                entries: vec![IocEntry {
                    flag: "critical".into(),
                    tag: "existing investigation".into(),
                    query: "other".into(),
                    cell: None,
                }],
            },
        )
        .unwrap();
        Self { root, store, id }
    }

    fn import(&self, path: &Path) -> Result<Vec<IocEntry>, AppError> {
        import_iocs(
            &self.store,
            ImportIocsPayload {
                project_id: self.id,
                path: path.to_string_lossy().into(),
            },
        )
    }

    fn saved(&self) -> Vec<u8> {
        load_setting_bytes(&self.store.project_dir(&self.id), "iocs")
            .unwrap()
            .unwrap()
    }

    fn import_error(&self, path: &Path) -> String {
        match self.import(path) {
            Err(error) => error.to_string(),
            Ok(entries) => panic!(
                "expected IOC import rejection, but imported {} rules",
                entries.len()
            ),
        }
    }

    fn assert_rejected_unchanged(&self, path: &Path, expected_error: &str) -> String {
        let saved = self.saved();
        let source = source_digest(path);
        let error = self.import_error(path);
        assert!(error.contains(expected_error), "{error}");
        assert_eq!(self.saved(), saved, "rejection replaced saved IOC rules");
        assert_eq!(source_digest(path), source, "selected IOC source changed");
        assert_eq!(
            fs::read(self.root.path().join("evidence.csv")).unwrap(),
            EVIDENCE,
            "original project evidence changed"
        );
        error
    }
}

// Hash the source with a fixed buffer instead of retaining another large copy.
fn source_digest(path: &Path) -> (u64, String) {
    let mut source = File::open(path).unwrap();
    let mut hash = Sha256::new();
    let mut buffer = [0; 8192];
    let mut bytes = 0;
    loop {
        let count = source.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        bytes += count as u64;
    }
    (bytes, format!("{:x}", hash.finalize()))
}

fn write_large_field(path: &Path, prefix: &[u8], suffix: &[u8]) {
    let mut source = File::create(path).unwrap();
    source.write_all(prefix).unwrap();
    io::copy(&mut io::repeat(b'A').take(LARGE_FIELD_BYTES), &mut source).unwrap();
    source.write_all(suffix).unwrap();
}

fn query_rule(query: &str) -> IocEntry {
    IocEntry {
        flag: "safe".into(),
        tag: "review".into(),
        query: query.into(),
        cell: None,
    }
}

fn assert_rule_limit(error: &str) {
    assert!(
        error.replace(',', "").contains("10000") && error.to_lowercase().contains("rule"),
        "{error}"
    );
}

#[test]
fn exactly_four_mib_are_accepted_and_one_extra_byte_is_rejected() {
    let f = Fixture::new();
    let source = f.root.path().join("maximum-bytes.csv");
    let prefix = b"flag,tag,query\nsafe,review,needle\n";
    let mut writer = File::create(&source).unwrap();
    writer.write_all(prefix).unwrap();
    // Padding with blank lines reaches the byte boundary without retaining a
    // giant field or consuming accepted-rule slots.
    io::copy(
        &mut io::repeat(b'\n').take(MAX_FILE_BYTES - prefix.len() as u64),
        &mut writer,
    )
    .unwrap();
    drop(writer);

    let before = source_digest(&source);
    assert_eq!(before.0, MAX_FILE_BYTES);
    let expected = vec![query_rule("needle")];
    assert_eq!(f.import(&source).unwrap(), expected);
    let saved: Vec<IocEntry> = serde_json::from_slice(&f.saved()).unwrap();
    assert_eq!(saved, expected);
    assert_eq!(source_digest(&source), before);
    assert_eq!(
        fs::read(f.root.path().join("evidence.csv")).unwrap(),
        EVIDENCE
    );

    let mut writer = fs::OpenOptions::new().append(true).open(&source).unwrap();
    writer.write_all(b"\n").unwrap();
    drop(writer);
    assert_eq!(source_digest(&source).0, MAX_FILE_BYTES + 1);
    f.assert_rejected_unchanged(&source, "4 MiB");
}

#[test]
fn oversized_single_tag_is_rejected_without_replacing_rules_or_changing_sources() {
    let f = Fixture::new();
    let source = f.root.path().join("large-tag.csv");
    write_large_field(&source, b"flag,tag,query\nsafe,\"", b"\",needle\n");
    f.assert_rejected_unchanged(&source, "4 MiB");
}

#[test]
fn oversized_header_is_rejected_before_it_can_replace_saved_rules() {
    let f = Fixture::new();
    let source = f.root.path().join("large-header.csv");
    // The legacy first three headers are positional, so this is otherwise a
    // valid input. Header parsing needs the same byte bound as data records.
    write_large_field(&source, b"", b",tag,query\nsafe,review,needle\n");
    f.assert_rejected_unchanged(&source, "4 MiB");
}

#[test]
fn more_than_ten_thousand_valid_rules_cannot_replace_the_existing_set() {
    let f = Fixture::new();
    let source = f.root.path().join("too-many-rules.csv");
    let mut writer = File::create(&source).unwrap();
    writer.write_all(b"flag,tag,query\n").unwrap();
    for index in 0..=MAX_RULES {
        writeln!(writer, "safe,rule-{index},needle").unwrap();
    }
    drop(writer);
    let saved = f.saved();
    let before = source_digest(&source);
    let error = f.import_error(&source);
    assert_rule_limit(&error);
    assert_eq!(f.saved(), saved);
    assert_eq!(source_digest(&source), before);
    assert_eq!(
        fs::read(f.root.path().join("evidence.csv")).unwrap(),
        EVIDENCE
    );
}

#[test]
fn public_save_rejects_more_than_ten_thousand_rules_without_replacing_saved_bytes() {
    let f = Fixture::new();
    let before = f.saved();
    let error = save_iocs(
        &f.store,
        SaveIocsPayload {
            project_id: f.id,
            entries: vec![query_rule("needle"); MAX_RULES + 1],
        },
    )
    .unwrap_err()
    .to_string();
    assert_rule_limit(&error);
    assert_eq!(f.saved(), before);
}

#[test]
fn direct_save_and_preparation_enforce_the_same_accepted_rule_limit() {
    let f = Fixture::new();
    let before = f.saved();
    let entries = vec![query_rule("needle"); MAX_RULES + 1];
    let error = save_ioc_entries(&f.store.project_dir(&f.id), &entries)
        .unwrap_err()
        .to_string();
    assert_rule_limit(&error);
    assert_eq!(f.saved(), before);

    let error = prepare_ioc_entries(entries).unwrap_err().to_string();
    assert_rule_limit(&error);
    assert_eq!(f.saved(), before);
}

#[test]
fn more_than_ten_thousand_skipped_queries_do_not_consume_accepted_rule_slots() {
    let f = Fixture::new();
    let source = f.root.path().join("skipped-rules.csv");
    let mut writer = File::create(&source).unwrap();
    writer.write_all(b"flag,tag,query\n").unwrap();
    for _ in 0..=MAX_RULES {
        writer.write_all(b"safe,,\n").unwrap();
    }
    writer.write_all(b"safe,review,needle\n").unwrap();
    drop(writer);
    let before = source_digest(&source);
    let expected = vec![query_rule("needle")];
    assert_eq!(f.import(&source).unwrap(), expected);
    assert_eq!(source_digest(&source), before);

    let mut entries = vec![query_rule(" \t "); MAX_RULES + 1];
    entries.push(query_rule("needle"));
    assert_eq!(prepare_ioc_entries(entries.clone()).unwrap(), expected);
    save_ioc_entries(&f.store.project_dir(&f.id), &entries).unwrap();
    let saved: Vec<IocEntry> = serde_json::from_slice(&f.saved()).unwrap();
    assert_eq!(saved, expected);
    save_iocs(
        &f.store,
        SaveIocsPayload {
            project_id: f.id,
            entries,
        },
    )
    .unwrap();
    let saved: Vec<IocEntry> = serde_json::from_slice(&f.saved()).unwrap();
    assert_eq!(saved, expected);
    assert_eq!(
        fs::read(f.root.path().join("evidence.csv")).unwrap(),
        EVIDENCE
    );
}

#[test]
fn exactly_ten_thousand_valid_rules_are_accepted_with_skipped_empty_query_rows() {
    let f = Fixture::new();
    let source = f.root.path().join("maximum-rules.csv");
    let mut writer = File::create(&source).unwrap();
    writer.write_all(b"flag,tag,query\n").unwrap();
    for index in 0..MAX_RULES {
        writeln!(writer, "safe,rule-{index},needle").unwrap();
        if index % 100 == 0 {
            // These rows do not produce a definition and must not use a slot.
            writer.write_all(b"safe,,\n").unwrap();
        }
    }
    drop(writer);
    let before = source_digest(&source);
    let entries = f.import(&source).unwrap();
    assert_eq!(entries.len(), MAX_RULES);
    assert!(entries
        .iter()
        .all(|entry| entry.flag == "safe" && entry.query == "needle"));
    let saved: Vec<IocEntry> = serde_json::from_slice(&f.saved()).unwrap();
    assert_eq!(saved.len(), MAX_RULES);
    assert_eq!(source_digest(&source), before);
    assert_eq!(
        fs::read(f.root.path().join("evidence.csv")).unwrap(),
        EVIDENCE
    );
}

#[test]
fn small_quoted_multiline_imports_and_invalid_flag_locations_keep_their_behavior() {
    let f = Fixture::new();
    let source = f.root.path().join("small-rules.csv");
    for newline in ["\n", "\r\n", "\r"] {
        fs::write(
            &source,
            format!(
                "flag,tag,query{newline} SAFE ,\"multi{newline}line\",needle{newline}?,second,other{newline}"
            ),
        )
        .unwrap();
        let before = source_digest(&source);
        let entries = f.import(&source).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].flag, "safe");
        assert_eq!(entries[0].tag, format!("multi{newline}line"));
        assert_eq!(entries[1].flag, "suspicious");
        assert_eq!(source_digest(&source), before);

        fs::write(
            &source,
            format!(
                "flag,tag,query{newline}safe,\"multi{newline}line\",needle{newline}critcal,bad,other{newline}"
            ),
        )
        .unwrap();
        let error = f.assert_rejected_unchanged(&source, "IOC CSV line 4:");
        assert!(error.contains("\"critcal\""), "{error}");
    }
}
