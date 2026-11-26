//! Reports share query membership and retain complete source-cell values.
use std::{fs, path::Path};

use trivium_engine::{
    ops::{
        flags::{update_flag, UpdateFlagPayload},
        projects::{create_project, CreateProjectPayload},
        report::{build_report, BuildReportPayload},
    },
    store::ProjectsStore,
};
use uuid::Uuid;

fn payload(project_id: Uuid, destination: &Path) -> BuildReportPayload {
    BuildReportPayload {
        project_id,
        search: None,
        columns: None,
        search_columns: None,
        flag_filter: None,
        bookmarked_only: None,
        sort_key: None,
        sort_direction: None,
        condition: None,
        destination: destination.to_string_lossy().into(),
        allow_overwrite: false,
    }
}

fn annotate(store: &ProjectsStore, project_id: Uuid, row_index: usize) {
    update_flag(
        store,
        UpdateFlagPayload {
            project_id,
            row_index,
            flag: Some("critical".into()),
            memo: Some(Some("Evidence <alice@example.org>".into())),
            tags: None,
            bookmarked: None,
        },
    )
    .unwrap();
}

#[test]
fn filtered_report_statistics_cover_matching_rows_including_unannotated_matches() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    let source = root.path().join("scope.csv");
    let bytes = "scope,value\nkeep,alpha\nkeep,beta\ndrop,excluded-secret\n";
    fs::write(&source, bytes).unwrap();
    let imported = create_project(
        &store,
        CreateProjectPayload {
            import_options: None,
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap();
    let id = imported.meta.id;
    annotate(&store, id, 0);
    annotate(&store, id, 2);
    for (search, hits, expected_values) in [
        ("c000001:keep", 2, vec!["alpha", "beta"]),
        ("absent", 0, vec![]),
    ] {
        let destination = root.path().join(format!("{hits}.html"));
        let mut request = payload(id, &destination);
        request.search = Some(search.into());
        request.columns = Some(vec!["c000002".into()]);
        request.search_columns = Some(vec!["c000001".into()]);
        let result = build_report(&store, request).unwrap();
        assert_eq!(result.hit_rows, hits);
        assert_eq!(result.flagged_in_matches, usize::from(hits > 0));
        let html = fs::read_to_string(&destination).unwrap();
        let stats = html.split("<section class=\"stats\">").nth(1).unwrap();
        assert!(!stats.contains("excluded-secret"));
        assert!(stats.contains(&format!(
            "<tr data-column=\"c000002\"><td>c000002</td><td>{}</td><td>0</td><td>{hits}</td>",
            if hits == 0 { "null" } else { "string" }
        )));
        for value in expected_values {
            assert!(stats.contains(&format!("<tr><td>{value}</td><td>1</td></tr>")));
        }
    }
    assert_eq!(fs::read_to_string(source).unwrap(), bytes);
}

#[test]
fn sorted_report_rows_keep_full_unicode_and_escape_source_fields_and_notes() {
    let root = tempfile::tempdir().unwrap();
    let store = ProjectsStore::new(root.path().join("workspace")).unwrap();
    let source = root.path().join("values.csv");
    let text = format!(
        "{}<CommandLine>whoami & \"test\"</CommandLine>END_OF_FIELD",
        "雪😀".repeat(300)
    );
    let mut writer = csv::Writer::from_path(&source).unwrap();
    writer.write_record(["order", "value"]).unwrap();
    writer.write_record(["1", &text]).unwrap();
    writer.write_record(["2", "second-row"]).unwrap();
    writer.flush().unwrap();
    drop(writer);
    let original = fs::read(&source).unwrap();
    let imported = create_project(
        &store,
        CreateProjectPayload {
            import_options: None,
            path: source.to_string_lossy().into(),
            description: None,
            delimiter: None,
            restore_annotations: None,
        },
    )
    .unwrap();
    annotate(&store, imported.meta.id, 0);
    annotate(&store, imported.meta.id, 1);
    let destination = root.path().join("full.html");
    let mut request = payload(imported.meta.id, &destination);
    request.sort_key = Some("c000001".into());
    request.sort_direction = Some("desc".into());
    let result = build_report(&store, request).unwrap();
    assert_eq!(result.flagged_in_matches, 2);
    let html = fs::read_to_string(&destination).unwrap();
    let annotated = html
        .split("<section class=\"annotated\">")
        .nth(1)
        .unwrap()
        .split("</section>")
        .next()
        .unwrap();
    assert_eq!(annotated.matches("雪😀").count(), 300);
    assert!(annotated.contains(
        "&lt;CommandLine&gt;whoami &amp; &quot;test&quot;&lt;/CommandLine&gt;END_OF_FIELD"
    ));
    assert!(annotated.contains("Evidence &lt;alice@example.org&gt;"));
    assert!(annotated.find("second-row").unwrap() < annotated.find("雪😀").unwrap());
    assert!(!annotated.contains("<CommandLine>"));
    assert_eq!(fs::read(source).unwrap(), original);
}
