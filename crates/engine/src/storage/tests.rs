use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
};

use chrono::Utc;
use uuid::Uuid;

use crate::{
    annotation::AnnotationPatch,
    jobs::{self, JobContext, JobKind},
    models::{FlagEntry, ProjectMeta},
    store::ProjectsStore,
};

use super::*;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("trivium-sqlite-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
    fn flags(&self) -> PathBuf {
        self.0.join("flags.json")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn entry(flag: &str) -> FlagEntry {
    FlagEntry {
        flag: flag.into(),
        memo: Some("legacy note".into()),
        tags: vec!["host".into()],
        bookmarked: true,
    }
}
fn meta(id: Uuid) -> ProjectMeta {
    ProjectMeta {
        id,
        name: "legacy.csv".into(),
        description: None,
        created_at: Utc::now(),
        total_records: 1000,
        flagged_records: 0,
        ioc_applied_records: 0,
        source_sha256: None,
        source_file_name: None,
        source_format: None,
        source_size: None,
        source_path: None,
        hidden_columns: vec![],
        storage_version: 0,
        columns: vec![],
    }
}
fn patch(flag: &str) -> AnnotationPatch {
    AnnotationPatch {
        flag: Some(flag.into()),
        ..Default::default()
    }
}

mod migrations;
mod mutations;
