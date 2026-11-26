use super::*;
use std::path::PathBuf;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("trivium-theme-test-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn profile(id: &str) -> String {
    format!("{{\"type\":\"trivium-theme\",\"version\":1,\"id\":\"{id}\",\"name\":\"紫\",\"base\":\"dark\",\"colors\":{{\"accent\":\"#aBc\"}}}}")
}

#[test]
fn profiles_keep_json_bytes_and_replace_only_the_managed_id() {
    let directory = Directory::new();
    let themes = directory.0.join("themes");
    let source = format!("  {}\n", profile("9-investigation"));
    save_profile(&themes, "9-investigation", &source).unwrap();
    let destination = themes.join("9-investigation.json");
    assert_eq!(fs::read_to_string(&destination).unwrap(), source);
    let replacement = source.replace("#aBc", "#112233");
    save_profile(&themes, "9-investigation", &replacement).unwrap();
    assert_eq!(fs::read_to_string(destination).unwrap(), replacement);
    assert_eq!(fs::read_dir(themes).unwrap().count(), 1);
}

#[test]
fn invalid_ids_documents_and_oversized_json_never_change_existing_profiles() {
    let directory = Directory::new();
    let original = profile("saved");
    save_profile(&directory.0, "saved", &original).unwrap();
    for id in [
        "",
        "../saved",
        "a/b",
        "a\\b",
        "UPPER",
        "_hidden",
        "con",
        "com1",
        "lpt9",
        "constructor",
        "prototype",
    ] {
        assert!(
            save_profile(&directory.0, id, &profile(id)).is_err(),
            "{id}"
        );
    }
    for text in [
        "null".to_owned(),
        "{}".to_owned(),
        profile("other"),
        profile("saved").replace("\"version\":1", "\"version\":2"),
        format!("{}{}", profile("saved"), " ".repeat(MAX_THEME_BYTES)),
    ] {
        assert!(save_profile(&directory.0, "saved", &text).is_err());
        assert_eq!(
            fs::read_to_string(directory.0.join("saved.json")).unwrap(),
            original
        );
    }
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
}

#[test]
fn catalog_limit_allows_replacement_but_not_another_profile() {
    let directory = Directory::new();
    for index in 0..MAX_USER_THEMES {
        let id = format!("theme-{index}");
        save_profile(&directory.0, &id, &profile(&id)).unwrap();
    }
    assert!(save_profile(&directory.0, "overflow", &profile("overflow")).is_err());
    assert!(!directory.0.join("overflow.json").exists());
    save_profile(&directory.0, "theme-0", &profile("theme-0")).unwrap();
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), MAX_USER_THEMES);
}

#[test]
fn a_directory_cannot_be_replaced_by_a_theme() {
    let directory = Directory::new();
    fs::create_dir(directory.0.join("saved.json")).unwrap();
    assert!(save_profile(&directory.0, "saved", &profile("saved")).is_err());
    assert!(directory.0.join("saved.json").is_dir());
}

#[cfg(unix)]
#[test]
fn theme_symlinks_and_redirected_catalogs_cannot_overwrite_external_files() {
    use std::os::unix::fs::symlink;
    let directory = Directory::new();
    let themes = directory.0.join("themes");
    fs::create_dir(&themes).unwrap();
    let external = directory.0.join("external.json");
    fs::write(&external, "private original").unwrap();
    symlink(&external, themes.join("saved.json")).unwrap();
    assert!(save_profile(&themes, "saved", &profile("saved")).is_err());
    let linked = directory.0.join("linked");
    symlink(&themes, &linked).unwrap();
    assert!(save_profile(&linked, "other", &profile("other")).is_err());
    assert_eq!(fs::read_to_string(external).unwrap(), "private original");
    assert!(!themes.join("other.json").exists());
}
