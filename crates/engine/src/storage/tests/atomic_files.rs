use super::*;

#[test]
fn partial_temporary_writes_preserve_previous_files_and_remove_unpublished_bytes() {
    let fixture = Fixture::new();
    let path = fixture.path().join("theme.json");
    let original = br##"{"accent":"#123456"}"##;
    fs::write(&path, original).unwrap();
    let fail_after_partial_write = |temporary: &Path, bytes: &[u8]| {
        fs::write(temporary, &bytes[..8])?;
        Err(std::io::Error::other("synthetic partial write"))
    };
    assert!(write_file_atomic_with(
        &path,
        br##"{"accent":"#654321"}"##,
        fail_after_partial_write
    )
    .is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 1);

    fs::remove_file(&path).unwrap();
    assert!(write_file_atomic_with(
        &path,
        br##"{"accent":"#654321"}"##,
        fail_after_partial_write
    )
    .is_err());
    assert!(!path.exists());
    assert_eq!(fs::read_dir(fixture.path()).unwrap().count(), 0);
}
