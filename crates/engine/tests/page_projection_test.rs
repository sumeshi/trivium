//! Requested display cells do not change filtering or persist view preferences.
use serde_json::json;
use trivium_engine::{jobs::JobContext, ops, store::ProjectsStore};

#[test]
fn explicit_projection_reveals_saved_hidden_columns_without_changing_query_or_storage() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source.csv");
    let bytes = b"Event,Status\nfirst,keep\nsecond,skip\nthird,keep\n";
    std::fs::write(&source, bytes).unwrap();
    let store = ProjectsStore::new(root.path().join("cases")).unwrap();
    let meta = ops::projects::create_project(
        &store,
        serde_json::from_value(json!({"path":source})).unwrap(),
    )
    .unwrap()
    .meta;
    let event = meta.columns[0].id.clone();
    let status = meta.columns[1].id.clone();
    ops::flags::set_hidden_columns(
        &store,
        serde_json::from_value(json!({"projectId":meta.id,"hidden_columns":[status]})).unwrap(),
    )
    .unwrap();
    let payload =
        || serde_json::from_value(json!({"projectId":meta.id,"search":"Status:keep"})).unwrap();
    let saved = ops::rows::query_project_rows(&store, payload()).unwrap();
    assert!(saved
        .rows
        .iter()
        .all(|row| !row.cells.contains_key(&status)));
    for projection in [
        vec![event.clone(), status.clone()],
        vec![event.clone()],
        vec![status.clone()],
        vec![],
    ] {
        let page = ops::rows::query_project_rows_with_projection(
            &store,
            payload(),
            Some(&projection),
            &JobContext::none(),
        )
        .unwrap();
        assert_eq!((page.total_rows, page.total_filtered_rows), (3, 2));
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>(),
            [0, 2]
        );
        for row in &page.rows {
            assert_eq!(row.cells.len(), projection.len());
            for column in &projection {
                assert!(row.cells.contains_key(column));
            }
            if projection.contains(&status) {
                assert_eq!(row.cells[&status].text.as_deref(), Some("keep"));
            }
        }
        assert_eq!(
            store
                .try_find(&meta.id)
                .unwrap()
                .unwrap()
                .hidden_columns
                .as_slice(),
            std::slice::from_ref(&status)
        );
    }
    // Output selection also remains independent of the display projection.
    let destination = root.path().join("filtered.csv");
    ops::export::export_project(&store, serde_json::from_value(json!({"projectId":meta.id,"search":"Status:keep","destination":destination,"columns":[status],"includeAnnotations":false})).unwrap()).unwrap();
    let output = csv::Reader::from_path(destination)
        .unwrap()
        .records()
        .map(|row| row.unwrap()[0].to_owned())
        .collect::<Vec<_>>();
    assert_eq!(output, ["keep", "keep"]);
    for projection in [
        vec!["unknown".to_string()],
        vec![event.clone(), event],
        vec!["__original".to_string()],
    ] {
        assert!(ops::rows::query_project_rows_with_projection(
            &store,
            payload(),
            Some(&projection),
            &JobContext::none()
        )
        .is_err());
    }
    assert_eq!(std::fs::read(source).unwrap(), bytes);
}
