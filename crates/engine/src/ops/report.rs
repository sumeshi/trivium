use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::analysis::stats::{summarize_series, ColumnStats};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::{FlagEntry, ProjectMeta, ProjectRow};
use crate::project_io::{read_project_dataframe, ProjectColumns};
use crate::storage::load_flags;
use crate::store::ProjectsStore;

use super::query::{data_columns, materialize_page, resolve_target, user_flags, QuerySpec};
use super::util::resolve_columns;

#[derive(Debug, Deserialize)]
pub struct BuildReportPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    #[serde(default)]
    pub search: Option<String>,
    /// Display columns. For old callers this also remains the search scope
    /// when `searchColumns` is omitted.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "searchColumns")]
    pub search_columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default, rename = "bookmarkedOnly")]
    pub bookmarked_only: Option<bool>,
    #[serde(default, rename = "sortKey")]
    pub sort_key: Option<String>,
    #[serde(default, rename = "sortDirection")]
    pub sort_direction: Option<String>,
    #[serde(default)]
    pub condition: Option<crate::condition::Node>,
    pub destination: String,
}

#[derive(Debug, Serialize)]
pub struct ReportMeta {
    pub total_rows: usize,
    /// Same value as `query_project_rows(...).total_filtered_rows`.
    pub hit_rows: usize,
    pub flagged_in_matches: usize,
}

/// Writes a static HTML report of the current filter results.
pub fn build_report(
    store: &ProjectsStore,
    payload: BuildReportPayload,
) -> Result<ReportMeta, AppError> {
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let all_columns = data_columns(&df, meta.storage_version);
    let flags = load_flags(&store.flags_path(&meta.id)).map_err(AppError::from)?;

    let display_columns = match payload.columns.as_deref() {
        Some(requested) => resolve_columns(&df, requested, "display columns")?,
        None => all_columns.clone(),
    };

    let final_flag_vec = user_flags(df.height(), &flags);
    let mut spec = QuerySpec {
        search: payload.search.clone(),
        search_columns: payload
            .search_columns
            .clone()
            .or_else(|| payload.columns.clone()),
        condition: payload.condition.clone(),
        flag_filter: payload.flag_filter.clone(),
        bookmarked_only: payload.bookmarked_only,
        sort_key: payload.sort_key.clone(),
        sort_direction: payload.sort_direction.clone(),
    };
    spec.resolve_search_labels(&meta.columns)?;
    let target = resolve_target(&df, &spec, &all_columns, &flags, &JobContext::none())?;
    let flagged_in_matches = target.annotated_filtered;
    let filtered = target.filtered;
    let hit_rows = filtered.len();

    let annotated_indices: Vec<usize> = filtered
        .iter()
        .copied()
        .filter(|idx| is_annotated(*idx, &final_flag_vec, &flags))
        .collect();
    let project_columns = ProjectColumns::from_meta(&meta, &df);
    let display_meta: Vec<_> = project_columns
        .columns()
        .iter()
        .filter(|column| display_columns.contains(&column.id))
        .cloned()
        .collect();
    let annotated_rows = materialize_page(
        &df,
        &annotated_indices,
        &display_meta,
        &flags,
        &[],
        false,
    )?;

    let stats: Vec<ColumnStats> = display_columns
        .iter()
        .filter_map(|column| {
            df.column(column)
                .ok()
                .map(|series| summarize_series(column, series, 1))
        })
        .collect();

    let html = render_report(
        &meta,
        payload.search.as_deref(),
        payload.condition.as_ref(),
        df.height(),
        hit_rows,
        flagged_in_matches,
        &display_columns,
        &annotated_rows,
        &stats,
    );
    write_report(&payload.destination, &html)?;

    Ok(ReportMeta {
        total_rows: df.height(),
        hit_rows,
        flagged_in_matches,
    })
}

fn is_annotated(
    idx: usize,
    final_flag_vec: &[String],
    flags: &HashMap<usize, FlagEntry>,
) -> bool {
    !final_flag_vec[idx].trim().is_empty()
        || flags.get(&idx).is_some_and(|entry| entry.is_annotated())
}

#[allow(clippy::too_many_arguments)]
fn render_report(
    meta: &ProjectMeta,
    search: Option<&str>,
    condition: Option<&crate::condition::Node>,
    total_rows: usize,
    hit_rows: usize,
    flagged_in_matches: usize,
    display_columns: &[String],
    annotated_rows: &[ProjectRow],
    stats: &[ColumnStats],
) -> String {
    let mut html = String::new();
    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    push_meta(&mut html, "trivium-version", "1");
    push_meta(&mut html, "trivium-total-rows", &total_rows.to_string());
    push_meta(&mut html, "trivium-hit-rows", &hit_rows.to_string());
    push_meta(
        &mut html,
        "trivium-flagged-in-matches",
        &flagged_in_matches.to_string(),
    );
    html.push_str("<title>Trivium Report</title>\n<style>");
    html.push_str(CSS);
    html.push_str("</style>\n</head>\n<body>\n");
    html.push_str(&format!(
        "<h1>Trivium Report {}</h1>\n",
        html_escape(&meta.name)
    ));

    html.push_str("<section class=\"meta\"><h2>Project</h2><ul>");
    push_stat_li(&mut html, "Total rows", &meta.total_records.to_string());
    push_stat_li(&mut html, "Annotated rows", &meta.flagged_records.to_string());
    push_stat_li(
        &mut html,
        "IOC applied rows",
        &meta.ioc_applied_records.to_string(),
    );
    html.push_str("</ul></section>\n");

    html.push_str("<section class=\"filter\"><h2>Filter</h2>");
    html.push_str(&format!(
        "<p><strong>Search:</strong> {}</p>\n",
        html_escape(search.unwrap_or(""))
    ));
    if let Some(condition) = condition {
        let json = serde_json::to_string_pretty(condition)
            .unwrap_or_else(|_| "<unserializable condition>".to_string());
        html.push_str(&format!("<pre>{}</pre>\n", html_escape(&json)));
    }
    html.push_str("</section>\n");

    html.push_str("<section class=\"counts\"><h2>Counts</h2><ul>");
    push_stat_li(&mut html, "Total rows", &total_rows.to_string());
    push_stat_li(&mut html, "Hit rows", &hit_rows.to_string());
    push_stat_li(
        &mut html,
        "Flagged in matches",
        &flagged_in_matches.to_string(),
    );
    html.push_str("</ul></section>\n");

    html.push_str("<section class=\"annotated\"><h2>Annotated rows</h2>\n");
    if annotated_rows.is_empty() {
        html.push_str("<p>No annotated rows in the current filter.</p>\n");
    } else {
        render_rows_table(&mut html, display_columns, annotated_rows);
    }
    html.push_str("</section>\n");

    html.push_str("<section class=\"stats\"><h2>Column stats</h2>\n");
    render_stats_table(&mut html, stats);
    html.push_str("</section>\n</body>\n</html>\n");
    html
}

fn push_meta(html: &mut String, name: &str, value: &str) {
    html.push_str(&format!(
        "<meta name=\"{}\" content=\"{}\">\n",
        name,
        html_escape(value)
    ));
}

fn push_stat_li(html: &mut String, label: &str, value: &str) {
    html.push_str(&format!(
        "<li>{}: {}</li>",
        html_escape(label),
        html_escape(value)
    ));
}

fn render_rows_table(html: &mut String, columns: &[String], rows: &[ProjectRow]) {
    html.push_str("<table><thead><tr><th>row</th>");
    for column in columns {
        html.push_str(&format!("<th>{}</th>", html_escape(column)));
    }
    html.push_str("<th>flag</th><th>memo</th></tr></thead><tbody>\n");
    for row in rows {
        html.push_str(&format!("<tr><td>{}</td>", row.row_index));
        for column in columns {
            let text = row
                .cells
                .get(column)
                .and_then(|cell| cell.text.as_deref())
                .unwrap_or_default();
            html.push_str(&format!("<td>{}</td>", html_escape(text)));
        }
        html.push_str(&format!(
            "<td class=\"flag\">{}</td><td class=\"memo\">{}</td></tr>\n",
            html_escape(&row.flag),
            html_escape(row.memo.as_deref().unwrap_or(""))
        ));
    }
    html.push_str("</tbody></table>\n");
}

fn render_stats_table(html: &mut String, stats: &[ColumnStats]) {
    html.push_str(
        "<table><thead><tr><th>column</th><th>kind</th><th>missing</th><th>unique</th>\
         <th>top values</th></tr></thead><tbody>\n",
    );
    for stat in stats {
        html.push_str(&format!(
            "<tr data-column=\"{}\"><td>{}</td><td>{}</td><td>{}</td><td>{}</td>",
            html_escape(&stat.column),
            html_escape(&stat.column),
            html_escape(&stat.kind),
            stat.missing,
            stat.unique
        ));
        html.push_str("<td><table>");
        for item in stat.top.iter().take(5) {
            html.push_str(&format!(
                "<tr><td>{}</td><td>{}</td></tr>",
                html_escape(&item.value),
                item.count
            ));
        }
        html.push_str("</table></td></tr>\n");
    }
    html.push_str("</tbody></table>\n");
}

fn write_report(destination: &str, html: &str) -> Result<(), AppError> {
    let path = PathBuf::from(destination);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create report output dir {:?}", parent))
                .map_err(AppError::from)?;
        }
    }
    fs::write(&path, html)
        .with_context(|| format!("failed to write report {}", destination))
        .map_err(AppError::from)
}

fn html_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

const CSS: &str = "body{font-family:system-ui,sans-serif;margin:2rem;color:#1f2933;background:#fff}\
h1{font-size:1.5rem}h2{font-size:1.1rem;margin-top:1.5rem}\
table{border-collapse:collapse;margin:0.5rem 0}\
th,td{border:1px solid #cbd2d9;padding:0.3rem 0.5rem;text-align:left;vertical-align:top}\
th{background:#f0f4f8}pre{background:#f0f4f8;padding:0.5rem;overflow:auto}\
.flag{font-weight:600}.memo{white-space:pre-wrap}";
