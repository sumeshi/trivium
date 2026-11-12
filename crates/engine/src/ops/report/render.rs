use crate::{
    analysis::stats::ColumnStats,
    models::{ProjectMeta, ProjectRow},
};
#[allow(clippy::too_many_arguments)]
pub(super) fn prefix(
    meta: &ProjectMeta,
    search: Option<&str>,
    condition: Option<&crate::condition::Node>,
    total_rows: usize,
    hit_rows: usize,
    flagged_in_matches: usize,
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
    push_stat_li(
        &mut html,
        "Annotated rows",
        &meta.flagged_records.to_string(),
    );
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
    html
}

pub(super) fn suffix(stats: &[ColumnStats]) -> String {
    let mut html = String::from("</section>\n<section class=\"stats\"><h2>Column stats</h2>\n");
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

pub(super) fn rows_header(columns: &[String]) -> String {
    let mut html = String::new();
    html.push_str("<table><thead><tr><th>row</th>");
    for column in columns {
        html.push_str(&format!("<th>{}</th>", html_escape(column)));
    }
    html.push_str("<th>flag</th><th>memo</th></tr></thead><tbody>\n");
    html
}

pub(super) fn row(columns: &[String], row: &ProjectRow) -> String {
    let mut html = String::new();
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
    html
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

const CSS: &str =
    "body{font-family:system-ui,sans-serif;margin:2rem;color:#1f2933;background:#fff}\
h1{font-size:1.5rem}h2{font-size:1.1rem;margin-top:1.5rem}\
table{border-collapse:collapse;margin:0.5rem 0}\
th,td{border:1px solid #cbd2d9;padding:0.3rem 0.5rem;text-align:left;vertical-align:top}\
th{background:#f0f4f8}pre{background:#f0f4f8;padding:0.5rem;overflow:auto}\
.flag{font-weight:600}.memo{white-space:pre-wrap}";
