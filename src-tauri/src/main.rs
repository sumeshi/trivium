#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows release builds
use std::{
    collections::{HashMap},
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use csv::{ReaderBuilder, WriterBuilder};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tauri::{Manager, State};
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_PAGE_SIZE: usize = 250;

#[derive(Debug, Error)]
enum AppError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<AppError> for String {
    fn from(err: AppError) -> Self {
        err.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectMeta {
    id: Uuid,
    name: String,
    description: Option<String>,
    created_at: DateTime<Utc>,
    total_records: usize,
    #[serde(default)]
    flagged_records: usize,
    #[serde(default)]
    ioc_applied_records: usize,
    hidden_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectSummary {
    meta: ProjectMeta,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectRow {
    row_index: usize,
    data: HashMap<String, Value>,
    flag: String,
    memo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IocEntry {
    flag: String,
    tag: String,
    query: String,
}

#[derive(Debug, Clone, Serialize)]
struct LoadProjectResponse {
    project: ProjectSummary,
    columns: Vec<String>,
    hidden_columns: Vec<String>,
    column_max_chars: HashMap<String, usize>,
    iocs: Vec<IocEntry>,
    initial_rows: Vec<ProjectRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FlagEntry {
    flag: String,
    memo: Option<String>,
}

#[derive(Debug)]
struct ProjectsStore {
    root_dir: PathBuf,
    meta_path: PathBuf,
    inner: Mutex<Vec<ProjectMeta>>,
}

impl ProjectsStore {
    fn new(root_dir: PathBuf) -> Result<Self> {
        let projects_dir = root_dir.join("projects");
        fs::create_dir_all(&projects_dir)
            .with_context(|| format!("failed to create projects dir at {:?}", projects_dir))?;

        let meta_path = root_dir.join("projects.json");
        let mut projects: Vec<ProjectMeta> = if meta_path.exists() {
            let data = fs::read(&meta_path)
                .with_context(|| format!("failed to read metadata file {:?}", meta_path))?;
            serde_json::from_slice(&data)
                .with_context(|| format!("failed to parse metadata file {:?}", meta_path))?
        } else {
            Vec::new()
        };

        // Migrate existing project data (calculate if flagged_records is 0)
        let mut needs_save = false;
        for project in &mut projects {
            if project.flagged_records == 0 {
                let project_dir = root_dir.join("projects").join(project.id.to_string());
                let flags_path = project_dir.join("flags.json");
                if flags_path.exists() {
                    if let Ok(flags) = load_flags(&flags_path) {
                        project.flagged_records = flags
                            .values()
                            .filter(|entry| !entry.flag.trim().is_empty())
                            .count();
                        needs_save = true;
                    }
                }
            }
        }
        
        // Save migrated data
        if needs_save {
            let data = serde_json::to_vec_pretty(&projects)
                .with_context(|| format!("failed to serialize metadata to {:?}", meta_path))?;
            fs::write(&meta_path, data)
                .with_context(|| format!("failed to write metadata file {:?}", meta_path))?;
        }

        Ok(Self {
            root_dir,
            meta_path,
            inner: Mutex::new(projects),
        })
    }

    fn all(&self) -> Vec<ProjectMeta> {
        self.inner.lock().clone()
    }

    fn insert(&self, project: ProjectMeta) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.push(project);
        self.persist_locked(&guard)
    }

    fn update_hidden_columns(&self, id: &Uuid, hidden_columns: Vec<String>) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(meta) = guard.iter_mut().find(|meta| &meta.id == id) {
            meta.hidden_columns = hidden_columns;
        }
        self.persist_locked(&guard)
    }

    fn update_flagged_records(&self, id: &Uuid, flagged_records: usize) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(meta) = guard.iter_mut().find(|meta| &meta.id == id) {
            meta.flagged_records = flagged_records;
        }
        self.persist_locked(&guard)
    }

    fn update_ioc_applied_records(&self, id: &Uuid, ioc_applied_records: usize) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(meta) = guard.iter_mut().find(|meta| &meta.id == id) {
            meta.ioc_applied_records = ioc_applied_records;
        }
        self.persist_locked(&guard)
    }

    fn remove(&self, id: &Uuid) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.retain(|meta| &meta.id != id);
        self.persist_locked(&guard)
    }

    fn find(&self, id: &Uuid) -> Option<ProjectMeta> {
        self.inner.lock().iter().find(|meta| &meta.id == id).cloned()
    }

    fn persist_locked(&self, guard: &[ProjectMeta]) -> Result<()> {
        let data = serde_json::to_vec_pretty(guard)?;
        fs::write(&self.meta_path, data)
            .with_context(|| format!("failed to write metadata file {:?}", self.meta_path))
    }

    fn project_dir(&self, id: &Uuid) -> PathBuf {
        self.root_dir.join("projects").join(id.to_string())
    }
}

struct AppState {
    projects: ProjectsStore,
    // lightweight caches per project to avoid recomputation
    searchable_cache: Mutex<HashMap<Uuid, Vec<String>>>,
    ioc_flag_cache: Mutex<HashMap<Uuid, Vec<String>>>,
}

impl AppState {
    fn new(app: &tauri::App<tauri::Wry>) -> Result<Self> {
        let base_dir = tauri::api::path::app_local_data_dir(&app.config())
            .context("failed to resolve app data dir")?
            .join("trivium");
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("failed to create app data dir {:?}", base_dir))?;
        let projects = ProjectsStore::new(base_dir)?;
        Ok(Self {
            projects,
            searchable_cache: Mutex::new(HashMap::new()),
            ioc_flag_cache: Mutex::new(HashMap::new()),
        })
    }
}

fn load_flags(path: &Path) -> Result<HashMap<usize, FlagEntry>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let data = fs::read(path)
        .with_context(|| format!("failed to read flags file {:?}", path))?;
    let map: HashMap<usize, FlagEntry> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse flags file {:?}", path))?;
    Ok(map)
}

fn save_flags(path: &Path, flags: &HashMap<usize, FlagEntry>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create flag dir {:?}", parent))?;
    }
    let data = serde_json::to_vec_pretty(flags)?;
    fs::write(path, data)
        .with_context(|| format!("failed to write flags file {:?}", path))
}

fn anyvalue_to_json(value: &AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(v) => Value::Bool(*v),
        AnyValue::Int8(v) => Value::from(*v),
        AnyValue::Int16(v) => Value::from(*v),
        AnyValue::Int32(v) => Value::from(*v),
        AnyValue::Int64(v) => Value::from(*v),
        AnyValue::UInt8(v) => Value::from(*v),
        AnyValue::UInt16(v) => Value::from(*v),
        AnyValue::UInt32(v) => Value::from(*v),
        AnyValue::UInt64(v) => Value::from(*v),
        AnyValue::Float32(v) => Value::from(f64::from(*v)),
        AnyValue::Float64(v) => Value::from(*v),
        AnyValue::String(v) => Value::String(v.to_string()),
        AnyValue::Date(v) => Value::String(v.to_string()),
        AnyValue::Datetime(v, _, _) => Value::String(v.to_string()),
        AnyValue::Time(v) => Value::String(v.to_string()),
        AnyValue::List(series) => {
            let values: Vec<Value> = series.iter().map(|v| anyvalue_to_json(&v)).collect();
            Value::Array(values)
        }
        other => Value::String(other.to_string()),
    }
}

fn value_display_length(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::String(text) => text.chars().count(),
        Value::Number(number) => number.to_string().chars().count(),
        Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value)
            .map(|text| text.chars().count())
            .unwrap_or(0),
    }
}

fn normalize_flag_value(flag: &str) -> String {
    let trimmed = flag.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    match trimmed.to_lowercase().as_str() {
        "safe" => "safe".to_string(),
        "suspicious" => "suspicious".to_string(),
        "critical" => "critical".to_string(),
        "◯" => "safe".to_string(),
        "?" => "suspicious".to_string(),
        "✗" => "critical".to_string(),
        _ => String::new(),
    }
}

fn severity_rank(value: &str) -> u8 {
    match value {
        "critical" => 3,
        "suspicious" => 2,
        "safe" => 1,
        _ => 0,
    }
}

fn value_to_search_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).ok(),
    }
}

fn anyvalue_to_search_string(value: &AnyValue) -> Option<String> {
    match value {
        AnyValue::Null => None,
        AnyValue::Boolean(v) => Some(v.to_string()),
        AnyValue::Int8(v) => Some(v.to_string()),
        AnyValue::Int16(v) => Some(v.to_string()),
        AnyValue::Int32(v) => Some(v.to_string()),
        AnyValue::Int64(v) => Some(v.to_string()),
        AnyValue::UInt8(v) => Some(v.to_string()),
        AnyValue::UInt16(v) => Some(v.to_string()),
        AnyValue::UInt32(v) => Some(v.to_string()),
        AnyValue::UInt64(v) => Some(v.to_string()),
        AnyValue::Float32(v) => Some(f64::from(*v).to_string()),
        AnyValue::Float64(v) => Some(v.to_string()),
        AnyValue::String(v) => Some(v.to_string()),
        AnyValue::StringOwned(v) => Some(v.to_string()),
        AnyValue::Datetime(_, _, _) => Some(value.to_string()),
        AnyValue::Date(_) => Some(value.to_string()),
        AnyValue::Time(_) => Some(value.to_string()),
        AnyValue::List(series) => {
            let mut parts: Vec<String> = Vec::new();
            for inner in series.iter() {
                if let Some(text) = anyvalue_to_search_string(&inner) {
                    parts.push(text);
                }
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(","))
            }
        }
        other => Some(other.to_string()),
    }
}


// Boolean-search support: tokens, RPN conversion, and evaluation on prebuilt per-row searchable text
#[derive(Debug, Clone, PartialEq, Eq)]
enum SearchToken {
    Term { col: Option<String>, text: String },
    QuotedTerm { col: Option<String>, text: String },
    And,
    Or,
    Not,
}

fn is_operand_token(tok: &SearchToken) -> bool {
    matches!(tok, SearchToken::Term { .. } | SearchToken::QuotedTerm { .. })
}

fn tokenize_search_query(input: &str) -> Vec<SearchToken> {
    // Token rules:
    // - Phrases in double quotes become a single Term (without quotes)
    // - OR operator: word "OR" (upper case) or pipe character '|'
    // - AND operator: explicit "AND" allowed, but also implicit between operands (handled later)
    // - NOT operator: unary, written as leading '-' before a term, or explicit word "NOT"
    // - Case-insensitive matching overall; terms are lowercased here
    let mut raw_parts: Vec<(String, bool)> = Vec::new(); // (text, quoted)
    let mut buf = String::new();
    let mut in_quotes = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes {
                    // end quote -> push buffer as a part
                    if !buf.trim().is_empty() {
                        raw_parts.push((buf.trim().to_string(), true));
                    }
                    buf.clear();
                    in_quotes = false;
                } else {
                    // start quote -> flush current buf as part if any
                    if !buf.trim().is_empty() {
                        raw_parts.push((buf.trim().to_string(), false));
                    }
                    buf.clear();
                    in_quotes = true;
                }
            }
            '|' => {
                if in_quotes {
                    buf.push(ch);
                } else {
                    if !buf.trim().is_empty() {
                        raw_parts.push((buf.trim().to_string(), false));
                    }
                    buf.clear();
                    raw_parts.push(("|".to_string(), false));
                    // collapse consecutive pipes
                    while let Some('|') = chars.peek() {
                        chars.next();
                    }
                }
            }
            c if c.is_whitespace() => {
                if in_quotes {
                    buf.push(c);
                } else {
                    if !buf.trim().is_empty() {
                        raw_parts.push((buf.trim().to_string(), false));
                    }
                    buf.clear();
                }
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() {
        raw_parts.push((buf.trim().to_string(), in_quotes));
    }

    // Merge pattern: col: "phrase with space" → single quoted term with column
    let mut merged: Vec<(String, bool)> = Vec::new();
    let mut i = 0usize;
    while i < raw_parts.len() {
        let (ref part, quoted) = raw_parts[i];
        if !quoted && part.ends_with(':') && i + 1 < raw_parts.len() && raw_parts[i + 1].1 {
            let col = part[..part.len() - 1].to_string();
            let phrase = raw_parts[i + 1].0.clone();
            merged.push((format!("{}:\"{}\"", col, phrase), true));
            i += 2;
            continue;
        }
        merged.push((part.clone(), quoted));
        i += 1;
    }

    // Map merged parts to tokens with unary '-' handling and column prefixes
    let mut tokens: Vec<SearchToken> = Vec::new();
    for (part, quoted) in merged {
        if part == "|" && !quoted {
            tokens.push(SearchToken::Or);
            continue;
        }
        // Do not treat words AND/OR/NOT as operators; users must use space, '|', or '-' only
        // Hyphen NOT: -term or -col:term (only when not quoted)
        if !quoted && part.starts_with('-') && part.len() > 1 {
            tokens.push(SearchToken::Not);
            let rest = &part[1..];
            if let Some(pos) = (!quoted).then(|| rest.find(':')).flatten() {
                let (c, t) = rest.split_at(pos);
                let text = t[1..].to_lowercase();
                let col = c.to_lowercase();
                tokens.push(SearchToken::Term { col: Some(col), text });
            } else {
                tokens.push(SearchToken::Term { col: None, text: rest.to_lowercase() });
            }
            continue;
        }
        // Column prefix: col:term (unquoted) or col:"phrase" (merged, quoted=true)
        if let Some(pos) = (!quoted).then(|| part.find(':')).flatten() {
            let (c, t) = part.split_at(pos);
            let text = t[1..].to_lowercase();
            let col = c.to_lowercase();
            tokens.push(SearchToken::Term { col: Some(col), text });
            continue;
        }
        if quoted {
            // Only treat as column-scoped when pattern is col:"phrase" (merged case)
            if let Some(pos) = part.find(":\"") {
                let (c, t) = part.split_at(pos);
                let text_raw = t[1..].trim();
                let text = text_raw.trim_matches('"').to_lowercase();
                let col = c.to_lowercase();
                tokens.push(SearchToken::QuotedTerm { col: Some(col), text });
            } else {
                tokens.push(SearchToken::QuotedTerm { col: None, text: part.to_lowercase() });
            }
        } else {
            tokens.push(SearchToken::Term { col: None, text: part.to_lowercase() });
        }
    }

    // Insert implicit ANDs between adjacent operands (or operand followed by NOT)
    let mut with_and: Vec<SearchToken> = Vec::new();
    let mut i = 0usize;
    while i < tokens.len() {
        let cur = tokens[i].clone();
        with_and.push(cur.clone());
        if i + 1 < tokens.len() {
            let a = &tokens[i];
            let b = &tokens[i + 1];
            let a_is_operand = is_operand_token(a);
            let b_starts_operand = is_operand_token(b) || matches!(b, SearchToken::Not);
            if a_is_operand && b_starts_operand {
                with_and.push(SearchToken::And);
            }
        }
        i += 1;
    }
    with_and
}

fn to_rpn(tokens: &[SearchToken]) -> Vec<SearchToken> {
    // Shunting-yard without parentheses. Precedence: NOT(3, right), AND(2, left), OR(1, left)
    fn precedence(tok: &SearchToken) -> (u8, bool) {
        match tok {
            SearchToken::Not => (3, true),
            SearchToken::And => (2, false),
            SearchToken::Or => (1, false),
            SearchToken::Term { .. } | SearchToken::QuotedTerm { .. } => (0, false),
        }
    }

    let mut output: Vec<SearchToken> = Vec::new();
    let mut ops: Vec<SearchToken> = Vec::new();

    for tok in tokens {
        match tok {
            SearchToken::Term { .. } | SearchToken::QuotedTerm { .. } => output.push(tok.clone()),
            SearchToken::And | SearchToken::Or | SearchToken::Not => {
                let (p_cur, right_assoc) = precedence(tok);
                while let Some(top) = ops.last() {
                    let (p_top, _) = precedence(top);
                    let should_pop = if right_assoc { p_cur < p_top } else { p_cur <= p_top };
                    if should_pop {
                        output.push(ops.pop().unwrap());
                    } else {
                        break;
                    }
                }
                ops.push(tok.clone());
            }
        }
    }
    while let Some(op) = ops.pop() {
        output.push(op);
    }
    output
}

fn build_search_mask_boolean(
    rpn: &[SearchToken],
    terms: &[(Option<String>, String)],
    searchable_text: &[String],
    // Optional: per-column searchable texts; when None, falls back to row-wide text
    per_column: Option<&HashMap<String, Vec<String>>>,
) -> Vec<bool> {
    // Precompute per-(col,term) masks
    let mut key_masks: HashMap<(Option<String>, String), Vec<bool>> = HashMap::new();
    for (col_opt, term) in terms {
        let key = (col_opt.clone(), term.clone());
        if key_masks.contains_key(&key) { continue; }
        let mut mask = vec![false; searchable_text.len()];
        match (col_opt.as_ref().map(|c| c.to_lowercase()), per_column) {
            (Some(col), Some(per_col)) => {
                if let Some(col_texts) = per_col.get(&col) {
                    for i in 0..searchable_text.len() {
                        if let Some(t) = col_texts.get(i) {
                            if !t.is_empty() && t.contains(term) { mask[i] = true; }
                        }
                    }
                }
            }
            _ => {
                for i in 0..searchable_text.len() {
                    if !searchable_text[i].is_empty() && searchable_text[i].contains(term) {
                        mask[i] = true;
                    }
                }
            }
        }
        key_masks.insert(key, mask);
    }

    // Evaluate per row
    let mut mask_out = vec![false; searchable_text.len()];
    for i in 0..searchable_text.len() {
        let mut stack: Vec<bool> = Vec::new();
        for tok in rpn {
            match tok {
                SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } => {
                    let key = (col.clone(), text.clone());
                    let v = key_masks.get(&key).and_then(|m| m.get(i)).copied().unwrap_or(false);
                    stack.push(v);
                }
                SearchToken::Not => {
                    let a = stack.pop().unwrap_or(false);
                    stack.push(!a);
                }
                SearchToken::And => {
                    let b = stack.pop().unwrap_or(false);
                    let a = stack.pop().unwrap_or(false);
                    stack.push(a && b);
                }
                SearchToken::Or => {
                    let b = stack.pop().unwrap_or(false);
                    let a = stack.pop().unwrap_or(false);
                    stack.push(a || b);
                }
            }
        }
        mask_out[i] = stack.pop().unwrap_or(false);
    }
    mask_out
}


fn materialize_rows(
    df: &DataFrame,
    columns: &[String],
    row_indices: impl Iterator<Item = usize>,
    flags: &HashMap<usize, FlagEntry>,
) -> Vec<ProjectRow> {
    let mut column_series: HashMap<&str, Series> = HashMap::with_capacity(columns.len());
    for column in columns {
        if let Ok(series) = df.column(column) {
            column_series.insert(column.as_str(), series.clone());
        }
    }

    let mut rows = Vec::new();
    for row_idx in row_indices {
        let mut record = HashMap::new();
        for column in columns {
            if let Some(series) = column_series.get(column.as_str()) {
                if let Ok(value) = series.get(row_idx) {
                    record.insert(column.clone(), anyvalue_to_json(&value));
                }
            }
        }
        let flag_entry = flags.get(&row_idx);
        rows.push(ProjectRow {
            row_index: row_idx,
            data: record,
            flag: flag_entry
                .as_ref()
                .map(|entry| normalize_flag_value(&entry.flag))
                .unwrap_or_default(),
            memo: flag_entry.and_then(|entry| entry.memo.clone()),
        });
    }
    rows
}

fn row_contains_query(row: &ProjectRow, query: &str) -> bool {
    if query.trim().is_empty() {
        return false;
    }
    // Build concatenated lowercase text and per-column lowercase texts for the row
    let mut row_text = String::new();
    let mut per_col: HashMap<String, Vec<String>> = HashMap::new();
    for (col, value) in &row.data {
        if let Some(text) = value_to_search_string(value) {
            let lower = text.to_lowercase();
            if !lower.is_empty() {
                if !row_text.is_empty() { row_text.push(' '); }
                row_text.push_str(&lower);
                per_col.insert(col.to_lowercase(), vec![lower]);
            }
        }
    }
    if row_text.is_empty() { return false; }
    // Boolean evaluation using the shared tokenizer and RPN evaluator
    let tokens = tokenize_search_query(query);
    let mut terms: Vec<(Option<String>, String)> = Vec::new();
    for t in &tokens {
        if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t {
            let key = (col.clone(), text.clone());
            if !text.is_empty() && !terms.contains(&key) { terms.push(key); }
        }
    }
    if terms.is_empty() { return false; }
    let rpn = to_rpn(&tokens);
    let mask = build_search_mask_boolean(&rpn, &terms, &vec![row_text], Some(&per_col));
    mask.get(0).copied().unwrap_or(false)
}

fn calculate_ioc_applied_records(project_dir: &Path) -> Result<usize> {
    let parquet_path = project_dir.join("data.parquet");
    let df = read_project_dataframe(&parquet_path)?;
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path)?;
    let iocs = load_ioc_entries(project_dir)?;
    
    let mut ioc_applied_count = 0;
    for row_idx in 0..df.height() {
        let flag_entry = flags.get(&row_idx);
        let user_flag = flag_entry
            .as_ref()
            .map(|entry| normalize_flag_value(&entry.flag))
            .unwrap_or_default();
        
        // Count IOC applications (only when no user flag exists)
        if severity_rank(&user_flag) == 0 && !iocs.is_empty() {
            let mut has_ioc_match = false;
            
            // Build searchable text once for this row
            let mut row_text = String::new();
            for column in df.get_column_names() {
                if column == "__rowid" { continue; }
                if let Ok(series) = df.column(column) {
                    if let Ok(value) = series.get(row_idx) {
                        if let Some(text) = anyvalue_to_search_string(&value) {
                            let lower = text.to_lowercase();
                            if !lower.is_empty() {
                                if !row_text.is_empty() { row_text.push(' '); }
                                row_text.push_str(&lower);
                            }
                        }
                    }
                }
            }
            for entry in &iocs {
                let query = entry.query.trim();
                if query.is_empty() { continue; }
                let tokens = tokenize_search_query(query);
                let mut terms: Vec<(Option<String>, String)> = Vec::new();
                for t in &tokens { if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t { let key = (col.clone(), text.clone()); if !text.is_empty() && !terms.contains(&key) { terms.push(key); } } }
                if terms.is_empty() { continue; }
                let rpn = to_rpn(&tokens);
                // Build single-row per-column map
                let mut single_per_col: HashMap<String, Vec<String>> = HashMap::new();
                for column in df.get_column_names() {
                    if column == "__rowid" { continue; }
                    let mut s = String::new();
                    if let Ok(series) = df.column(column) { if let Ok(v) = series.get(row_idx) { if let Some(t) = anyvalue_to_search_string(&v) { s = t.to_lowercase(); } } }
                    single_per_col.insert(column.to_string().to_lowercase(), vec![s]);
                }
                let mask = build_search_mask_boolean(&rpn, &terms, &vec![row_text.clone()], Some(&single_per_col));
                if mask.get(0).copied().unwrap_or(false) { has_ioc_match = true; break; }
            }
            
            if has_ioc_match {
                ioc_applied_count += 1;
            }
        }
    }
    
    Ok(ioc_applied_count)
}

fn matches_flag_filter(current_flag: &str, filter: &str) -> bool {
    match filter {
        "all" => true,
        "none" => current_flag.is_empty(),
        "priority" => current_flag == "suspicious" || current_flag == "critical",
        "safe" | "suspicious" | "critical" => current_flag == filter,
        _ => true,
    }
}

fn apply_iocs_to_rows(rows: &mut [ProjectRow], entries: &[IocEntry]) {
    if entries.is_empty() {
        return;
    }
    for row in rows {
        let mut best_flag = normalize_flag_value(&row.flag);
        let mut best_rank = severity_rank(&best_flag);
        let mut memo = row.memo.clone().unwrap_or_default();
        let mut memo_changed = false;

        for entry in entries {
            let query = entry.query.trim();
            if query.is_empty() {
                continue;
            }
            if !row_contains_query(row, query) {
                continue;
            }

            let severity = normalize_flag_value(&entry.flag);
            let severity_rank_value = severity_rank(&severity);
            // If user already set a flag, keep it (user wins). Only apply IOC when no user flag.
            if best_rank == 0 && severity_rank_value > 0 {
                best_rank = severity_rank_value;
                    best_flag = severity.clone();
            }

            let tag = entry.tag.trim();
            if !tag.is_empty() {
                let token = format!("[{}]", tag);
                if !memo.contains(&token) {
                    if !memo.is_empty() && !memo.ends_with(' ') {
                        memo.push(' ');
                    }
                    memo.push_str(&token);
                    memo_changed = true;
                }
            }
        }

        // Persist the resolved flag into the row only if it was empty before
        if severity_rank(&normalize_flag_value(&row.flag)) == 0 && best_rank > 0 {
            row.flag = best_flag.clone();
        }
        if memo_changed {
            let trimmed = memo.trim().to_string();
            row.memo = if trimmed.is_empty() { None } else { Some(trimmed) };
        }
    }
}

fn load_ioc_entries(project_dir: &Path) -> Result<Vec<IocEntry>> {
    let path = project_dir.join("iocs.json");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = fs::read(&path)
        .with_context(|| format!("failed to read ioc file {:?}", path))?;
    let mut entries: Vec<IocEntry> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse ioc file {:?}", path))?;
    for entry in &mut entries {
        entry.flag = normalize_flag_value(&entry.flag);
        entry.tag = entry.tag.trim().to_string();
        entry.query = entry.query.trim().to_string();
    }
    Ok(entries)
}

fn save_ioc_entries(project_dir: &Path, entries: &[IocEntry]) -> Result<()> {
    let path = project_dir.join("iocs.json");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to prepare ioc dir {:?}", parent))?;
    }
    let data = serde_json::to_vec_pretty(entries)
        .context("failed to serialize ioc entries")?;
    fs::write(&path, data)
        .with_context(|| format!("failed to write ioc file {:?}", path))
}

fn read_ioc_csv(path: &Path) -> Result<Vec<IocEntry>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("failed to open IOC CSV {:?}", path))?;
    let mut entries = Vec::new();
    for record in reader.records() {
        let record = record.with_context(|| "failed to read IOC CSV record")?;
        let flag_value = record.get(0).unwrap_or("").trim().to_string();
        let tag = record.get(1).unwrap_or("").trim().to_string();
        let query = record.get(2).unwrap_or("").trim().to_string();
        if query.is_empty() {
            continue;
        }
        entries.push(IocEntry {
            flag: normalize_flag_value(&flag_value),
            tag,
            query,
        });
    }
    Ok(entries)
}

fn write_ioc_csv(entries: &[IocEntry], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create export dir {:?}", parent))?;
    }
    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("failed to create IOC CSV {:?}", path))?;
    writer
        .write_record(["flag", "tag", "query"])
        .context("failed to write IOC CSV header")?;
    for entry in entries {
        writer
            .write_record([entry.flag.as_str(), entry.tag.as_str(), entry.query.as_str()])
            .context("failed to write IOC CSV row")?;
    }
    writer.flush().context("failed to flush IOC CSV writer")
}

fn read_project_dataframe(path: &Path) -> Result<DataFrame> {
    ParquetReader::new(File::open(path)?)
        .finish()
        .context("failed to read parquet file")
}

fn write_project_dataframe(path: &Path, df: &mut DataFrame) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("failed to create parquet file {:?}", path))?;
    let writer = ParquetWriter::new(file);
    writer.finish(df).context("failed to write parquet file")?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct CreateProjectPayload {
    path: String,
    description: Option<String>,
}

#[tauri::command]
fn list_projects(state: State<AppState>) -> Result<Vec<ProjectSummary>, String> {
    println!("[debug] list_projects called");
    let metas = state.projects.all();
    let mut result = Vec::with_capacity(metas.len());
    for meta in metas {
        result.push(ProjectSummary {
            meta: meta.clone(),
        });
    }
    result.sort_by(|a, b| b.meta.created_at.cmp(&a.meta.created_at));
    Ok(result)
}

#[tauri::command]
fn create_project(state: State<AppState>, payload: CreateProjectPayload) -> Result<ProjectSummary, String> {
    let source_path = PathBuf::from(&payload.path);
    if !source_path.exists() {
        return Err(AppError::Message("Selected file no longer exists.".into()).into());
    }

    let file = File::open(&source_path)
        .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let mut df = CsvReader::new(file)
        .finish()
        .map_err(|_| AppError::Message("Failed to parse the CSV data.".into()))?;

    // Import flags/memo from CSV if trivium-* columns are present
    let mut imported_flags: HashMap<usize, FlagEntry> = HashMap::new();
    let has_safe = df.get_column_names().iter().any(|c| c == &"trivium-safe");
    let has_suspicious = df.get_column_names().iter().any(|c| c == &"trivium-suspicious");
    let has_critical = df.get_column_names().iter().any(|c| c == &"trivium-critical");
    let has_memo = df.get_column_names().iter().any(|c| c == &"trivium-memo");
    if has_safe || has_suspicious || has_critical || has_memo {
        let safe_col = if has_safe { df.column("trivium-safe").ok() } else { None };
        let suspicious_col = if has_suspicious { df.column("trivium-suspicious").ok() } else { None };
        let critical_col = if has_critical { df.column("trivium-critical").ok() } else { None };
        let memo_col = if has_memo { df.column("trivium-memo").ok() } else { None };

        let height = df.height();
        for row_idx in 0..height {
            let mut best_flag = String::new();
            // priority: critical > suspicious > safe
            if let Some(series) = critical_col.as_ref() {
                if let Ok(v) = series.get(row_idx) { if let Some(text) = anyvalue_to_search_string(&v) { if text != "0" && !text.is_empty() { best_flag = "critical".to_string(); } } }
            }
            if best_flag.is_empty() {
                if let Some(series) = suspicious_col.as_ref() {
                    if let Ok(v) = series.get(row_idx) { if let Some(text) = anyvalue_to_search_string(&v) { if text != "0" && !text.is_empty() { best_flag = "suspicious".to_string(); } } }
                }
            }
            if best_flag.is_empty() {
                if let Some(series) = safe_col.as_ref() {
                    if let Ok(v) = series.get(row_idx) { if let Some(text) = anyvalue_to_search_string(&v) { if text != "0" && !text.is_empty() { best_flag = "safe".to_string(); } } }
                }
            }
            let memo_val = if let Some(series) = memo_col.as_ref() { series.get(row_idx).ok().and_then(|v| anyvalue_to_search_string(&v)) } else { None };
            if !best_flag.is_empty() || memo_val.as_deref().map(|m| !m.trim().is_empty()).unwrap_or(false) {
                imported_flags.insert(row_idx, FlagEntry { flag: best_flag, memo: memo_val.map(|m| m.trim().to_string()).filter(|m| !m.is_empty()) });
            }
        }

        // Drop trivium-* columns before persisting parquet
        for name in &["trivium-safe", "trivium-suspicious", "trivium-critical", "trivium-memo"] {
            if let Ok(next) = df.drop(name) { df = next; }
        }
    }

    let project_id = Uuid::new_v4();
    let project_dir = state.projects.project_dir(&project_id);
    fs::create_dir_all(&project_dir)
        .with_context(|| format!("failed to create project dir {:?}", project_dir))
        .map_err(AppError::from)?;

    let parquet_path = project_dir.join("data.parquet");
    write_project_dataframe(&parquet_path, &mut df).map_err(AppError::from)?;

    // Persist imported flags if any
    if !imported_flags.is_empty() {
        let flags_path = state.projects.project_dir(&project_id).join("flags.json");
        save_flags(&flags_path, &imported_flags).map_err(AppError::from)?;
        // update flagged_records
        let flagged_records = imported_flags.values().filter(|e| !e.flag.trim().is_empty()).count();
        state.projects.update_flagged_records(&project_id, flagged_records).map_err(AppError::from)?;
    }

    let project_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("Untitled")
        .to_string();

    let meta = ProjectMeta {
        id: project_id,
        name: project_name,
        description: payload.description,
        created_at: Utc::now(),
        total_records: df.height(),
        flagged_records: 0,
        ioc_applied_records: 0,
        hidden_columns: Vec::new(),
    };

    state.projects.insert(meta.clone()).map_err(AppError::from)?;
    let summary = ProjectSummary {
        meta: meta.clone(),
    };
    Ok(summary)
}

#[derive(Debug, Deserialize)]
struct ProjectRequest {
    #[serde(rename = "projectId")]
    project_id: Uuid,
}

#[derive(Debug, Deserialize)]
struct QueryRowsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    #[serde(rename = "flagFilter", default)]
    flag_filter: Option<String>,
    #[serde(rename = "search", default)]
    search: Option<String>,
    #[serde(default)]
    columns: Option<Vec<String>>,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(rename = "sortKey", default)]
    sort_key: Option<String>,
    #[serde(rename = "sortDirection", default)]
    sort_direction: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SaveIocsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    entries: Vec<IocEntry>,
}

#[derive(Debug, Deserialize)]
struct ImportIocsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    path: String,
}

#[derive(Debug, Deserialize)]
struct ExportIocsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    destination: String,
}

#[derive(Debug, Serialize)]
struct QueryRowsResponse {
    rows: Vec<ProjectRow>,
    total_flagged: usize,
    total_rows: usize,
    total_filtered_rows: usize,
    offset: usize,
}

#[tauri::command]
fn delete_project(state: State<AppState>, request: ProjectRequest) -> Result<(), String> {
    let Some(meta) = state.projects.find(&request.project_id) else {
        return Ok(());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    if project_dir.exists() {
        fs::remove_dir_all(&project_dir)
            .with_context(|| format!("failed to remove project dir {:?}", project_dir))
            .map_err(AppError::from)?;
    }
    state.projects.remove(&meta.id).map_err(AppError::from)?;
    // Invalidate caches for this project
    state.searchable_cache.lock().remove(&request.project_id);
    state.ioc_flag_cache.lock().remove(&request.project_id);
    Ok(())
}

#[tauri::command]
fn load_project(state: State<AppState>, request: ProjectRequest) -> Result<LoadProjectResponse, String> {
    let meta = state
        .projects
        .find(&request.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = state.projects.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()).into());
    }

    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| name != &"__rowid")
        .map(|name| name.to_string())
        .collect();

    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;

    let mut column_max_chars: HashMap<String, usize> = columns
        .iter()
        .map(|name| {
            let header_len = name.chars().count();
            (name.clone(), header_len)
        })
        .collect();

    // Calculate maximum string length for all data
    for column in &columns {
        if let Ok(series) = df.column(column) {
            for value in series.iter() {
                let json_value = anyvalue_to_json(&value);
                let display_len = value_display_length(&json_value);
                if let Some(entry) = column_max_chars.get_mut(column) {
                    if display_len > *entry {
                        *entry = display_len;
                    }
                }
            }
        }
    }

    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let page_limit = usize::min(DEFAULT_PAGE_SIZE, df.height());
    let mut initial_rows = materialize_rows(&df, &columns, 0..page_limit, &flags);
    apply_iocs_to_rows(&mut initial_rows, &iocs);

    // Note: Do NOT persist IOC-applied flags here; persistence happens only via explicit updates

    println!(
        "[debug] load_project id={} total_rows={} initial_rows={}",
        meta.id,
        df.height(),
        initial_rows.len()
    );

    let summary = ProjectSummary {
        meta: meta.clone(),
    };

    Ok(LoadProjectResponse {
        project: summary,
        columns,
        hidden_columns: meta.hidden_columns.clone(),
        column_max_chars,
        iocs,
        initial_rows,
    })
}


#[tauri::command]
fn query_project_rows(state: State<AppState>, payload: QueryRowsPayload) -> Result<QueryRowsResponse, String> {
    let meta = state
        .projects
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = state.projects.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()).into());
    }

    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| name != &"__rowid")
        .map(|name| name.to_string())
        .collect();
    
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let offset = payload.offset.unwrap_or(0);
    let limit = payload.limit.unwrap_or(DEFAULT_PAGE_SIZE).max(1);

    let total_rows_before_flag_filter = df.height();

    // Stream rows: count filtered matches and only materialize page rows (IOC applied per-row)
    let mut rows: Vec<ProjectRow> = Vec::with_capacity(limit);
    let mut total_flagged_after_ioc: usize = 0;

    // Precompute column names and a lookup for series to speed per-row checks
    let column_names: Vec<String> = columns.clone();
    let column_series: HashMap<&str, &Series> = df.get_columns().iter().map(|s| (s.name(), s)).collect();
    // Case-insensitive column lookup for column-scoped queries
    let column_series_lower: HashMap<String, &Series> = df
        .get_columns()
        .iter()
        .map(|s| (s.name().to_lowercase(), s))
        .collect();

    // Build a concatenated lowercase searchable text per row once (targets: specified columns or all)
    let search_cols: Vec<String> = payload
        .columns
        .as_ref()
        .cloned()
        .unwrap_or_else(|| column_names.clone());
    // cache or build searchable_text per project
    let mut searchable_text: Vec<String> = if let Some(cached) = state.searchable_cache.lock().get(&meta.id).cloned() {
        if cached.len() == df.height() { cached } else { vec![String::new(); df.height()] }
    } else {
        vec![String::new(); df.height()]
    };
    // Per-column lowercase caches for column-scoped queries (lazily built below as needed)
    let mut per_column_text: HashMap<String, Vec<String>> = HashMap::new();
    if searchable_text.iter().all(|s| s.is_empty()) {
        for col in &search_cols {
            if let Some(series) = column_series.get(col.as_str()) {
                for i in 0..df.height() {
                    if let Ok(value) = series.get(i) {
                        if let Some(text) = anyvalue_to_search_string(&value) {
                            let lower = text.to_lowercase();
                            if !lower.is_empty() {
                                if !searchable_text[i].is_empty() { searchable_text[i].push(' '); }
                                searchable_text[i].push_str(&lower);
                                }
                            }
                        }
                    }
                }
        }
        state.searchable_cache.lock().insert(meta.id, searchable_text.clone());
    }

    // Precompute search mask: boolean query (AND/OR/NOT, quotes, implicit AND). Falls back to simple term when single token.
    let mut search_mask: Option<Vec<bool>> = None;
    if let Some(search_str_raw) = payload.search.as_ref().map(|s| s.trim().to_string()) {
        if !search_str_raw.is_empty() {
            let tokens = tokenize_search_query(&search_str_raw);
            // Collect distinct (col,term) pairs
            let mut terms: Vec<(Option<String>, String)> = Vec::new();
            let mut needed_cols: Vec<String> = Vec::new();
            for t in &tokens {
                match t {
                    SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } => {
                        let key = (col.clone(), text.clone());
                        if !text.is_empty() && !terms.contains(&key) { terms.push(key); }
                        if let Some(c) = col {
                            if !needed_cols.contains(c) { needed_cols.push(c.clone()); }
                        }
                    }
                    _ => {}
                }
            }
            // Lazily build per-column caches only for referenced columns
            for c in needed_cols {
                if per_column_text.contains_key(&c) { continue; }
                if let Some(series) = column_series_lower.get(&c) {
                    let series = *series;
                    let mut col_vec: Vec<String> = vec![String::new(); df.height()];
                    for i in 0..df.height() {
                        if let Ok(value) = series.get(i) {
                            if let Some(text) = anyvalue_to_search_string(&value) {
                                let lower = text.to_lowercase();
                                if !lower.is_empty() { col_vec[i] = lower; }
                            }
                        }
                    }
                    per_column_text.insert(c.clone(), col_vec);
                }
            }
            if !terms.is_empty() {
                let rpn = to_rpn(&tokens);
                let mask = build_search_mask_boolean(&rpn, &terms, &searchable_text, Some(&per_column_text));
                search_mask = Some(mask);
            }
        }
    }

    // Vectorized IOC application: build user_flag and ioc_flag arrays once
    let mut user_flag_vec: Vec<String> = vec![String::new(); df.height()];
    for (idx, entry) in flags.iter() {
        if *idx < df.height() {
            user_flag_vec[*idx] = normalize_flag_value(&entry.flag);
        }
    }

    // Initialize ioc_flag vector
    let mut ioc_flag_vec: Vec<String> = if let Some(cached) = state.ioc_flag_cache.lock().get(&meta.id).cloned() {
        if cached.len() == df.height() { cached } else { vec![String::new(); df.height()] }
    } else { vec![String::new(); df.height()] };
    // Sort IOC entries by severity descending so higher priority flags applied first
    let mut sorted_iocs = iocs.clone();
    sorted_iocs.sort_by_key(|e| std::cmp::Reverse(severity_rank(&normalize_flag_value(&e.flag))));
    let need_rebuild_ioc = ioc_flag_vec.iter().all(|s| s.is_empty());
    if need_rebuild_ioc {
        for ioc_entry in &sorted_iocs {
            let query = ioc_entry.query.trim();
            if query.is_empty() { continue; }
            // Build boolean mask for this IOC query across all rows (column selectors supported)
            let tokens = tokenize_search_query(query);
            let mut terms: Vec<(Option<String>, String)> = Vec::new();
            let mut needed_cols: Vec<String> = Vec::new();
            for t in &tokens { if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t { let key = (col.clone(), text.clone()); if !text.is_empty() && !terms.contains(&key) { terms.push(key); } if let Some(c) = col { if !needed_cols.contains(c) { needed_cols.push(c.clone()); } } } }
            // Ensure per-column caches for IOC-referenced columns
            for c in needed_cols {
                if per_column_text.contains_key(&c) { continue; }
                if let Some(series) = column_series_lower.get(&c) {
                    let series = *series;
                    let mut col_vec: Vec<String> = vec![String::new(); df.height()];
                    for i in 0..df.height() {
                        if let Ok(value) = series.get(i) {
                            if let Some(text) = anyvalue_to_search_string(&value) {
                                let lower = text.to_lowercase();
                                if !lower.is_empty() { col_vec[i] = lower; }
                            }
                        }
                    }
                    per_column_text.insert(c.clone(), col_vec);
                }
            }
            if terms.is_empty() { continue; }
            let rpn = to_rpn(&tokens);
            let mask = build_search_mask_boolean(&rpn, &terms, &searchable_text, Some(&per_column_text));
            for i in 0..df.height() {
                if !ioc_flag_vec[i].is_empty() || !user_flag_vec[i].is_empty() { continue; }
                if mask.get(i).copied().unwrap_or(false) {
                    ioc_flag_vec[i] = normalize_flag_value(&ioc_entry.flag);
                }
            }
        }
        state.ioc_flag_cache.lock().insert(meta.id, ioc_flag_vec.clone());
    }

    // Prepare ordered row indices; if sorting requested, sort indices by the sort column
    let mut ordered_indices: Vec<usize> = (0..df.height()).collect();
    if let Some(sort_key) = &payload.sort_key {
        if let Ok(series) = df.column(sort_key) {
            // Use custom comparison logic with numeric parsing support
            ordered_indices.sort_by(|a, b| {
                // Get values as strings where possible and normalize
                let a_s = series.get(*a).ok().and_then(|v| anyvalue_to_search_string(&v)).map(|s| s.trim().replace(",", "").replace('\u{00A0}', ""));
                let b_s = series.get(*b).ok().and_then(|v| anyvalue_to_search_string(&v)).map(|s| s.trim().replace(",", "").replace('\u{00A0}', ""));

                // Try numeric comparison when either side parses
                let a_num = a_s.as_ref().and_then(|s| s.parse::<f64>().ok());
                let b_num = b_s.as_ref().and_then(|s| s.parse::<f64>().ok());

                if a_num.is_some() || b_num.is_some() {
                    let av = a_num.unwrap_or(f64::INFINITY);
                    let bv = b_num.unwrap_or(f64::INFINITY);
                    let mut ord = av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal);
                    if payload.sort_direction.as_deref() == Some("desc") { ord = ord.reverse(); }
                    return ord;
                }

                // Fallback: case-insensitive string compare
                let mut ord = match (&a_s, &b_s) {
                    (Some(a_str), Some(b_str)) => a_str.to_lowercase().cmp(&b_str.to_lowercase()),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                if payload.sort_direction.as_deref() == Some("desc") { ord = ord.reverse(); }
                ord
            });
        }
    }

    // Build final_flag for all rows (user wins)
    let mut final_flag_vec: Vec<String> = Vec::with_capacity(df.height());
    final_flag_vec.extend((0..df.height()).map(|i| {
        if !user_flag_vec[i].is_empty() { user_flag_vec[i].clone() } else { ioc_flag_vec[i].clone() }
    }));

    // Build filtered row indices using vectorized masks
    let mut filtered_indices: Vec<usize> = Vec::with_capacity(df.height());
    for &idx in &ordered_indices {
        // Flag filter
        let ff = &final_flag_vec[idx];
        let flag_ok = if let Some(filter) = &payload.flag_filter { matches_flag_filter(ff, filter) } else { true };
        if !flag_ok { continue; }
        // Search filter (precomputed mask)
        if let Some(mask) = &search_mask { if !mask[idx] { continue; } }
        filtered_indices.push(idx);
    }
    // Compute totals on filtered set
    for &idx in &filtered_indices { if !final_flag_vec[idx].trim().is_empty() { total_flagged_after_ioc += 1; } }
    let total_filtered_rows = filtered_indices.len();

    // Second pass: materialize only the requested page
    for &row_idx in filtered_indices.iter().skip(offset).take(limit) {
        let mut record = HashMap::new();
        for column in &column_names {
            if let Some(series) = column_series.get(column.as_str()) {
                if let Ok(value) = series.get(row_idx) {
                    record.insert(column.clone(), anyvalue_to_json(&value));
                }
            }
        }
        // Compose memo for page rows only (avoid full-scan earlier)
        let user_memo = flags.get(&row_idx).and_then(|e| e.memo.clone()).unwrap_or_default();
        let mut final_memo = user_memo;
        if !iocs.is_empty() && final_flag_vec[row_idx] == ioc_flag_vec[row_idx] {
            // only when IOC provides the flag (no user flag), collect memo tags
            let mut memo_tags: Vec<String> = Vec::new();
            for ioc_entry in &iocs {
                let query = ioc_entry.query.trim();
                if query.is_empty() { continue; }
                // Evaluate boolean query on this row only (column selectors supported)
                let tokens = tokenize_search_query(query);
                let mut terms: Vec<(Option<String>, String)> = Vec::new();
                for t in &tokens { if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t { let key = (col.clone(), text.clone()); if !text.is_empty() && !terms.contains(&key) { terms.push(key); } } }
                if terms.is_empty() { continue; }
                let rpn = to_rpn(&tokens);
                // Prepare a per-column map containing only this row for evaluation
                let mut single_per_col: HashMap<String, Vec<String>> = HashMap::new();
                for (col, vec_texts) in per_column_text.iter() {
                    if let Some(v) = vec_texts.get(row_idx) { single_per_col.insert(col.to_lowercase(), vec![v.clone()]); }
                }
                let single_mask = build_search_mask_boolean(&rpn, &terms, &vec![searchable_text[row_idx].clone()], Some(&single_per_col));
                if single_mask.get(0).copied().unwrap_or(false) {
                    let tag = ioc_entry.tag.trim();
                    if !tag.is_empty() {
                        let token = format!("[{}]", tag);
                        if !memo_tags.contains(&token) { memo_tags.push(token); }
                    }
                }
            }
            for tag in memo_tags {
                if !final_memo.contains(&tag) {
                    if !final_memo.is_empty() && !final_memo.ends_with(' ') { final_memo.push(' '); }
                    final_memo.push_str(&tag);
                }
            }
        }
        rows.push(ProjectRow {
            row_index: row_idx,
            data: record,
            flag: final_flag_vec[row_idx].clone(),
            memo: if final_memo.is_empty() { None } else { Some(final_memo) },
        });
    }

    // (debug logs removed)

    Ok(QueryRowsResponse {
        rows,
        total_flagged: total_flagged_after_ioc,
        total_rows: total_rows_before_flag_filter,
        total_filtered_rows,
        offset,
    })
}

#[tauri::command]
fn save_iocs(state: State<AppState>, payload: SaveIocsPayload) -> Result<(), String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let mut entries: Vec<IocEntry> = payload
        .entries
        .into_iter()
        .map(|entry| IocEntry {
            flag: normalize_flag_value(&entry.flag),
            tag: entry.tag.trim().to_string(),
            query: entry.query.trim().to_string(),
        })
        .filter(|entry| !entry.query.is_empty())
        .collect();
    entries.sort_by(|a, b| a.tag.cmp(&b.tag));
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;
    
    // Invalidate IOC caches so they are recalculated on next query
    state.ioc_flag_cache.lock().remove(&payload.project_id);
    
    // Update ioc_applied_records
    let ioc_applied_records = calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state.projects.update_ioc_applied_records(&payload.project_id, ioc_applied_records).map_err(AppError::from)?;
    
    Ok(())
}

#[tauri::command]
fn import_iocs(state: State<AppState>, payload: ImportIocsPayload) -> Result<Vec<IocEntry>, String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let source = PathBuf::from(payload.path);
    if !source.exists() {
        return Err(AppError::Message("Selected file does not exist.".into()).into());
    }
    let mut entries = read_ioc_csv(&source).map_err(AppError::from)?;
    entries.sort_by(|a, b| a.tag.cmp(&b.tag));
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;
    
    // Invalidate IOC caches so they are recalculated on next query
    state.ioc_flag_cache.lock().remove(&payload.project_id);
    
    // Update ioc_applied_records
    let ioc_applied_records = calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state.projects.update_ioc_applied_records(&payload.project_id, ioc_applied_records).map_err(AppError::from)?;
    
    let final_entries = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    Ok(final_entries)
}

#[tauri::command]
fn export_iocs(state: State<AppState>, payload: ExportIocsPayload) -> Result<(), String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let entries = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    let destination = PathBuf::from(payload.destination);
    write_ioc_csv(&entries, &destination).map_err(AppError::from)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct UpdateFlagPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    row_index: usize,
    flag: String,
    memo: Option<String>,
}

#[tauri::command]
fn update_flag(state: State<AppState>, payload: UpdateFlagPayload) -> Result<ProjectRow, String> {
    let Some(_) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&payload.project_id);
    let flags_path = project_dir.join("flags.json");
    let mut flags = load_flags(&flags_path).map_err(AppError::from)?;

    if payload.flag.trim().is_empty() && payload.memo.as_ref().map(|m| m.trim().is_empty()).unwrap_or(true) {
        flags.remove(&payload.row_index);
    } else {
        flags.insert(
            payload.row_index,
            FlagEntry {
                flag: payload.flag.clone(),
                memo: payload.memo.clone(),
            },
        );
    }

    save_flags(&flags_path, &flags).map_err(AppError::from)?;

        // Update flagged_records (user flags only)
    let flagged_records = flags
        .values()
        .filter(|entry| !entry.flag.trim().is_empty())
        .count();
    state.projects.update_flagged_records(&payload.project_id, flagged_records).map_err(AppError::from)?;
    
        // Update ioc_applied_records
    let ioc_applied_records = calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state.projects.update_ioc_applied_records(&payload.project_id, ioc_applied_records).map_err(AppError::from)?;

    let parquet_path = project_dir.join("data.parquet");
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let mut record = HashMap::new();
    for column in df.get_column_names() {
        if column == "__rowid" {
            continue;
        }
        if let Ok(series) = df.column(column) {
            if let Ok(value) = series.get(payload.row_index) {
                record.insert(column.to_string(), anyvalue_to_json(&value));
            }
        }
    }

    Ok(ProjectRow {
        row_index: payload.row_index,
        data: record,
        flag: payload.flag,
        memo: payload.memo,
    })
}

#[derive(Debug, Deserialize)]
struct HiddenColumnsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    hidden_columns: Vec<String>,
}

#[tauri::command]
fn set_hidden_columns(state: State<AppState>, payload: HiddenColumnsPayload) -> Result<(), String> {
    let Some(_) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    state
        .projects
        .update_hidden_columns(&payload.project_id, payload.hidden_columns)
        .map_err(AppError::from)?;
    // Invalidate searchable cache because visible columns changed and search text is built from columns
    state.searchable_cache.lock().remove(&payload.project_id);
    Ok(())
}

#[derive(Debug, Deserialize)]
struct ExportProjectPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    destination: String,
}

#[tauri::command]
fn export_project(state: State<AppState>, payload: ExportProjectPayload) -> Result<(), String> {
    let meta = state
        .projects
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = state.projects.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    let mut df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let mut safe_flags: Vec<i32> = vec![0; df.height()];
    let mut suspicious_flags: Vec<i32> = vec![0; df.height()];
    let mut critical_flags: Vec<i32> = vec![0; df.height()];
    let mut memo_series: Vec<String> = vec![String::new(); df.height()];

    let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let column_series: HashMap<&str, &Series> = df.get_columns().iter().map(|s| (s.name(), s)).collect();

    for i in 0..df.height() {
        // 1. Always check for IOC matches to get potential flag and memo tags.
        let mut ioc_flag = String::new();
        let mut ioc_rank = 0;
        let mut memo_tags = Vec::new();

        if !iocs.is_empty() {
            // Build searchable text once for this row
            let mut row_text = String::new();
            for col_name in &column_names {
                if let Some(series) = column_series.get(col_name.as_str()) {
                    if let Ok(value) = series.get(i) {
                        if let Some(text) = anyvalue_to_search_string(&value) {
                            let lower = text.to_lowercase();
                            if !lower.is_empty() {
                                if !row_text.is_empty() { row_text.push(' '); }
                                row_text.push_str(&lower);
                            }
                        }
                    }
                }
            }
            for ioc_entry in &iocs {
                let query = ioc_entry.query.trim();
                if query.is_empty() { continue; }
                let tokens = tokenize_search_query(query);
                let mut terms: Vec<(Option<String>, String)> = Vec::new();
                for t in &tokens { if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t { let key = (col.clone(), text.clone()); if !text.is_empty() && !terms.contains(&key) { terms.push(key); } } }
                if terms.is_empty() { continue; }
                let rpn = to_rpn(&tokens);
                // Build single-row per-column map
                let mut single_per_col: HashMap<String, Vec<String>> = HashMap::new();
                for col_name in &column_names {
                    let mut s = String::new();
                    if let Some(series) = column_series.get(col_name.as_str()) { if let Ok(v) = series.get(i) { if let Some(t) = anyvalue_to_search_string(&v) { s = t.to_lowercase(); } } }
                    single_per_col.insert(col_name.to_lowercase(), vec![s]);
                }
                let mask = build_search_mask_boolean(&rpn, &terms, &vec![row_text.clone()], Some(&single_per_col));
                let row_matches = mask.get(0).copied().unwrap_or(false);

                if row_matches {
                    let severity = normalize_flag_value(&ioc_entry.flag);
                    let severity_rank_value = severity_rank(&severity);
                    if severity_rank_value > ioc_rank {
                        ioc_rank = severity_rank_value;
                        ioc_flag = severity.clone();
                    }
                    let tag = ioc_entry.tag.trim();
                    if !tag.is_empty() {
                        let token = format!("[{}]", tag);
                        if !memo_tags.contains(&token) {
                            memo_tags.push(token);
                        }
                    }
                }
            }
        }

        // 2. Check for a user-set flag and determine final flag/memo.
        let final_flag: String;
        let mut final_memo: String;

        if let Some(user_entry) = flags.get(&i) {
            // User flag exists. It wins for the flag value.
            final_flag = normalize_flag_value(user_entry.flag.trim());
            final_memo = user_entry.memo.clone().unwrap_or_default();
        } else {
            // No user flag. Use the IOC flag.
            final_flag = ioc_flag;
            final_memo = String::new();
        }

        // 3. Append IOC memo tags to the final memo, avoiding duplicates.
        for tag in memo_tags {
            if !final_memo.contains(&tag) {
                if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                    final_memo.push(' ');
                }
                final_memo.push_str(&tag);
            }
        }

        // 4. Populate the output vectors.
        match final_flag.as_str() {
            "safe" => safe_flags[i] = 1,
            "suspicious" => suspicious_flags[i] = 1,
            "critical" => critical_flags[i] = 1,
            _ => {}
        }
        memo_series[i] = final_memo;
    }

    if let Ok(next) = df.drop("__rowid") {
        df = next;
    }
    if let Ok(next) = df.drop("flag") {
        df = next;
    }
    if let Ok(next) = df.drop("memo") {
        df = next;
    }
    // Prepend columns at the front in order: trivium-safe, trivium-suspicious, trivium-critical, trivium-memo
    let mut out_cols: Vec<Series> = Vec::new();
    out_cols.push(Series::new("trivium-safe", safe_flags));
    out_cols.push(Series::new("trivium-suspicious", suspicious_flags));
    out_cols.push(Series::new("trivium-critical", critical_flags));
    out_cols.push(Series::new("trivium-memo", memo_series));
    for name in df.get_column_names() {
        if let Ok(series) = df.column(name) { out_cols.push(series.clone()); }
    }
    df = DataFrame::new(out_cols).map_err(|e| AppError::Other(e.into()))?;

    let destination = PathBuf::from(payload.destination);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create export dir {:?}", parent))
            .map_err(AppError::from)?;
    }

    let file = File::create(&destination)
        .with_context(|| format!("failed to create export file {:?}", destination))
        .map_err(AppError::from)?;
    let mut writer = CsvWriter::new(BufWriter::new(file));
    let mut df_out = df.clone();
    writer
        .finish(&mut df_out)
        .context("failed to write export CSV")
        .map_err(AppError::from)?;
    Ok(())
}

fn main() {
    tauri::Builder::new()
        .setup(|app| {
            let state = AppState::new(app)?;
            app.manage(state);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_projects,
            create_project,
            delete_project,
            load_project,
            query_project_rows,
            save_iocs,
            import_iocs,
            export_iocs,
            update_flag,
            set_hidden_columns,
            export_project
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
