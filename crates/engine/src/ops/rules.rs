use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError,
    models::ColorRule,
    storage::{load_color_rules, save_color_rules as persist_color_rules},
    store::ProjectsStore,
};

use super::projects::ProjectRequest;

const MAX_COLOR_RULES: usize = 50;

#[derive(Debug, Deserialize)]
pub struct SaveColorRulesPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub rules: Vec<ColorRule>,
}

#[derive(Debug, Serialize)]
pub struct ColorRulesResponse {
    pub rules: Vec<ColorRule>,
}

/// Trims names, validates colors, and lowercases the hex digits. Rules with a
/// blank name are dropped rather than persisted as unlabelled entries.
fn normalize_rules(rules: Vec<ColorRule>) -> Result<Vec<ColorRule>, AppError> {
    let mut normalized = Vec::with_capacity(rules.len());
    for rule in rules {
        let name = rule.name.trim();
        if name.is_empty() {
            continue;
        }
        normalized.push(ColorRule {
            name: name.to_string(),
            condition: rule.condition,
            color: normalize_color(&rule.color)?,
        });
    }
    if normalized.len() > MAX_COLOR_RULES {
        return Err(AppError::Message(
            "Too many color rules (max 50).".to_string(),
        ));
    }
    Ok(normalized)
}

/// Accepts `#rgb` and `#rrggbb` (case-insensitive) and stores lowercase.
fn normalize_color(raw: &str) -> Result<String, AppError> {
    let value = raw.trim();
    let digits = value.strip_prefix('#').unwrap_or("");
    let valid_length = digits.len() == 3 || digits.len() == 6;
    if !valid_length || !digits.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(AppError::Message(format!(
            "Invalid color '{}'. Use #rgb or #rrggbb.",
            value
        )));
    }
    Ok(format!("#{}", digits.to_ascii_lowercase()))
}

/// Normalizes and persists a project's color rules.
pub fn save_color_rules(
    store: &ProjectsStore,
    payload: SaveColorRulesPayload,
) -> Result<ColorRulesResponse, AppError> {
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".to_string()))?;
    let project_dir = store.project_dir(&meta.id);
    let rules = normalize_rules(payload.rules)?;
    persist_color_rules(&project_dir, &rules).map_err(AppError::from)?;
    Ok(ColorRulesResponse { rules })
}

/// Returns a project's color rules. A project with no saved file has no rules.
pub fn color_rules(
    store: &ProjectsStore,
    request: ProjectRequest,
) -> Result<ColorRulesResponse, AppError> {
    let meta = store
        .find(&request.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".to_string()))?;
    let project_dir = store.project_dir(&meta.id);
    let rules = load_color_rules(&project_dir)
        .map_err(AppError::from)?
        .unwrap_or_default();
    Ok(ColorRulesResponse { rules })
}
