//! Declarative named formats shared by filters and extraction.
pub mod catalog;
mod matcher;
mod model;
mod validation;

pub use catalog::{
    delete_format, list_formats, save_format, DeleteFormatPayload, FormatCatalogEntry,
    SaveFormatPayload,
};
pub use matcher::{CompiledFormat, FormatOccurrence, FormatOccurrences};
pub use model::{FormatDefinition, FormatExample, FormatValidation};

#[cfg(test)]
mod tests;
