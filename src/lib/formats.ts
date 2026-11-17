import type * as Wire from "./generated/engine";

/** Frozen wire definitions are shared by filtering, extraction and the catalog. */
export type FormatDefinition = Wire.FormatDefinition;
export type FormatCatalogEntry = Wire.FormatCatalogEntry;
export type FormatCondition = Wire.FormatCondition;
export type FormatValidation = Wire.FormatValidation;

const VALIDATIONS: readonly FormatValidation[] = ["ipv4-like", "ipv6-like", "domain-like", "http-url-like",
  "ipv6", "ipv4-cidr", "ipv6-cidr", "ip-port", "http-url", "domain",
  "email", "mac", "windows-sid", "uuid", "cve", "defanged-ipv4", "defanged-url", "defanged-domain",
  "powershell-encoded", "jwt", "pem", "ssh-public-key", "bitlocker-recovery-key", "base64", "hex"];

/** Field order can change during a persisted JSON round trip. */
export function formatDefinitionKey(value: FormatDefinition): string {
  return JSON.stringify([value.schemaVersion, value.id, value.name, value.pattern, value.valueGroup ?? null, value.validation ?? null,
    value.examples?.map(example => [example.text, example.values]) ?? []]);
}

const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/** Structural checks only. The Rust matcher validates expressions and examples. */
export function isFormatDefinition(value: unknown): value is FormatDefinition {
  if (!record(value) || value.schemaVersion !== 1 || typeof value.id !== "string" || !value.id.trim()
    || typeof value.name !== "string" || !value.name.trim() || typeof value.pattern !== "string") return false;
  if (value.valueGroup !== undefined && typeof value.valueGroup !== "string") return false;
  if (value.validation !== undefined && !VALIDATIONS.includes(value.validation as FormatValidation)) return false;
  return value.examples === undefined || (Array.isArray(value.examples) && value.examples.every(example =>
    record(example) && typeof example.text === "string" && Array.isArray(example.values)
    && example.values.every(item => typeof item === "string")));
}

export function parseFormatDefinition(value: unknown): FormatDefinition {
  if (!isFormatDefinition(value)) throw new Error("Invalid pattern definition. Expected a version 1 pattern file.");
  const { schemaVersion, id, name, pattern, valueGroup, validation, examples } = value;
  return structuredClone({ schemaVersion, id, name, pattern,
    ...(valueGroup === undefined ? {} : { valueGroup }),
    ...(validation === undefined ? {} : { validation }),
    ...(examples === undefined ? {} : { examples }) });
}

export function formatFileName(definition: FormatDefinition): string {
  const name = definition.name.trim().replace(/[^\p{L}\p{N}._-]+/gu, "-").slice(0, 80).replace(/[.-]+$/g, "");
  return `trivium-pattern-${name || "custom"}.json`;
}
