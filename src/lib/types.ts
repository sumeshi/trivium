export interface ColumnMeta {
  /** Internal column id (`c000001` for new projects, the column name for legacy projects). */
  id: string;
  /** Display label. Not guaranteed to be unique. */
  label: string;
  /** Original CSV header or JSON Pointer. */
  source: string;
  order: number;
}

export interface ProjectMeta {
  id: string;
  name: string;
  description?: string | null;
  created_at: string;
  total_records: number;
  flagged_records: number;
  ioc_applied_records: number;
  source_sha256?: string | null;
  source_file_name?: string | null;
  source_format?: "csv" | "json" | "jsonl" | "ndjson" | string | null;
  source_size?: number | null;
  /** Absolute path of the evidence file when the project was created. */
  source_path?: string | null;
  hidden_columns: string[];
  storage_version?: number;
  columns?: ColumnMeta[];
}

export interface ProjectSummary {
  meta: ProjectMeta;
}

export type FlagSymbol = 'safe' | 'suspicious' | 'critical';

export type CellKind =
  | "string"
  | "int"
  | "float"
  | "bool"
  | "array"
  | "object"
  | "null"
  | "missing";

/**
 * Cell value from the backend. In list responses `text` is a preview up to 256
 * characters; `truncated` and `length` describe the original value. `getCell`
 * returns the full text in the same shape. Integers arrive as decimal strings
 * so the UI never converts them through `Number`.
 */
export interface Cell {
  text: string | null;
  kind: CellKind;
  /** True when `text` is only the list preview. */
  truncated?: boolean;
  /** Original character count; `null`/absent for null and missing cells. */
  length?: number | null;
}

export interface ProjectRow {
  row_index: number;
  /** Compatibility copy of the row values. The UI reads `cells`. */
  data: Record<string, unknown>;
  /** Exact display/copy cells, keyed by column id. */
  cells: Record<string, Cell>;
  flag: string;
  memo?: string | null;
  tags: string[];
  bookmarked: boolean;
  color?: string | null;
}

/** Stored annotation for one row. */
export interface FlagEntry {
  flag: string;
  memo?: string | null;
  tags: string[];
  bookmarked: boolean;
}

export type FlagFilterValue =
  | "all"
  | "none"
  | "priority"
  | "safe"
  | "suspicious"
  | "critical";

/**
 * Shared query contract. `searchColumns` is the search scope, not the visible
 * columns: an empty array means every data column.
 */
export type QuerySpec = {
  search: string;
  searchColumns: string[];
  condition: ConditionNode | null;
  flagFilter: FlagFilterValue;
  bookmarkedOnly: boolean;
  sortKey: string | null;
  sortDirection: "asc" | "desc";
};

export interface IocEntry {
  id?: string;
  flag: FlagSymbol;
  tag: string;
  query: string;
}

export type ColumnKind = "null" | "int" | "float" | "datetime" | "string";

export interface ColorRule {
  name: string;
  condition: ConditionNode;
  color: string;
}

export interface PresetData {
  name?: string | null;
  condition?: ConditionNode | null;
  hiddenColumns: string[];
  sortKey: string | null;
  sortDirection: "asc" | "desc" | null;
  unknown_columns: string[];
}

export interface LoadProjectResponse {
  project: ProjectSummary;
  /** Data column ids in source order. Legacy ids are the original column names. */
  columns: string[];
  /** Display/source metadata for `columns`. */
  columns_meta: ColumnMeta[];
  hidden_columns: string[];
  column_max_chars: Record<string, number>;
  column_kinds: Record<string, ColumnKind>;
  iocs: IocEntry[];
  color_rules: ColorRule[];
  initial_rows: ProjectRow[];
}

/**
 * Rebuildable view settings stored per project by the engine. This is
 * independent from annotations: the UI can fall back to defaults when it is
 * missing or unreadable without hiding evidence or saved annotations.
 */
/** Result of reading view state; `status` separates missing from unreadable. */
export interface LoadViewStateResult {
  state: ViewState | null;
  status: "ok" | "missing" | "unreadable" | "newer";
}

export interface ViewState {
  version: number;
  search: string;
  searchColumns: string[];
  /** EditorGroup JSON; the engine stores it without interpreting it. */
  condition: Record<string, unknown> | null;
  flagFilter: FlagFilterValue;
  bookmarkedOnly: boolean;
  sortKey: string | null;
  sortDirection: "asc" | "desc";
  hiddenColumns: string[];
  columnWidths: Record<string, number>;
  logicalRow: number;
}

export interface ValueCount {
  value: string;
  count: number;
}

export interface ColumnStats {
  column: string;
  kind: ColumnKind;
  missing: number;
  unique: number;
  top: ValueCount[];
  rare: ValueCount[];
}

export interface HistogramBucket {
  start: string;
  end: string;
  count: number;
}

export interface Histogram {
  column: string;
  kind: ColumnKind;
  buckets: HistogramBucket[];
  missing: number;
  outside: number;
}

export interface TimeSpan {
  start: string;
  end: string;
  count: number;
}

export interface TimeAnalysis {
  column: string;
  gaps: TimeSpan[];
  sessions: TimeSpan[];
  bursts: TimeSpan[];
}

export interface SurroundingRows {
  rows: ProjectRow[];
}

export type CompareOp =
  | "eq"
  | "ne"
  | "gt"
  | "lt"
  | "ge"
  | "le"
  | "contains"
  | "startsWith"
  | "endsWith"
  | "regex"
  | "in";

export interface ConditionLeaf {
  column: string;
  op: CompareOp;
  values: string[];
  negated: boolean;
}

export interface ConditionGroup {
  kind: "and" | "or";
  children: ConditionNode[];
}

export type ConditionNode = ConditionLeaf | ConditionGroup;

export interface GroupTreeNode {
  value: unknown;
  label: string;
  count: number;
  children: GroupTreeNode[];
}

export interface GroupTreeResponse {
  columns: string[];
  nodes: GroupTreeNode[];
  total: number;
}

export interface BulkCount {
  count: number;
  previewToken: string;
}

export interface BulkApplied {
  updated: number;
}

export type JobKind = "import" | "query" | "export";

/** A background job reported by the desktop backend. */
export interface JobInfo {
  /** Monotonic process-local id; also the job generation. */
  id: number;
  kind: JobKind;
  projectId: string | null;
}

/** A throttled background-job progress value. */
export interface JobProgress {
  id: number;
  kind: JobKind;
  done: number;
  total: number | null;
  message: string | null;
}
