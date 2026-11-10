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
  hidden_columns: string[];
  storage_version?: number;
  columns?: ColumnMeta[];
}

export interface ProjectSummary {
  meta: ProjectMeta;
}

export type FlagSymbol = 'safe' | 'suspicious' | 'critical';

export interface ProjectRow {
  row_index: number;
  data: Record<string, unknown>;
  flag: string;
  memo?: string | null;
  tags: string[];
  bookmarked: boolean;
  color?: string | null;
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
