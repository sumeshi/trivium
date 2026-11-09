export interface ProjectMeta {
  id: string;
  name: string;
  description?: string | null;
  created_at: string;
  total_records: number;
  flagged_records: number;
  ioc_applied_records: number;
  hidden_columns: string[];
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
}

export interface IocEntry {
  id?: string;
  flag: FlagSymbol;
  tag: string;
  query: string;
}

export type ColumnKind = "null" | "int" | "float" | "datetime" | "string";

export interface LoadProjectResponse {
  project: ProjectSummary;
  columns: string[];
  hidden_columns: string[];
  column_max_chars: Record<string, number>;
  column_kinds: Record<string, ColumnKind>;
  iocs: IocEntry[];
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

export interface QueryRowsResponse {
  rows: ProjectRow[];
  total_flagged: number;
  total_rows: number;
  total_filtered_rows: number;
  offset: number;
}

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
