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

export interface LoadProjectResponse {
  project: ProjectSummary;
  columns: string[];
  hidden_columns: string[];
  column_max_chars: Record<string, number>;
  iocs: IocEntry[];
  initial_rows: ProjectRow[];
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
