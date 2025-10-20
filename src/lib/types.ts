export interface ProjectMeta {
  id: string;
  name: string;
  description?: string | null;
  created_at: string;
  total_records: number;
  flagged_records: number;
  hidden_columns: string[];
}

export interface ProjectSummary {
  meta: ProjectMeta;
}

export interface ProjectRow {
  row_index: number;
  data: Record<string, unknown>;
  flag: string;
  memo?: string | null;
}

export interface IocEntry {
  flag: string;
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
