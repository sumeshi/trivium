export interface ProjectMeta {
  id: string;
  name: string;
  description?: string | null;
  created_at: string;
  total_records: number;
  hidden_columns: string[];
}

export interface ProjectSummary {
  meta: ProjectMeta;
  flagged_records: number;
}

export interface ProjectRow {
  row_index: number;
  data: Record<string, unknown>;
  flag: string;
  memo?: string | null;
}

export interface LoadProjectResponse {
  project: ProjectSummary;
  columns: string[];
  rows: ProjectRow[];
  hidden_columns: string[];
}
