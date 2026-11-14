import type * as Wire from "./generated/engine";

// Wire shapes are generated from Rust. These refinements express UI enums and
// accept optional fields from older saved projects without changing the wire.
export type ColumnMeta = Wire.ColumnMeta;
type LegacyMetaFields = "description" | "source_sha256" | "source_file_name" |
  "source_format" | "source_size" | "source_path" | "storage_version" | "columns";
export type ProjectMeta = Omit<Wire.ProjectMeta, LegacyMetaFields> &
  Partial<Pick<Wire.ProjectMeta, LegacyMetaFields>>;
export type ProjectSummary = Omit<Wire.ProjectSummary, "meta"> & { meta: ProjectMeta };
export type FlagSymbol = "safe" | "suspicious" | "critical";
export type CellKind = "string" | "int" | "float" | "bool" | "array" | "object" | "null" | "missing";
/** Source integers and timestamps stay in text, never JavaScript Number. */
export type Cell = Omit<Wire.Cell, "kind" | "truncated" | "length"> &
  Partial<Pick<Wire.Cell, "truncated" | "length">> & { kind: CellKind };
export type ProjectRow = Omit<Wire.ProjectRow, "cells" | "memo" | "color"> &
  Partial<Pick<Wire.ProjectRow, "memo" | "color">> & { cells: Record<string, Cell> };
export type FlagEntry = Omit<Wire.FlagEntry, "memo"> & Partial<Pick<Wire.FlagEntry, "memo">>;
export type FlagFilterSelection = FlagSymbol | "none";
/** Legacy single values remain readable; new combinations are pipe-separated. */
export type FlagFilterValue = "all" | "priority" | FlagFilterSelection |
  `${FlagFilterSelection}|${string}`;
export type CompareOp = Wire.Op;
export type ConditionLeaf = Wire.Condition;
export type ConditionGroup = Wire.Group;
export type ConditionNode = Wire.Node;
/** Normalized UI query; omission/null remain supported in the wire input. */
export type QuerySpec = { [K in keyof Required<Wire.QuerySpec>]:
  K extends "condition" | "sortKey" ? Wire.QuerySpec[K] : NonNullable<Wire.QuerySpec[K]>
} & { flagFilter: FlagFilterValue; sortDirection: "asc" | "desc" };
export type IocEntry = Omit<Wire.IocEntry, "flag"> & { id?: string; flag: FlagSymbol };
export type ColumnKind = "null" | "int" | "float" | "datetime" | "string";
export type ColorRule = Wire.ColorRule;
export interface PresetData {
  name?: string | null;
  condition?: ConditionNode | null;
  hiddenColumns: string[];
  sortKey: string | null;
  sortDirection: "asc" | "desc" | null;
  columnOrder?: string[];
  columnWidths?: Record<string, number>;
  unknown_columns: string[];
}
export type LoadProjectResponse = Omit<Wire.LoadProjectResponse,
  "project" | "column_max_chars" | "column_kinds" | "iocs" | "initial_rows"> & {
  project: ProjectSummary;
  column_max_chars: Record<string, number>;
  column_kinds: Record<string, ColumnKind>;
  iocs: IocEntry[];
  initial_rows: ProjectRow[];
};
export type ViewState = Omit<Wire.ViewState,
  "condition" | "flagFilter" | "sortDirection" | "columnWidths"> & {
  condition: Record<string, unknown> | null;
  flagFilter: FlagFilterValue;
  sortDirection: "asc" | "desc";
  columnWidths: Record<string, number>;
};
export type LoadViewStateResult = Omit<Wire.ViewStateLoad, "state" | "status"> & {
  state: ViewState | null;
  status: "ok" | "missing" | "unreadable" | "newer";
};
export type ValueCount = Wire.ValueCount;
export type ColumnStats = Omit<Wire.ColumnStats, "kind"> & { kind: ColumnKind };
export type HistogramBucket = Wire.HistogramBucket;
export type Histogram = Omit<Wire.Histogram, "kind"> & { kind: ColumnKind };
export type TimeSpan = Wire.TimeSpan;
export type TimeAnalysis = Wire.TimeAnalysis;
export interface SurroundingRows { rows: ProjectRow[] }
export type GroupTreeNode = Omit<Wire.GroupTreeNode, "value" | "children"> & {
  value: unknown;
  children: GroupTreeNode[];
};
export type GroupTreeResponse = Omit<Wire.GroupTreeResponse, "nodes"> & { nodes: GroupTreeNode[] };
export type BulkCount = Wire.BulkCount;
export type BulkApplied = Wire.BulkApplied;
export type JobKind = Wire.JobKind;
export type JobInfo = Wire.JobInfo;
export type JobProgress = Wire.JobProgress;
