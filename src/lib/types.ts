import type * as Wire from "./generated/engine";

// Wire shapes are generated from Rust; refinements express UI enums.
export type ColumnMeta = Wire.ColumnMeta;
export type ImportOptions = Wire.ImportOptions;
export type ImportReport = Wire.ImportReport;
export type ImportPreview = Wire.ImportPreview;
export type TextEncoding = Wire.TextEncoding;
export type ProjectMeta = Wire.ProjectMeta;
export type ProjectSummary = Omit<Wire.ProjectSummary, "meta"> & { meta: ProjectMeta };
export type FlagSymbol = "safe" | "suspicious" | "critical";
export type CellKind = "string" | "int" | "float" | "bool" | "array" | "object" | "null" | "missing";
/** Source integers and timestamps stay in text, never JavaScript Number. */
export type Cell = Omit<Wire.Cell, "kind"> & { kind: CellKind };
export type ProjectRow = Omit<Wire.ProjectRow, "cells"> & { cells: Record<string, Cell> };
export type FlagEntry = Wire.FlagEntry;
export type FlagFilterSelection = FlagSymbol | "none";
/** Multiple selected flags use canonical pipe-separated tokens. */
export type FlagFilterValue = "all" | FlagFilterSelection |
  `${FlagFilterSelection}|${string}`;
export type CompareOp = Wire.Op;
export type ConditionLeaf = Wire.Condition;
export type ConditionGroup = Wire.Group;
export type ConditionNode = Wire.Node;
/** Normalized UI query; omission/null remain supported in the wire input. */
export type QuerySpec = { [K in keyof Required<Wire.QuerySpec>]:
  K extends "condition" | "sortKey" ? Wire.QuerySpec[K] : NonNullable<Wire.QuerySpec[K]>
} & { flagFilter: FlagFilterValue; sortDirection: "asc" | "desc" };
/** Invalid stored IOC flags stay visible until the user corrects them. */
export type IocEntry = Wire.IocEntry & { id?: string };
export type ColumnKind = "null" | "int" | "float" | "datetime" | "string";
export type ColorRule = Wire.ColorRule;
export interface PresetFilters {
  search: string;
  searchColumns: string[];
  flagFilter: FlagFilterValue;
  bookmarkedOnly: boolean;
}
export interface PresetData {
  name?: string | null;
  condition?: ConditionNode | null;
  hiddenColumns: string[];
  sortKey: string | null;
  sortDirection: "asc" | "desc" | null;
  columnOrder?: string[];
  columnWidths?: Record<string, number>;
  filters?: PresetFilters;
  unknown_columns: string[];
}
export type LoadProjectResponse = Omit<Wire.LoadProjectResponse,
  "project" | "column_max_chars" | "column_kinds" | "iocs"> & {
  project: ProjectSummary;
  column_max_chars: Record<string, number>;
  column_kinds: Record<string, ColumnKind>;
  iocs: IocEntry[];
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
