import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import type { UnlistenFn } from "@tauri-apps/api/event";
import type {
  CellKind,
  ColorRule,
  ConditionNode,
  FlagFilterValue,
  IocEntry,
  BulkApplied,
  BulkCount,
  ColumnStats,
  GroupTreeResponse,
  Histogram,
  JobInfo,
  JobProgress,
  LoadProjectResponse,
  PresetData,
  QuerySpec,
  TimeAnalysis,
  SurroundingRows,
  ProjectRow,
  ProjectSummary,
  FlagEntry,
} from "./types";

export type { FlagFilterValue } from "./types";

export interface CreateProjectArgs {
  description?: string | null;
  path?: string;
  delimiter?: string | null;
}

export interface AnnotationPatch {
  /** `undefined` keeps the stored flag; `""` clears it. */
  flag?: "" | "safe" | "suspicious" | "critical";
  /** `undefined` keeps the stored memo; `null` or `""` clears it. */
  memo?: string | null;
  /** `undefined` keeps the stored tags; `[]` clears them. */
  tags?: string[];
  /** Tags appended without duplicates; `undefined` adds nothing. */
  add_tags?: string[];
  /** Tags removed after `tags`/`add_tags`; `undefined` removes nothing. */
  remove_tags?: string[];
  /** `undefined` keeps the stored bookmark; `false` clears it. */
  bookmarked?: boolean;
}

export interface UpdateFlagArgs extends AnnotationPatch {
  projectId: string;
  rowIndex: number;
}

export interface UpdateFlagResult {
  /** `null` when this update removed the row's last annotation. */
  entry: FlagEntry | null;
  /** Process-local monotonic revision for the project. */
  revision: number;
  /** Whole-project annotated row count after the update. */
  flaggedRecords: number;
}

export interface HiddenColumnsArgs {
  projectId: string;
  hiddenColumns: string[];
}

export type ExportScope = "all" | "filtered" | "selected";

export interface ExportProjectArgs {
  projectId: string;
  destination?: string;
  /** `"filtered"` (default) uses the query fields; `"selected"` intersects `rowIndices`. */
  scope?: ExportScope;
  search?: string;
  searchColumns?: string[];
  condition?: ConditionNode | null;
  flagFilter?: FlagFilterValue;
  bookmarkedOnly?: boolean;
  sortKey?: string | null;
  sortDirection?: "asc" | "desc";
  /** Output columns only; search scope is `searchColumns`. */
  columns?: string[];
  /** Required for the `"selected"` scope. Ids keep the query result order. */
  rowIndices?: number[];
  /** Adds the independent `trivium-*` columns; defaults to true. */
  includeAnnotations?: boolean;
}

export interface ExportResult {
  /** Data rows written, excluding the header. */
  rows: number;
  /** Header labels in output order. */
  columns: string[];
  /** Non-fatal notes, such as an annotation column renamed to avoid a clash. */
  warnings: string[];
}

export interface SaveIocsArgs {
  projectId: string;
  entries: IocEntry[];
}

export interface ImportIocsArgs {
  projectId: string;
  path: string;
}

export interface ExportIocsArgs {
  projectId: string;
  destination: string;
}

export interface SavePresetArgs {
  projectId: string;
  name?: string | null;
  condition?: unknown;
  hiddenColumns: string[];
  sortKey?: string | null;
  sortDirection?: "asc" | "desc" | null;
  destination: string;
}

export interface LoadPresetArgs {
  projectId?: string | null;
  source: string;
}

export interface ColorRuleArgs {
  projectId: string;
}

export interface SaveColorRulesArgs {
  projectId: string;
  rules: ColorRule[];
}

export interface ColorRulesResponse {
  rules: ColorRule[];
}

export interface ColumnStatsArgs {
  projectId: string;
  column: string;
  rareMax?: number;
}

export interface HistogramArgs {
  projectId: string;
  column: string;
  buckets?: number;
  search?: string;
  flagFilter?: FlagFilterValue;
  columns?: string[];
  condition?: ConditionNode | null;
  range?: { min: string; max: string } | null;
}

export interface SurroundingRowsArgs {
  projectId: string;
  rowIndex: number;
  column: string;
  mode: "neighbors" | "window";
  count: number;
}

export interface TimeAnalysisArgs {
  projectId: string;
  column: string;
  gapSeconds?: number;
  sessionMinutes?: number;
  burstWindowSeconds?: number;
  burstMin?: number;
  search?: string;
  flagFilter?: FlagFilterValue;
  columns?: string[];
  condition?: ConditionNode | null;
}

export interface QueryProjectRowsArgs extends QuerySpec {
  projectId: string;
  offset?: number;
  limit?: number;
}

export interface QueryProjectRowsResponse {
  rows: ProjectRow[];
  annotated_total: number;
  annotated_filtered: number;
  total_rows: number;
  total_filtered_rows: number;
  offset: number;
}

export interface GetCellArgs {
  projectId: string;
  /** Original row id, independent of the current query result order. */
  rowIndex: number;
  columnId: string;
}

/** Full cell value from `getCell`; `text` is never a list preview. */
export interface GetCellResult {
  text: string | null;
  kind: CellKind;
  truncated: boolean;
  length: number | null;
}

export interface GroupTreeArgs {
  projectId: string;
  groupColumns: string[];
  search?: string;
  flagFilter?: FlagFilterValue;
  columns?: string[];
  condition?: ConditionNode | null;
}

export interface BulkAnnotateArgs extends AnnotationPatch {
  projectId: string;
  target: "selection" | "filtered" | "all";
  rowIndices?: number[];
  search?: string;
  searchColumns?: string[];
  condition?: ConditionNode | null;
  flagFilter?: FlagFilterValue;
  bookmarkedOnly?: boolean;
  sortKey?: string | null;
  sortDirection?: "asc" | "desc";
  /** Token returned by `bulkPreview`; required by `bulkUpdate`. */
  previewToken?: string;
}

export interface BulkUndoArgs {
  projectId: string;
}

export interface Backend {
  readonly isNative: boolean;
  listProjects(): Promise<ProjectSummary[]>;
  createProject(args: CreateProjectArgs): Promise<ProjectSummary>;
  deleteProject(projectId: string): Promise<void>;
  loadProject(projectId: string): Promise<LoadProjectResponse>;
  columnStats(args: ColumnStatsArgs): Promise<ColumnStats>;
  histogram(args: HistogramArgs): Promise<Histogram>;
  timeAnalysis(args: TimeAnalysisArgs): Promise<TimeAnalysis>;
  surroundingRows(args: SurroundingRowsArgs): Promise<SurroundingRows>;
  queryProjectRows(
    args: QueryProjectRowsArgs
  ): Promise<QueryProjectRowsResponse>;
  getCell(args: GetCellArgs): Promise<GetCellResult>;
  groupTree(args: GroupTreeArgs): Promise<GroupTreeResponse>;
  bulkPreview(args: BulkAnnotateArgs): Promise<BulkCount>;
  bulkUpdate(args: BulkAnnotateArgs): Promise<BulkApplied>;
  bulkUndo(args: BulkUndoArgs): Promise<BulkApplied>;
  saveIocs(args: SaveIocsArgs): Promise<void>;
  importIocs(args: ImportIocsArgs): Promise<void>;
  exportIocs(args: ExportIocsArgs): Promise<void>;
  savePreset(args: SavePresetArgs): Promise<void>;
  loadPreset(args: LoadPresetArgs): Promise<PresetData>;
  colorRules(args: ColorRuleArgs): Promise<ColorRulesResponse>;
  saveColorRules(args: SaveColorRulesArgs): Promise<ColorRulesResponse>;
  updateFlag(args: UpdateFlagArgs): Promise<UpdateFlagResult>;
  setHiddenColumns(args: HiddenColumnsArgs): Promise<void>;
  exportProject(args: ExportProjectArgs): Promise<ExportResult>;
  /** Requests cancellation of one running job; `false` means it already ended. */
  cancelJob(jobId: number): Promise<boolean>;
  /** Lists running background jobs. */
  activeJobs(): Promise<JobInfo[]>;
}

/**
 * Keeps AnnotationPatch's omission semantics across IPC: only fields the
 * caller supplied are sent, so an absent key means "keep the stored value".
 */
function annotationFields(
  args: AnnotationPatch
): Record<string, string | string[] | boolean | null> {
  const fields: Record<string, string | string[] | boolean | null> = {};
  if (args.flag !== undefined) {
    fields.flag = args.flag;
  }
  if (args.memo !== undefined) {
    fields.memo = args.memo;
  }
  if (args.tags !== undefined) {
    fields.tags = args.tags;
  }
  if (args.add_tags !== undefined) {
    fields.add_tags = args.add_tags;
  }
  if (args.remove_tags !== undefined) {
    fields.remove_tags = args.remove_tags;
  }
  if (args.bookmarked !== undefined) {
    fields.bookmarked = args.bookmarked;
  }
  return fields;
}

class NativeBackend implements Backend {
  readonly isNative = true;

  listProjects(): Promise<ProjectSummary[]> {
    return invoke("list_projects");
  }

  createProject(args: CreateProjectArgs): Promise<ProjectSummary> {
    if (!args.path) {
      return Promise.reject(new Error("Path is required to create a project."));
    }
    return invoke("create_project", {
      payload: {
        path: args.path,
        description: args.description ?? null,
        delimiter: args.delimiter ?? null,
      },
    });
  }

  deleteProject(projectId: string): Promise<void> {
    return invoke("delete_project", { request: { projectId: projectId } });
  }

  loadProject(projectId: string): Promise<LoadProjectResponse> {
    return invoke("load_project", {
      request: { projectId: projectId },
    });
  }

  columnStats(args: ColumnStatsArgs): Promise<ColumnStats> {
    return invoke("column_stats", {
      payload: {
        projectId: args.projectId,
        column: args.column,
        rareMax: args.rareMax ?? 1,
      },
    });
  }

  histogram(args: HistogramArgs): Promise<Histogram> {
    return invoke("histogram", {
      payload: {
        projectId: args.projectId,
        column: args.column,
        buckets: args.buckets ?? 50,
        search: args.search ?? null,
        flagFilter: args.flagFilter ?? null,
        columns: args.columns ?? null,
        condition: args.condition ?? null,
        range: args.range ?? null,
      },
    });
  }

  timeAnalysis(args: TimeAnalysisArgs): Promise<TimeAnalysis> {
    return invoke("time_analysis", {
      payload: {
        projectId: args.projectId,
        column: args.column,
        gapSeconds: args.gapSeconds ?? 60,
        sessionMinutes: args.sessionMinutes ?? 30,
        burstWindowSeconds: args.burstWindowSeconds ?? 60,
        burstMin: args.burstMin ?? 10,
        search: args.search ?? null,
        flagFilter: args.flagFilter ?? null,
        columns: args.columns ?? null,
        condition: args.condition ?? null,
      },
    });
  }

  surroundingRows(args: SurroundingRowsArgs): Promise<SurroundingRows> {
    return invoke("surrounding_rows", {
      payload: {
        projectId: args.projectId,
        rowIndex: args.rowIndex,
        column: args.column,
        mode: args.mode,
        count: args.count,
      },
    });
  }

  queryProjectRows(
    args: QueryProjectRowsArgs
  ): Promise<QueryProjectRowsResponse> {
    return invoke("query_project_rows", {
      payload: {
        projectId: args.projectId,
        search: args.search,
        searchColumns: args.searchColumns,
        flagFilter: args.flagFilter,
        bookmarkedOnly: args.bookmarkedOnly,
        offset: args.offset ?? null,
        limit: args.limit ?? null,
        sortKey: args.sortKey,
        sortDirection: args.sortDirection,
        condition: args.condition,
      },
    });
  }

  getCell(args: GetCellArgs): Promise<GetCellResult> {
    return invoke("get_cell", {
      payload: {
        projectId: args.projectId,
        rowIndex: args.rowIndex,
        columnId: args.columnId,
      },
    });
  }

  groupTree(args: GroupTreeArgs): Promise<GroupTreeResponse> {
    return invoke("group_tree", {
      payload: {
        projectId: args.projectId,
        search: args.search ?? null,
        flagFilter: args.flagFilter ?? null,
        columns: args.columns ?? null,
        condition: args.condition ?? null,
        groupColumns: args.groupColumns,
      },
    });
  }

  bulkPreview(args: BulkAnnotateArgs): Promise<BulkCount> {
    return invoke("bulk_preview", {
      payload: {
        projectId: args.projectId,
        target: args.target,
        rowIndices: args.rowIndices ?? null,
        search: args.search ?? null,
        searchColumns: args.searchColumns ?? null,
        flagFilter: args.flagFilter ?? null,
        bookmarkedOnly: args.bookmarkedOnly ?? null,
        sortKey: args.sortKey ?? null,
        sortDirection: args.sortDirection ?? null,
        condition: args.condition ?? null,
        previewToken: args.previewToken ?? null,
        ...annotationFields(args),
      },
    });
  }

  bulkUpdate(args: BulkAnnotateArgs): Promise<BulkApplied> {
    return invoke("bulk_update", {
      payload: {
        projectId: args.projectId,
        target: args.target,
        rowIndices: args.rowIndices ?? null,
        search: args.search ?? null,
        searchColumns: args.searchColumns ?? null,
        flagFilter: args.flagFilter ?? null,
        bookmarkedOnly: args.bookmarkedOnly ?? null,
        sortKey: args.sortKey ?? null,
        sortDirection: args.sortDirection ?? null,
        condition: args.condition ?? null,
        previewToken: args.previewToken ?? null,
        ...annotationFields(args),
      },
    });
  }

  bulkUndo(args: BulkUndoArgs): Promise<BulkApplied> {
    return invoke("bulk_undo", {
      payload: {
        projectId: args.projectId,
      },
    });
  }

  saveIocs(args: SaveIocsArgs): Promise<void> {
    return invoke("save_iocs", {
      payload: {
        projectId: args.projectId,
        entries: args.entries,
      },
    });
  }

  importIocs(args: ImportIocsArgs): Promise<void> {
    return invoke("import_iocs", {
      payload: {
        projectId: args.projectId,
        path: args.path,
      },
    });
  }

  exportIocs(args: ExportIocsArgs): Promise<void> {
    return invoke("export_iocs", {
      payload: {
        projectId: args.projectId,
        destination: args.destination,
      },
    });
  }

  savePreset(args: SavePresetArgs): Promise<void> {
    return invoke("save_preset", {
      payload: {
        projectId: args.projectId,
        name: args.name ?? null,
        condition: args.condition ?? null,
        hiddenColumns: args.hiddenColumns,
        sortKey: args.sortKey ?? null,
        sortDirection: args.sortDirection ?? null,
        destination: args.destination,
      },
    });
  }

  loadPreset(args: LoadPresetArgs): Promise<PresetData> {
    return invoke("load_preset", {
      payload: {
        projectId: args.projectId ?? null,
        source: args.source,
      },
    });
  }

  colorRules(args: ColorRuleArgs): Promise<ColorRulesResponse> {
    return invoke("color_rules", {
      request: {
        projectId: args.projectId,
      },
    });
  }

  saveColorRules(args: SaveColorRulesArgs): Promise<ColorRulesResponse> {
    return invoke("save_color_rules", {
      payload: {
        projectId: args.projectId,
        rules: args.rules,
      },
    });
  }

  updateFlag(args: UpdateFlagArgs): Promise<UpdateFlagResult> {
    return invoke("update_flag", {
      payload: {
        projectId: args.projectId,
        row_index: args.rowIndex,
        ...annotationFields(args),
      },
    });
  }

  setHiddenColumns(args: HiddenColumnsArgs): Promise<void> {
    return invoke("set_hidden_columns", {
      payload: {
        projectId: args.projectId,
        hidden_columns: args.hiddenColumns,
      },
    });
  }

  exportProject(args: ExportProjectArgs): Promise<ExportResult> {
    if (!args.destination) {
      return Promise.reject(
        new Error("Destination path is required to export a project.")
      );
    }
    return invoke("export_project", {
      payload: {
        projectId: args.projectId,
        destination: args.destination,
        scope: args.scope ?? "filtered",
        search: args.search ?? null,
        searchColumns: args.searchColumns ?? null,
        condition: args.condition ?? null,
        flagFilter: args.flagFilter ?? null,
        bookmarkedOnly: args.bookmarkedOnly ?? null,
        sortKey: args.sortKey ?? null,
        sortDirection: args.sortDirection ?? null,
        columns: args.columns ?? null,
        rowIndices: args.rowIndices ?? null,
        includeAnnotations: args.includeAnnotations ?? true,
      },
    });
  }

  cancelJob(jobId: number): Promise<boolean> {
    return invoke("cancel_job", { jobId });
  }

  activeJobs(): Promise<JobInfo[]> {
    return invoke("active_jobs");
  }
}

export const JOB_STARTED_EVENT = "trivium:job-started";
export const JOB_PROGRESS_EVENT = "trivium:job-progress";

export interface JobEventHandlers {
  onStarted?: (job: JobInfo) => void;
  onProgress?: (progress: JobProgress) => void;
}

/**
 * Subscribes to background-job events and returns one unlisten function for
 * both listeners. UI wiring is deferred to T18/T23.
 */
export async function listenToJobEvents(
  handlers: JobEventHandlers
): Promise<UnlistenFn> {
  const unlistenStarted = await listen<JobInfo>(
    JOB_STARTED_EVENT,
    (event) => handlers.onStarted?.(event.payload)
  );
  try {
    const unlistenProgress = await listen<JobProgress>(
      JOB_PROGRESS_EVENT,
      (event) => handlers.onProgress?.(event.payload)
    );
    return () => {
      unlistenStarted();
      unlistenProgress();
    };
  } catch (error) {
    unlistenStarted();
    throw error;
  }
}

export function createBackend(): Backend {
  return new NativeBackend();
}
