import type * as Wire from "./generated/engine";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import type { UnlistenFn } from "@tauri-apps/api/event";
import { queueHiddenColumnWrite, waitForHiddenColumnWrites } from "./hiddenColumnWrites";
import { queueAnnotationWrite, waitForAnnotationWrites } from "./annotationWrites";
import { markAnnotationsChanged, markBulkAnnotationsChanged, forgetAnnotationVersion } from "./annotationVersions";
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
  LoadViewStateResult,
  PresetData,
  QuerySpec,
  TimeAnalysis,
  SurroundingRows,
  ProjectRow,
  ProjectSummary,
  FlagEntry,
  ViewState,
} from "./types";

export type { FlagFilterValue } from "./types";

/** Transport ownership never participates in query/result identity. */
export type ValidateConditionValuesArgs = Wire.ValidateConditionValuesPayload;
export type ConditionValuesValidation = Wire.ConditionValuesValidation;

export interface RequestOwned {
  requestId?: string;
}

export interface CreateProjectArgs extends RequestOwned {
  description?: string | null;
  path?: string;
  delimiter?: string | null;
}

/** Partial updates preserve omitted fields; explicit memo null clears it. */
export type AnnotationPatch = Partial<Omit<Wire.AnnotationPatch, "flag" | "tags" | "bookmarked">> & {
  flag?: "" | "safe" | "suspicious" | "critical";
  tags?: string[];
  bookmarked?: boolean;
};

export interface UpdateFlagArgs extends AnnotationPatch {
  projectId: string;
  rowIndex: number;
}

export type UpdateFlagResult = Omit<Wire.UpdateFlagResponse, "entry"> & {
  entry: FlagEntry | null;
};

export interface HiddenColumnsArgs {
  projectId: string;
  hiddenColumns: string[];
}

export interface LoadViewStateArgs {
  projectId: string;
}

export interface SaveViewStateArgs {
  projectId: string;
  state: ViewState;
}

export type ExportScope = "all" | "filtered" | "selected";

export interface ExportProjectArgs extends RequestOwned {
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
  entries?: IocEntry[];
}

export interface SavePresetArgs {
  projectId: string;
  name?: string | null;
  condition?: unknown;
  hiddenColumns: string[];
  sortKey?: string | null;
  sortDirection?: "asc" | "desc" | null;
  columnOrder?: string[];
  columnWidths?: Record<string, number>;
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

export interface ColumnStatsArgs extends QuerySpec, RequestOwned {
  projectId: string;
  column: string;
  rareMax?: number;
}

export interface HistogramArgs extends RequestOwned {
  projectId: string;
  column: string;
  buckets?: number;
  search?: string;
  searchColumns?: string[];
  flagFilter?: FlagFilterValue;
  bookmarkedOnly?: boolean;
  /** Deprecated visible-column field; `searchColumns` takes precedence. */
  columns?: string[];
  condition?: ConditionNode | null;
  range?: { min: string; max: string } | null;
  /** Focus the initial datetime histogram on its main populated interval. */
  focus?: boolean;
}

export interface SurroundingRowsArgs extends RequestOwned {
  projectId: string;
  rowIndex: number;
  column: string;
  mode: "neighbors" | "window";
  count: number;
}

export interface TimeAnalysisArgs extends RequestOwned {
  projectId: string;
  column: string;
  gapSeconds?: number;
  sessionMinutes?: number;
  burstWindowSeconds?: number;
  burstMin?: number;
  search?: string;
  searchColumns?: string[];
  flagFilter?: FlagFilterValue;
  bookmarkedOnly?: boolean;
  /** Deprecated visible-column field; `searchColumns` takes precedence. */
  columns?: string[];
  condition?: ConditionNode | null;
}

export interface QueryProjectRowsArgs extends QuerySpec, RequestOwned {
  projectId: string;
  offset?: number;
  limit?: number;
}

export interface LocateProjectRowArgs extends QuerySpec, RequestOwned {
  projectId: string;
  /** Original zero-based row ID, independent of result order. */
  rowIndex: number;
}

export type LocateProjectRowResponse = Wire.LocateRowResponse;
export type QueryProjectRowsResponse = Omit<Wire.QueryRowsResponse, "rows"> & { rows: ProjectRow[] };

export interface GetCellArgs extends RequestOwned {
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

export interface GroupTreeArgs extends RequestOwned {
  projectId: string;
  groupColumns: string[];
  groupGranularities?: Wire.GroupGranularity[];
  search?: string;
  searchColumns?: string[];
  flagFilter?: FlagFilterValue;
  bookmarkedOnly?: boolean;
  /** Deprecated visible-column field; `searchColumns` takes precedence. */
  columns?: string[];
  condition?: ConditionNode | null;
}

export interface BulkAnnotateArgs extends AnnotationPatch, RequestOwned {
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

export interface BulkUndoArgs extends RequestOwned {
  projectId: string;
}

export interface Backend {
  readonly isNative: boolean;
  listProjects(): Promise<ProjectSummary[]>;
  createProject(args: CreateProjectArgs): Promise<ProjectSummary>;
  deleteProject(projectId: string): Promise<void>;
  loadProject(projectId: string, requestId?: string): Promise<LoadProjectResponse>;
  validateConditionValues(args: ValidateConditionValuesArgs): Promise<ConditionValuesValidation>;
  columnStats(args: ColumnStatsArgs): Promise<ColumnStats>;
  histogram(args: HistogramArgs): Promise<Histogram>;
  timeAnalysis(args: TimeAnalysisArgs): Promise<TimeAnalysis>;
  surroundingRows(args: SurroundingRowsArgs): Promise<SurroundingRows>;
  queryProjectRows(
    args: QueryProjectRowsArgs
  ): Promise<QueryProjectRowsResponse>;
  locateProjectRow(args: LocateProjectRowArgs): Promise<LocateProjectRowResponse>;
  getCell(args: GetCellArgs): Promise<GetCellResult>;
  groupTree(args: GroupTreeArgs): Promise<GroupTreeResponse>;
  bulkPreview(args: BulkAnnotateArgs): Promise<BulkCount>;
  bulkUpdate(args: BulkAnnotateArgs): Promise<BulkApplied>;
  bulkUndo(args: BulkUndoArgs): Promise<BulkApplied>;
  saveIocs(args: SaveIocsArgs): Promise<void>;
  importIocs(args: ImportIocsArgs): Promise<IocEntry[]>;
  exportIocs(args: ExportIocsArgs): Promise<void>;
  savePreset(args: SavePresetArgs): Promise<void>;
  loadPreset(args: LoadPresetArgs): Promise<PresetData>;
  colorRules(args: ColorRuleArgs): Promise<ColorRulesResponse>;
  saveColorRules(args: SaveColorRulesArgs): Promise<ColorRulesResponse>;
  updateFlag(args: UpdateFlagArgs): Promise<UpdateFlagResult>;
  setHiddenColumns(args: HiddenColumnsArgs): Promise<void>;
  /** Rebuildable per-project view state; null when the project has none yet. */
  loadViewState(args: LoadViewStateArgs): Promise<LoadViewStateResult>;
  saveViewState(args: SaveViewStateArgs): Promise<void>;
  exportProject(args: ExportProjectArgs): Promise<ExportResult>;
  /** Requests cancellation of one running job; `false` means it already ended. */
  cancelJob(jobId: number): Promise<boolean>;
  cancelRequest(requestId: string): Promise<number>;
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

function invokeOwned<T>(command: string, wire: Record<string, unknown>, owner: RequestOwned): Promise<T> {
  return invoke<T>(command, { ...wire, requestId: owner.requestId });
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
    return invokeOwned("create_project", {
      payload: {
        path: args.path,
        description: args.description ?? null,
        delimiter: args.delimiter ?? null,
      },
    }, args);
  }

  async deleteProject(projectId: string): Promise<void> {
    await invoke("delete_project", { request: { projectId: projectId } });
    forgetAnnotationVersion(projectId);
  }

  async loadProject(projectId: string, requestId?: string): Promise<LoadProjectResponse> {
    await Promise.all([waitForHiddenColumnWrites(projectId), waitForAnnotationWrites(projectId)]);
    return invoke("load_project", {
      request: { projectId: projectId },
      metadataOnly: true,
      requestId,
    });
  }

  validateConditionValues(args: ValidateConditionValuesArgs): Promise<ConditionValuesValidation> {
    return invoke("validate_condition_values", { payload: args });
  }

  columnStats(args: ColumnStatsArgs): Promise<ColumnStats> {
    return invokeOwned("column_stats", {
      payload: {
        projectId: args.projectId,
        column: args.column,
        rareMax: args.rareMax ?? 1,
        search: args.search,
        searchColumns: args.searchColumns,
        flagFilter: args.flagFilter,
        bookmarkedOnly: args.bookmarkedOnly,
        condition: args.condition,
        sortKey: args.sortKey,
        sortDirection: args.sortDirection,
      },
    }, args);
  }

  histogram(args: HistogramArgs): Promise<Histogram> {
    return invokeOwned("histogram", {
      payload: {
        projectId: args.projectId,
        column: args.column,
        buckets: args.buckets ?? 50,
        search: args.search ?? null,
        searchColumns: args.searchColumns ?? null,
        flagFilter: args.flagFilter ?? null,
        bookmarkedOnly: args.bookmarkedOnly ?? null,
        columns: args.columns ?? null,
        condition: args.condition ?? null,
        range: args.range ?? null,
        focus: args.focus ?? false,
      },
    }, args);
  }

  timeAnalysis(args: TimeAnalysisArgs): Promise<TimeAnalysis> {
    return invokeOwned("time_analysis", {
      payload: {
        projectId: args.projectId,
        column: args.column,
        gapSeconds: args.gapSeconds ?? 60,
        sessionMinutes: args.sessionMinutes ?? 30,
        burstWindowSeconds: args.burstWindowSeconds ?? 60,
        burstMin: args.burstMin ?? 10,
        search: args.search ?? null,
        searchColumns: args.searchColumns ?? null,
        flagFilter: args.flagFilter ?? null,
        bookmarkedOnly: args.bookmarkedOnly ?? null,
        columns: args.columns ?? null,
        condition: args.condition ?? null,
      },
    }, args);
  }

  surroundingRows(args: SurroundingRowsArgs): Promise<SurroundingRows> {
    return invokeOwned("surrounding_rows", {
      payload: {
        projectId: args.projectId,
        rowIndex: args.rowIndex,
        column: args.column,
        mode: args.mode,
        count: args.count,
      },
    }, args);
  }

  queryProjectRows(
    args: QueryProjectRowsArgs
  ): Promise<QueryProjectRowsResponse> {
    return invokeOwned("query_project_rows", {
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
    }, args);
  }

  locateProjectRow(args: LocateProjectRowArgs): Promise<LocateProjectRowResponse> {
    const { requestId: _owner, ...payload } = args;
    return invokeOwned("locate_project_row", { payload }, args);
  }

  getCell(args: GetCellArgs): Promise<GetCellResult> {
    return invokeOwned("get_cell", {
      payload: {
        projectId: args.projectId,
        rowIndex: args.rowIndex,
        columnId: args.columnId,
      },
    }, args);
  }

  groupTree(args: GroupTreeArgs): Promise<GroupTreeResponse> {
    return invokeOwned("group_tree", {
      payload: {
        projectId: args.projectId,
        search: args.search ?? null,
        searchColumns: args.searchColumns ?? null,
        flagFilter: args.flagFilter ?? null,
        bookmarkedOnly: args.bookmarkedOnly ?? null,
        columns: args.columns ?? null,
        condition: args.condition ?? null,
        groupColumns: args.groupColumns,
        groupGranularities: args.groupGranularities ?? [],
      },
    }, args);
  }

  bulkPreview(args: BulkAnnotateArgs): Promise<BulkCount> {
    return invokeOwned("bulk_preview", {
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
    }, args);
  }

  async bulkUpdate(args: BulkAnnotateArgs): Promise<BulkApplied> {
    return queueAnnotationWrite(args.projectId, async () => {
      const result = await invokeOwned<BulkApplied>("bulk_update", {
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
      }, args);
      markBulkAnnotationsChanged(args.projectId);
      return result;
    });
  }

  async bulkUndo(args: BulkUndoArgs): Promise<BulkApplied> {
    return queueAnnotationWrite(args.projectId, async () => {
      const result = await invokeOwned<BulkApplied>("bulk_undo", {
        payload: {
          projectId: args.projectId,
        },
      }, args);
      markBulkAnnotationsChanged(args.projectId);
      return result;
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

  importIocs(args: ImportIocsArgs): Promise<IocEntry[]> {
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
        entries: args.entries ?? null,
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
        ...(args.columnOrder !== undefined ? { columnOrder: args.columnOrder } : {}),
        ...(args.columnWidths !== undefined ? { columnWidths: args.columnWidths } : {}),
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

  async updateFlag(args: UpdateFlagArgs): Promise<UpdateFlagResult> {
    return queueAnnotationWrite(args.projectId, async () => {
      const result = await invoke<UpdateFlagResult>("update_flag", {
        payload: {
          projectId: args.projectId,
          row_index: args.rowIndex,
          ...annotationFields(args),
        },
      });
      markAnnotationsChanged(args.projectId);
      return result;
    });
  }

  setHiddenColumns(args: HiddenColumnsArgs): Promise<void> {
    const projectId = args.projectId;
    return queueHiddenColumnWrite(projectId, args.hiddenColumns, (hiddenColumns) =>
      invoke("set_hidden_columns", {
        payload: { projectId, hidden_columns: hiddenColumns },
      })
    );
  }

  loadViewState(args: LoadViewStateArgs): Promise<LoadViewStateResult> {
    return invoke("load_view_state", {
      request: { projectId: args.projectId },
    });
  }

  saveViewState(args: SaveViewStateArgs): Promise<void> {
    return invoke("save_view_state", {
      payload: {
        projectId: args.projectId,
        state: args.state,
      },
    });
  }

  exportProject(args: ExportProjectArgs): Promise<ExportResult> {
    if (!args.destination) {
      return Promise.reject(
        new Error("Destination path is required to export a project.")
      );
    }
    return invokeOwned("export_project", {
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
    }, args);
  }

  cancelJob(jobId: number): Promise<boolean> {
    return invoke("cancel_job", { jobId });
  }

  cancelRequest(requestId: string): Promise<number> {
    return invoke("cancel_request_jobs", { requestId });
  }

  activeJobs(): Promise<JobInfo[]> {
    return invoke("active_jobs");
  }
}

export const JOB_STARTED_EVENT = "trivium:job-started";
export const JOB_PROGRESS_EVENT = "trivium:job-progress";
export const JOB_FINISHED_EVENT = "trivium:job-finished";

export interface JobEventHandlers {
  onStarted?: (job: JobInfo) => void;
  onProgress?: (progress: JobProgress) => void;
  onFinished?: (job: JobInfo) => void;
}

/**
 * Subscribes to background-job events and returns one unlisten function for
 * all listeners. ProjectView uses this to cancel the previous query when a new
 * one is applied and to drop finished job ids.
 */
export async function listenToJobEvents(
  handlers: JobEventHandlers
): Promise<UnlistenFn> {
  const unlistenStarted = await listen<JobInfo>(
    JOB_STARTED_EVENT,
    (event) => handlers.onStarted?.(event.payload)
  );
  let unlistenProgress: (() => void) | null = null;
  try {
    unlistenProgress = await listen<JobProgress>(
      JOB_PROGRESS_EVENT,
      (event) => handlers.onProgress?.(event.payload)
    );
    const unlistenFinished = await listen<JobInfo>(
      JOB_FINISHED_EVENT,
      (event) => handlers.onFinished?.(event.payload)
    );
    return () => {
      unlistenStarted();
      unlistenProgress?.();
      unlistenFinished();
    };
  } catch (error) {
    unlistenStarted();
    unlistenProgress?.();
    throw error;
  }
}

export function createBackend(): Backend {
  return new NativeBackend();
}
