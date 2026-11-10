import { invoke } from "@tauri-apps/api/tauri";
import type {
  ColorRule,
  ConditionNode,
  IocEntry,
  BulkApplied,
  BulkCount,
  ColumnStats,
  GroupTreeResponse,
  Histogram,
  LoadProjectResponse,
  PresetData,
  TimeAnalysis,
  SurroundingRows,
  ProjectRow,
  ProjectSummary,
} from "./types";

export interface CreateProjectArgs {
  description?: string | null;
  path?: string;
  delimiter?: string | null;
}

export interface UpdateFlagArgs {
  projectId: string;
  rowIndex: number;
  flag: string;
  memo: string | null;
  tags?: string[] | null;
  bookmarked?: boolean | null;
}

export interface HiddenColumnsArgs {
  projectId: string;
  hiddenColumns: string[];
}

export interface ExportProjectArgs {
  projectId: string;
  destination?: string;
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

export type FlagFilterValue =
  | "all"
  | "none"
  | "priority"
  | "safe"
  | "suspicious"
  | "critical";

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

export interface QueryProjectRowsArgs {
  projectId: string;
  search?: string;
  flagFilter?: FlagFilterValue;
  bookmarkedOnly?: boolean;
  columns?: string[];
  offset?: number;
  limit?: number;
  sortKey?: string | null;
  sortDirection?: "asc" | "desc";
  condition?: ConditionNode | null;
}

export interface QueryProjectRowsResponse {
  rows: ProjectRow[];
  total_flagged: number;
  total_rows: number;
  total_filtered_rows: number;
  offset: number;
}

export interface GroupTreeArgs {
  projectId: string;
  groupColumns: string[];
  search?: string;
  flagFilter?: FlagFilterValue;
  columns?: string[];
  condition?: ConditionNode | null;
}

export interface BulkAnnotateArgs {
  projectId: string;
  target: "selection" | "filter" | "all";
  rowIndices?: number[];
  search?: string;
  flagFilter?: FlagFilterValue;
  columns?: string[];
  condition?: ConditionNode | null;
  flag: string;
  memo: string | null;
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
  updateFlag(args: UpdateFlagArgs): Promise<ProjectRow>;
  setHiddenColumns(args: HiddenColumnsArgs): Promise<void>;
  exportProject(args: ExportProjectArgs): Promise<void>;
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
        search: args.search ?? null,
        flagFilter: args.flagFilter ?? null,
        bookmarkedOnly: args.bookmarkedOnly ?? null,
        columns: args.columns ?? null,
        offset: args.offset ?? null,
        limit: args.limit ?? null,
        sortKey: args.sortKey ?? null,
        sortDirection: args.sortDirection ?? null,
        condition: args.condition ?? null,
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
        flagFilter: args.flagFilter ?? null,
        columns: args.columns ?? null,
        condition: args.condition ?? null,
        flag: args.flag,
        memo: args.memo,
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
        flagFilter: args.flagFilter ?? null,
        columns: args.columns ?? null,
        condition: args.condition ?? null,
        flag: args.flag,
        memo: args.memo,
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

  updateFlag(args: UpdateFlagArgs): Promise<ProjectRow> {
    return invoke("update_flag", {
      payload: {
        projectId: args.projectId,
        row_index: args.rowIndex,
        flag: args.flag,
        memo: args.memo,
        tags: args.tags ?? null,
        bookmarked: args.bookmarked ?? null,
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

  exportProject(args: ExportProjectArgs): Promise<void> {
    if (!args.destination) {
      return Promise.reject(
        new Error("Destination path is required to export a project.")
      );
    }
    return invoke("export_project", {
      payload: {
        projectId: args.projectId,
        destination: args.destination,
      },
    });
  }
}

export function createBackend(): Backend {
  return new NativeBackend();
}
