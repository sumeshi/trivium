import type { JobInfo, JobProgress } from './types';
import type { FormatDefinition } from './formats';
import type * as Wire from './generated/engine';

export type ExtractPayload = Wire.ExtractPayload;
export type ExtractValue = Wire.ExtractValue;
export type ExtractResult = Wire.ExtractResult;
export type ExtractPage = Wire.ExtractPage;
export type ExtractSource = Wire.ExtractSource;
export type ExtractSources = Wire.ExtractSources;
export interface ExtractExport { rows: number; columns: string[]; warnings: string[] }
export interface ExtractColumn { id: string; label: string }

/** Only metadata survives a tab switch; result pages belong to the active view. */
export interface ExtractIntent {
  request: ExtractPayload;
  sourceName: string;
  sourceColumns: ExtractColumn[];
}
export interface ExtractTab extends ExtractIntent { id: string; result: ExtractResult }
export interface ExtractPending extends ExtractIntent {
  requestId: string;
  jobId: number | null;
  done: number;
  total: number | null;
  message: string;
}

export interface ExtractBackend {
  extractValues(args: ExtractPayload & { requestId?: string }): Promise<ExtractResult>;
  extractValuesPage(args: { resultId: string; offset: number; limit: number; requestId?: string }): Promise<ExtractPage>;
  extractValueSources(args: { resultId: string; value: string; offset: number; limit: number; requestId?: string }): Promise<ExtractSources>;
  exportExtract(args: { resultId: string; destination: string; allowOverwrite?: boolean; requestId?: string }): Promise<ExtractExport>;
  releaseExtractResult(resultId: string): Promise<boolean>;
  cancelRequest(requestId: string): Promise<number>;
}

export interface ExtractJobHandlers {
  started(job: JobInfo): void;
  progress(progress: JobProgress): void;
  finished(job: JobInfo): void;
}

export interface ExtractFilter {
  projectId: string;
  columns: string[];
  format: FormatDefinition;
  value: string;
}
export interface ExtractNavigation { projectId: string; rowIndex: number | null }
