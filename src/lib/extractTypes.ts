import type { ProjectSummary } from './types';
import type * as Wire from './generated/engine';

export type ExtractPayload = Wire.ExtractPayload;
export interface ExtractIntent { request: ExtractPayload; sourceName: string }
export interface ExtractPending extends ExtractIntent {
  requestId: string;
  jobId: number | null;
  done: number;
  total: number | null;
  message: string;
}

/** A scan produces a CSV project for the ordinary viewer. */
export interface ExtractBackend {
  extractValues(args: ExtractPayload & { requestId?: string }): Promise<ProjectSummary>;
  deleteProject(projectId: string): Promise<void>;
  cancelRequest(requestId: string): Promise<number>;
}
