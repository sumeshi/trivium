import { invoke } from '@tauri-apps/api/tauri';
import type {
  LoadProjectResponse,
  ProjectMeta,
  ProjectRow,
  ProjectSummary
} from './types';

export interface CreateProjectArgs {
  description?: string | null;
  path?: string;
  file?: File;
}

export interface UpdateFlagArgs {
  projectId: string;
  rowIndex: number;
  flag: string;
  memo: string | null;
}

export interface HiddenColumnsArgs {
  projectId: string;
  hiddenColumns: string[];
}

export interface ExportProjectArgs {
  projectId: string;
  destination?: string;
}

export interface Backend {
  readonly isNative: boolean;
  listProjects(): Promise<ProjectSummary[]>;
  createProject(args: CreateProjectArgs): Promise<ProjectSummary>;
  deleteProject(projectId: string): Promise<void>;
  loadProject(projectId: string): Promise<LoadProjectResponse>;
  updateFlag(args: UpdateFlagArgs): Promise<ProjectRow>;
  setHiddenColumns(args: HiddenColumnsArgs): Promise<void>;
  exportProject(args: ExportProjectArgs): Promise<void>;
}

type RecordData = Record<string, unknown>;

interface WebProjectData {
  summary: ProjectSummary;
  columns: string[];
  rows: ProjectRow[];
  hiddenColumns: string[];
}

export function createBackend(): Backend {
  const hasNativeBridge =
    typeof window !== 'undefined' &&
    typeof (window as Window & { __TAURI_IPC__?: unknown }).__TAURI_IPC__ === 'function';

  return hasNativeBridge ? new NativeBackend() : new WebBackend();
}

type LegacyProjectSummary = ProjectMeta & {
  flagged_records: number;
  hidden_columns?: string[];
};

type RawProjectSummary = ProjectSummary | LegacyProjectSummary;

interface RawLoadProjectResponse {
  project: RawProjectSummary;
  columns: string[];
  rows: ProjectRow[];
  hidden_columns?: string[];
}

class NativeBackend implements Backend {
  readonly isNative = true;

  listProjects(): Promise<ProjectSummary[]> {
    return invoke<RawProjectSummary[]>('list_projects').then((items) =>
      items.map(normalizeSummary)
    );
  }

  createProject(args: CreateProjectArgs): Promise<ProjectSummary> {
    if (!args.path) {
      return Promise.reject(new Error('Path is required to create a project.'));
    }
    return invoke<RawProjectSummary>('create_project', {
      payload: {
        path: args.path,
        description: args.description ?? null
      }
    }).then(normalizeSummary);
  }

  deleteProject(projectId: string): Promise<void> {
    return invoke('delete_project', { request: { project_id: projectId } });
  }

  loadProject(projectId: string): Promise<LoadProjectResponse> {
    return invoke<RawLoadProjectResponse>('load_project', {
      request: { project_id: projectId }
    }).then(normalizeLoadResponse);
  }

  updateFlag(args: UpdateFlagArgs): Promise<ProjectRow> {
    return invoke<ProjectRow>('update_flag', {
      payload: {
        project_id: args.projectId,
        row_index: args.rowIndex,
        flag: args.flag,
        memo: args.memo
      }
    });
  }

  setHiddenColumns(args: HiddenColumnsArgs): Promise<void> {
    return invoke('set_hidden_columns', {
      payload: {
        project_id: args.projectId,
        hidden_columns: args.hiddenColumns
      }
    });
  }

  exportProject(args: ExportProjectArgs): Promise<void> {
    if (!args.destination) {
      return Promise.reject(new Error('Destination path is required to export a project.'));
    }
    return invoke('export_project', {
      payload: {
        project_id: args.projectId,
        destination: args.destination
      }
    });
  }
}

class WebBackend implements Backend {
  readonly isNative = false;
  private projects = new Map<string, WebProjectData>();

  async listProjects(): Promise<ProjectSummary[]> {
    const items = Array.from(this.projects.values());
    return items
      .map((item) => cloneSummary(item.summary))
      .sort((a, b) => b.meta.created_at.localeCompare(a.meta.created_at));
  }

  async createProject(args: CreateProjectArgs): Promise<ProjectSummary> {
    if (!args.file) {
      throw new Error('File is required to create a project in web mode.');
    }
    const parsed = await this.parseCsvFile(args.file);
    const id = this.randomId();
    const createdAt = new Date().toISOString();
    const meta: ProjectMeta = {
      id,
      name: args.file.name || 'Untitled.csv',
      description: args.description ?? null,
      created_at: createdAt,
      total_records: parsed.rows.length,
      hidden_columns: []
    };
    const summary: ProjectSummary = {
      meta,
      flagged_records: 0
    };
    this.projects.set(id, {
      summary,
      columns: parsed.columns,
      rows: parsed.rows,
      hiddenColumns: []
    });
    return cloneSummary(summary);
  }

  async deleteProject(projectId: string): Promise<void> {
    this.projects.delete(projectId);
  }

  async loadProject(projectId: string): Promise<LoadProjectResponse> {
    const project = this.projects.get(projectId);
    if (!project) {
      throw new Error('Project not found.');
    }
    return {
      project: cloneSummary(project.summary),
      columns: [...project.columns],
      rows: project.rows.map(cloneRow),
      hidden_columns: [...project.hiddenColumns]
    };
  }

  async updateFlag(args: UpdateFlagArgs): Promise<ProjectRow> {
    const project = this.projects.get(args.projectId);
    if (!project) {
      throw new Error('Project not found.');
    }
    const target = project.rows.find((row) => row.row_index === args.rowIndex);
    if (!target) {
      throw new Error('Row not found.');
    }
    target.flag = args.flag;
    target.memo = args.memo;
    project.summary.flagged_records = project.rows.filter((row) => row.flag.trim().length > 0).length;
    return cloneRow(target);
  }

  async setHiddenColumns(args: HiddenColumnsArgs): Promise<void> {
    const project = this.projects.get(args.projectId);
    if (!project) {
      throw new Error('Project not found.');
    }
    project.hiddenColumns = [...args.hiddenColumns];
    project.summary.meta.hidden_columns = [...args.hiddenColumns];
  }

  async exportProject(args: ExportProjectArgs): Promise<void> {
    const project = await this.loadProject(args.projectId);
    const columns = [...project.columns];
    const header = [...columns, 'flag', 'memo'];
    const lines: string[][] = [header];
    for (const row of project.rows) {
      const values = columns.map((column) => stringifyCell(row.data[column]));
      values.push(row.flag ?? '');
      values.push(row.memo ?? '');
      lines.push(values);
    }
    const csv = lines.map((line) => line.map(escapeCsvCell).join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download =
      (project.project.meta.name?.replace(/\.[^.]+$/, '') || 'trivium-export') + '-web.csv';
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    URL.revokeObjectURL(url);
  }

  private async parseCsvFile(file: File): Promise<{ columns: string[]; rows: ProjectRow[] }> {
    const text = await file.text();
    const table = parseCsv(text);
    if (!table.length) {
      throw new Error('CSV file is empty.');
    }
    const [header, ...body] = table;
    const columns = header.map((column, index) => column || `column_${index + 1}`);
    const rows: ProjectRow[] = body
      .filter((record) => record.some((cell) => cell.trim().length > 0))
      .map((record, rowIndex) => {
        const data: RecordData = {};
        for (let i = 0; i < columns.length; i += 1) {
          data[columns[i]] = record[i] ?? '';
        }
        return {
          row_index: rowIndex,
          data,
          flag: '',
          memo: ''
        };
      });
    return { columns, rows };
  }

  private randomId(): string {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }
    return `web-${Date.now().toString(16)}-${Math.random().toString(16).slice(2, 10)}`;
  }
}

function parseCsv(content: string): string[][] {
  const rows: string[][] = [];
  let currentRow: string[] = [];
  let currentField = '';
  let inQuotes = false;

  for (let i = 0; i < content.length; i += 1) {
    const char = content[i];
    const next = content[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        currentField += '"';
        i += 1;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }

    if (char === ',' && !inQuotes) {
      currentRow.push(currentField);
      currentField = '';
      continue;
    }

    if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && next === '\n') {
        i += 1;
      }
      currentRow.push(currentField);
      if (!isEmptyRow(currentRow)) {
        rows.push([...currentRow]);
      }
      currentRow = [];
      currentField = '';
      continue;
    }

    currentField += char;
  }

  if (currentField.length > 0 || currentRow.length > 0) {
    currentRow.push(currentField);
    if (!isEmptyRow(currentRow)) {
      rows.push([...currentRow]);
    }
  }

  return rows.filter((row) => row.length > 0);
}

function isEmptyRow(fields: string[]): boolean {
  return fields.every((field) => field === '');
}

function stringifyCell(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

function escapeCsvCell(value: string): string {
  if (/[",\n\r]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function normalizeSummary(raw: RawProjectSummary): ProjectSummary {
  if ('meta' in raw && raw.meta) {
    const hiddenColumns = Array.isArray(raw.meta.hidden_columns) ? raw.meta.hidden_columns : [];
    return {
      flagged_records: raw.flagged_records ?? 0,
      meta: {
        ...raw.meta,
        hidden_columns: [...hiddenColumns]
      }
    };
  }

  const legacy = raw as LegacyProjectSummary;
  const { flagged_records, hidden_columns, id, name, description, created_at, total_records } =
    legacy;
  const normalizedHidden = Array.isArray(hidden_columns) ? hidden_columns : [];
  return {
    flagged_records,
    meta: {
      id,
      name,
      description: description ?? null,
      created_at,
      total_records,
      hidden_columns: [...normalizedHidden]
    }
  };
}

function normalizeLoadResponse(raw: RawLoadProjectResponse): LoadProjectResponse {
  return {
    project: normalizeSummary(raw.project),
    columns: [...raw.columns],
    rows: raw.rows.map(cloneRow),
    hidden_columns: Array.isArray(raw.hidden_columns) ? [...raw.hidden_columns] : []
  };
}

function cloneSummary(summary: ProjectSummary): ProjectSummary {
  const normalized = normalizeSummary(summary);
  return {
    flagged_records: normalized.flagged_records,
    meta: {
      ...normalized.meta,
      hidden_columns: [...normalized.meta.hidden_columns]
    }
  };
}

function cloneRow(row: ProjectRow): ProjectRow {
  return {
    row_index: row.row_index,
    flag: row.flag,
    memo: row.memo,
    data: { ...row.data }
  };
}
