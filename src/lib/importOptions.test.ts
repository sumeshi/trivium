import assert from 'node:assert/strict';
import { test } from 'node:test';
import { get } from 'svelte/store';
import { clearMocks, mockIPC } from '@tauri-apps/api/mocks';
import { createBackend, type Backend, type CreateProjectArgs } from './backend.ts';
import { createProjectController } from './stores/projects.ts';
import { defaultImportOptions, importNeedsAttention, importReportSummary } from './importOptions.ts';
import type { ImportOptions, ImportReport, LoadProjectResponse, ProjectSummary } from './types';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

function fixture(onCreate?: (args: CreateProjectArgs) => void) {
  const calls: CreateProjectArgs[] = [];
  const projects: ProjectSummary[] = [];
  let failure = false;
  const backend = {
    createProject: async (args: CreateProjectArgs) => {
      calls.push(structuredClone(args));
      onCreate?.(args);
      if (failure) throw new Error('Unreadable source record');
      const project: ProjectSummary = { meta: { id: String(projects.length), name: args.path!,
        created_at: '2026-11-26', total_records: 2, flagged_records: 0, ioc_applied_records: 0, hidden_columns: [] } };
      projects.push(project);
      return project;
    },
    listProjects: async () => projects,
    loadProject: async (id: string): Promise<LoadProjectResponse> => ({ project: projects.find(p => p.meta.id === id)!,
      columns: [], columns_meta: [], column_max_chars: {}, column_kinds: {}, color_rules: [], iocs: [], hidden_columns: [], initial_rows: [] }),
  } as Backend;
  return { controller: createProjectController(backend), calls, fail: (next: boolean) => { failure = next; } };
}

test('recovery is explicitly selected, incompatible annotation restoration is cleared, and a new selection starts strict', async () => {
  const f = fixture();
  assert.deepEqual(get(f.controller.state).pendingImportOptions, { encoding: 'auto', hasHeaders: true, recover: false });
  f.controller.setPendingFiles(['/evidence/input.csv']);
  f.controller.setPendingRestoreAnnotations(true);
  const options: ImportOptions = { encoding: 'windows-1252', hasHeaders: false, recover: true };
  f.controller.setPendingImportOptions(options);
  options.recover = false;
  f.controller.setPendingRestoreAnnotations(true);
  assert.equal(get(f.controller.state).pendingRestoreAnnotations, false);
  assert.equal(get(f.controller.state).pendingImportOptions.recover, true);
  await f.controller.createProjects();
  assert.deepEqual(f.calls[0].importOptions, { encoding: 'windows-1252', hasHeaders: false, recover: true });
  assert.equal(f.calls[0].restoreAnnotations, false);
  f.controller.setPendingFiles(['/evidence/other.csv']);
  assert.deepEqual(get(f.controller.state).pendingImportOptions, defaultImportOptions());
  await f.controller.createProjects();
  assert.deepEqual(f.calls[1].importOptions, defaultImportOptions());
  f.controller.dispose();
});

test('one import batch snapshots its recovery choices and keeps CSV header settings out of JSON', async () => {
  const f = fixture((args) => {
    // Neither a caller changing the form nor a transport mutating its arguments
    // may alter the choices already accepted for the remaining files.
    f.controller.setPendingImportOptions(defaultImportOptions());
    if (args.importOptions) args.importOptions.recover = false;
  });
  f.controller.setPendingFiles(['/evidence/a.csv', '/evidence/b.jsonl', '/evidence/c.csv']);
  f.controller.setPendingImportOptions({ encoding: 'shift-jis', hasHeaders: false, recover: true });
  await f.controller.createProjects();
  assert.deepEqual(f.calls.map(call => call.importOptions), [
    { encoding: 'shift-jis', hasHeaders: false, recover: true },
    { encoding: 'shift-jis', hasHeaders: true, recover: true },
    { encoding: 'shift-jis', hasHeaders: false, recover: true },
  ]);
  f.controller.dispose();
});

test('a failed recovery import keeps its choices for retry and explicit reset clears them', async () => {
  const f = fixture();
  f.controller.setPendingFiles(['/evidence/broken.csv']);
  const options: ImportOptions = { encoding: 'utf-16le', hasHeaders: false, recover: true };
  f.controller.setPendingImportOptions(options);
  f.fail(true);
  const failed = await f.controller.createProjects();
  assert.equal(failed.failures.length, 1);
  assert.deepEqual(get(f.controller.state).pendingImportOptions, options);
  f.fail(false);
  await f.controller.createProjects();
  assert.deepEqual(f.calls.at(-1)?.importOptions, options);
  assert.deepEqual(get(f.controller.state).pendingImportOptions, defaultImportOptions());
  f.controller.dispose();
});

test('native creation passes the explicit import options and leaves older callers defaulted by the engine', async () => {
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'window');
  Object.defineProperty(globalThis, 'window', { value: {}, configurable: true });
  const payloads: Record<string, unknown>[] = [];
  mockIPC((command, args) => {
    assert.equal(command, 'create_project');
    payloads.push((args as { payload: Record<string, unknown> }).payload);
    return {};
  });
  try {
    const backend = createBackend();
    await backend.createProject({ path: '/evidence/log.jsonl' });
    assert.equal(Object.hasOwn(payloads[0], 'importOptions'), false);
    await backend.createProject({ path: '/evidence/log.jsonl', importOptions: { encoding: 'utf-8', hasHeaders: true, recover: true } });
    assert.deepEqual(payloads[1].importOptions, { encoding: 'utf-8', hasHeaders: true, recover: true });
  } finally {
    clearMocks();
    if (previous) Object.defineProperty(globalThis, 'window', previous);
    else Reflect.deleteProperty(globalThis, 'window');
  }
});

test('import status distinguishes repairs and known skips from an unreadable, uncounted remainder', () => {
  const report: ImportReport = { encoding: 'UTF-8', bom: false, hasHeaders: null, sourceRecords: 8,
    importedRecords: 5, skippedRecords: 3, repairedRecords: 0, generatedColumns: 0, unreadableTail: true,
    issueCount: 3, issues: [] };
  assert.equal(importNeedsAttention(undefined), false);
  assert.equal(importNeedsAttention(report), true);
  assert.equal(importReportSummary(report), '3 skipped · unreadable remainder');
  assert.equal(importNeedsAttention({ ...report, skippedRecords: 0, unreadableTail: false }), false);
  assert.equal(importReportSummary({ ...report, skippedRecords: 0, unreadableTail: false }), '5 rows imported');
});

test('import controls emit independent choices without changing the selected options object', () => runSvelteTest(String.raw`
const Controls=await compileHeadless('../app/ImportOptions');
const original={encoding:'auto',hasHeaders:true,recover:false};
const controls=await mountHeadless(Controls,{options:original,csv:true});
const changes=[];
controls.$on('change',event=>changes.push(event.detail));
controls.$capture_state().update({recover:true});
assert.deepEqual(original,{encoding:'auto',hasHeaders:true,recover:false});
assert.deepEqual(changes,[{encoding:'auto',hasHeaders:true,recover:true}]);
controls.$destroy();
`));

test('the saved import report allows keyboard scrolling, traps focus, and Escape closes without changing data', () => runSvelteTest(String.raw`
globalThis.HTMLElement=class {};
const Report=await compileHeadless('../app/ImportReportDialog');
const report={encoding:'UTF-8',bom:false,hasHeaders:null,sourceRecords:2,importedRecords:1,skippedRecords:1,repairedRecords:0,generatedColumns:0,unreadableTail:false,issueCount:1,issues:[]};
const dialog=await mountHeadless(Report,{report,fileName:'evidence.jsonl',format:'jsonl'});
const focus=[];let closed=0;
const closeButton={focus(){focus.push('close');}},reportBody={focus(){focus.push('report');}};
dialog.$inject_state({closeButton,reportBody});await tick();
dialog.$on('close',()=>closed++);
const keyboard=dialog.$capture_state().keyboard;
const event=(key,extra={})=>({key,stopPropagation(){},preventDefault(){},...extra});
document.activeElement=closeButton;keyboard(event('Tab'));
document.activeElement=reportBody;keyboard(event('Tab',{shiftKey:true}));
keyboard(event('ArrowDown',{preventDefault(){throw new Error('Report scrolling must remain available');}}));
keyboard(event('Enter'));assert.equal(closed,0);
keyboard(event('Escape'));assert.equal(closed,1);
assert.deepEqual(focus,['report','close']);
assert.equal(report.skippedRecords,1);
dialog.$destroy();
`));
