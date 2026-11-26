import assert from 'node:assert/strict';
import { test } from 'node:test';
import { get } from 'svelte/store';
import { createImportPreview } from './importPreview.ts';
import { defaultImportOptions } from './importOptions.ts';
import type { ImportPreview } from './types';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

const result = (value: string): ImportPreview => ({ columns: ['value'], rows: [[value]], encoding: 'utf-8',
  bom: false, truncated: false, suggestedDelimiter: null, warnings: [] });
const tick = () => new Promise<void>(resolve => setImmediate(resolve));
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((ok, fail) => { resolve = ok; reject = fail; });
  return { promise, resolve, reject };
}

test('disabled import preview performs no read and enabling snapshots settings without changing them', async () => {
  const calls: unknown[] = [];
  const request = deferred<ImportPreview>();
  const preview = createImportPreview({
    previewImport: args => { calls.push(structuredClone(args)); return request.promise; },
    cancelRequest: async () => 0,
  });
  preview.configure(null);
  assert.equal(calls.length, 0);
  assert.deepEqual(get(preview.state), { loading: false, result: null, error: '' });
  const options = defaultImportOptions();
  preview.configure({ path: '/logs/input.csv', delimiter: ';', importOptions: options });
  assert.equal(calls.length, 1);
  assert.equal(get(preview.state).loading, true);
  options.hasHeaders = false;
  request.resolve({ ...result('sample'), suggestedDelimiter: '\t' });
  await tick();
  assert.equal(get(preview.state).result?.suggestedDelimiter, '\t');
  assert.equal(options.hasHeaders, false, 'preview never changes import choices');
  assert.equal((calls[0] as { importOptions: { hasHeaders: boolean } }).importOptions.hasHeaders, true);
  preview.dispose();
});

test('switching files or settings cancels the old preview and rejects its late response', async () => {
  const reads: Array<{ args: { requestId?: string }; job: ReturnType<typeof deferred<ImportPreview>> }> = [];
  const cancelled: string[] = [];
  const preview = createImportPreview({
    previewImport: args => {
      const job = deferred<ImportPreview>(); reads.push({ args, job }); return job.promise;
    },
    cancelRequest: async id => { cancelled.push(id); return 1; },
  });
  const request = { path: '/logs/first.csv', delimiter: ',', importOptions: defaultImportOptions() };
  preview.configure(request);
  preview.configure(structuredClone(request));
  assert.equal(reads.length, 1, 'reactive updates with the same choices do not rescan');
  preview.configure({ ...request, path: '/logs/second.csv' });
  assert.deepEqual(cancelled, [reads[0].args.requestId]);
  reads[1].job.resolve(result('second')); await tick();
  reads[0].job.resolve(result('old')); await tick();
  assert.equal(get(preview.state).result?.rows[0][0], 'second');
  preview.configure({ ...request, path: '/logs/second.csv', delimiter: '\t' });
  assert.equal(reads.length, 3);
  assert.equal(get(preview.state).result, null, 'a previous delimiter preview is not presented as current');
  reads[2].job.reject(new Error('Bad selected encoding')); await tick();
  assert.match(get(preview.state).error, /Bad selected encoding/);
  preview.dispose();
});

test('turning preview off or closing its component cancels work and ignores late errors', async () => {
  const reads: Array<ReturnType<typeof deferred<ImportPreview>>> = [];
  let cancelled = 0;
  const preview = createImportPreview({
    previewImport: () => { const job = deferred<ImportPreview>(); reads.push(job); return job.promise; },
    cancelRequest: async () => ++cancelled,
  });
  preview.configure({ path: '/logs/input.csv' });
  preview.configure(null);
  reads[0].reject(new Error('Cancelled old read')); await tick();
  assert.deepEqual(get(preview.state), { loading: false, result: null, error: '' });
  assert.equal(cancelled, 1);
  preview.configure({ path: '/logs/input.csv' });
  preview.dispose();
  preview.configure({ path: '/logs/ignored.csv' });
  reads[1].resolve(result('closed')); await tick();
  assert.equal(reads.length, 2);
  assert.equal(cancelled, 2);
});

test('the real preview component starts off and suggestions change delimiter only after explicit selection', () => runSvelteTest(String.raw`
const Preview=await compileHeadless('../app/ImportPreview');
const calls=[];
const backend={previewImport:async args=>{calls.push(args);return {columns:['a','b'],rows:[['1','2']],encoding:'utf-8',bom:false,truncated:true,suggestedDelimiter:'\t',warnings:[]};},cancelRequest:async()=>0};
const options={encoding:'auto',hasHeaders:true,recover:false};
const component=await mountHeadless(Preview,{backend,path:'/logs/a.csv',delimiter:'',options});
await tick();await new Promise(r=>setImmediate(r));
assert.equal(component.$capture_state().enabled,false);assert.equal(calls.length,0);
const selected=[];component.$on('delimiter',event=>selected.push(event.detail));
component.$inject_state({enabled:true});await tick();await new Promise(r=>setImmediate(r));await tick();
assert.equal(calls.length,1);assert.deepEqual(selected,[]);
component.$capture_state().applySuggestion();assert.deepEqual(selected,['\t']);
assert.deepEqual(options,{encoding:'auto',hasHeaders:true,recover:false});
component.$inject_state({enabled:false});await tick();component.$destroy();
`));

test('report export uses native overwrite approval, reports failures, and never changes saved diagnostics', () => runSvelteTest(String.raw`
globalThis.HTMLElement=class {};
globalThis.window={};
const {mockIPC,clearMocks}=await import('@tauri-apps/api/mocks');
let chosen='/output/report.jsonl';
mockIPC((command)=>{assert.equal(command,'plugin:dialog|save');return chosen;});
const Report=await compileHeadless('../app/ImportReportDialog');
const report={encoding:'utf-8',bom:false,hasHeaders:true,sourceRecords:3,importedRecords:2,skippedRecords:1,repairedRecords:0,generatedColumns:0,unreadableTail:false,issueCount:1,issues:[]};
const saved=structuredClone(report),calls=[];let failure=false;
const backend={exportImportReport:async args=>{calls.push(args);if(failure)throw new Error('Disk unavailable');return {destination:args.destination,issuesWritten:1,complete:true};}};
const dialog=await mountHeadless(Report,{report,fileName:'source.csv',format:'csv',projectId:'owner',backend});
const focus=[];
const closeButton={focus(){focus.push('close');}},reportBody={focus(){focus.push('report');}},exportButton={focus(){focus.push('export');}};
dialog.$inject_state({closeButton,reportBody,exportButton});await tick();
const key=shiftKey=>({key:'Tab',shiftKey,stopPropagation(){},preventDefault(){}});
document.activeElement=reportBody;dialog.$capture_state().keyboard(key(false));
document.activeElement=exportButton;dialog.$capture_state().keyboard(key(false));
document.activeElement=closeButton;dialog.$capture_state().keyboard(key(true));
assert.deepEqual(focus,['export','close','export']);
await dialog.$capture_state().exportReport();
assert.deepEqual(calls,[{projectId:'owner',destination:'/output/report.jsonl',allowOverwrite:true}]);
assert.equal(dialog.$capture_state().notice,'Report exported.');
failure=true;await dialog.$capture_state().exportReport();assert.match(dialog.$capture_state().error,/Disk unavailable/);
chosen=null;await dialog.$capture_state().exportReport();assert.equal(calls.length,2);
assert.deepEqual(report,saved);dialog.$destroy();clearMocks();
`));

test('a report picker returning after the report closes cannot export a different case', () => runSvelteTest(String.raw`
globalThis.HTMLElement=class {};globalThis.window={};
const {mockIPC,clearMocks}=await import('@tauri-apps/api/mocks');
let finish;mockIPC(command=>{assert.equal(command,'plugin:dialog|save');return new Promise(resolve=>{finish=resolve;});});
const Report=await compileHeadless('../app/ImportReportDialog');
const report={encoding:'utf-8',bom:false,hasHeaders:null,sourceRecords:0,importedRecords:0,skippedRecords:0,repairedRecords:0,generatedColumns:0,unreadableTail:false,issueCount:0,issues:[]};
let writes=0;
const backend={exportImportReport:async()=>{writes++;throw new Error('No write should be accepted');}};
const dialog=await mountHeadless(Report,{report,fileName:'source.jsonl',format:'jsonl',projectId:'owner',backend});
const operation=dialog.$capture_state().exportReport();
dialog.$destroy();finish('/output/late.jsonl');await operation;
assert.equal(writes,0);clearMocks();
`));
