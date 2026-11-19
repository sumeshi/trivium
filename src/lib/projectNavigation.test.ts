import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

const appFixture = String.raw`
globalThis.window={};
globalThis.HTMLElement=class {};
document.querySelector=()=>null;
const {get}=await import('svelte/store');
const {mockIPC,clearMocks}=await import('@tauri-apps/api/mocks');
const {reopenSaveAdmission}=await import('./src/lib/saveDrain.ts');
const summary=id=>({meta:{id,name:id,source_file_name:id+'.csv',created_at:'2026-11-19',
  total_records:1,flagged_records:0,ioc_applied_records:0,hidden_columns:[]}});
const detail=id=>({project:summary(id),columns:['c1'],columns_meta:[],column_max_chars:{},
  column_kinds:{},color_rules:[],iocs:[],hidden_columns:[],initial_rows:[]});
const failReads=new Set();
let failDelete=false,finished=0;
mockIPC((command,args)=>{
  if(command==='list_projects') return [summary('a'),summary('b')];
  if(command==='load_project') {
    const id=args.request.projectId;
    if(failReads.delete(id)) throw new Error('Project data file missing.');
    return detail(id);
  }
  if(command==='delete_project') {
    if(failDelete) throw new Error('Delete failed.');
    return;
  }
  if(command==='finish_app_close') { finished++;return; }
  if(command==='cancel_app_close') return;
  if(command==='cancel_request_jobs') return 0;
  throw new Error('Unexpected IPC '+command);
});
const App=await compileHeadless('../../../App');
const app=await mountHeadless(App);
app.$inject_state({closeReady:true});
reopenSaveAdmission();
await tick();
const state=app.$capture_state();
await state.projectController.loadProjects();
`;

test('recovered project reads do not report a failed save on normal close', () => runSvelteTest(appFixture + String.raw`
failReads.add('a');
await state.handleSelectProject('a');
assert.match(get(state.projectState).detailError,/file missing/);
await state.handleSelectProject('a');
assert.equal(get(state.projectState).detailError,'');
failReads.add('a');
await state.handleRefreshRequest();
assert.match(get(state.projectState).detailError,/file missing/);
await state.handleSelectProject('a');
await state.handleSelectProject('b');
failReads.add('a');
await state.handleCloseTab('b');
assert.deepEqual(get(state.projectState).openProjectIds,['a']);
assert.match(get(state.projectState).detailError,/file missing/);
await state.handleSelectProject('a');
await state.close.request();
assert.equal(finished,1,'Recovered read failures must not poison the save drain');
assert.notEqual(get(state.closeState).phase,'failed');
app.$destroy();
clearMocks();
`));

test('a failed temporary-project delete still prevents ordinary close', () => runSvelteTest(appFixture + String.raw`
await state.projectController.adoptTemporaryProject(summary('temporary'));
failDelete=true;
await state.handleCloseTab('temporary');
assert.ok(get(state.projectState).temporaryProjectIds.includes('temporary'));
await state.close.request();
assert.equal(finished,0);
assert.equal(get(state.closeState).phase,'failed');
assert.match(get(state.closeState).message,/save or export failed/);
app.$destroy();
clearMocks();
`));
