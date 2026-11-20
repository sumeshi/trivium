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
const loaded=[],deleted=[],extractionJobs=[],deleteJobs=[];
let failDelete=false,holdDeletes=false,finished=0;
mockIPC((command,args)=>{
  if(command==='list_projects') return [summary('a'),summary('b')];
  if(command==='load_project') {
    const id=args.request.projectId;
    loaded.push(id);
    if(failReads.delete(id)) throw new Error('Project data file missing.');
    return detail(id);
  }
  if(command==='delete_project') {
    deleted.push(args.request.projectId);
    if(failDelete) throw new Error('Delete failed.');
    if(holdDeletes) return new Promise(resolve=>deleteJobs.push({id:args.request.projectId,resolve}));
    return;
  }
  if(command==='finish_app_close') { finished++;return; }
  if(command==='cancel_app_close') return;
  if(command==='cancel_request_jobs') return 0;
  if(command==='extract_values') return new Promise(resolve=>extractionJobs.push({request:args.payload,resolve}));
  throw new Error('Unexpected IPC '+command);
});
const App=await compileHeadless('../../../App');
const app=await mountHeadless(App);
app.$inject_state({closeReady:true});
reopenSaveAdmission();
await tick();
const state=app.$capture_state();
await state.projectController.loadProjects();
const extract=()=>state.startExtraction({sourceName:'a.csv',request:{projectId:'a',columns:['c1'],scope:'filtered',
  search:'host:ws01',format:{schemaVersion:1,id:'ipv4',name:'IPv4',pattern:'[0-9.]+'}}});
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

test('successive extractions keep the source workspace active and only load result rows when selected', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');
const sourceDetail=get(state.projectState).projectDetail;
for(let index=0;index<2;index++){
 const operation=extract();
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(extractionJobs.length,index+1);
 assert.equal(extractionJobs[index].request.projectId,'a');
 extractionJobs[index].resolve(summary('result-'+index));
 await operation;
 assert.equal(get(state.projectState).selectedProjectId,'a');
 assert.equal(get(state.projectState).projectDetail,sourceDetail,'the mounted source controls remain intact');
 assert.equal(get(state.extractState).pending,null);
}
assert.deepEqual(get(state.projectState).openProjectIds,['a','result-0','result-1']);
assert.deepEqual(get(state.projectState).temporaryProjectIds,['result-0','result-1']);
assert.deepEqual(loaded,['a'],'background result tabs hold only metadata');
await state.handleSelectProject('result-0');
assert.equal(get(state.projectState).selectedProjectId,'result-0');
assert.deepEqual(loaded,['a','result-0']);
await state.handleSelectProject('a');
assert.equal(get(state.projectState).projectDetail.project.meta.id,'a');
app.$destroy();
clearMocks();
`));

test('an extraction completing after navigation adds a background tab without replacing the selected project', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');
const operation=extract();
await new Promise(resolve=>setImmediate(resolve));
await state.handleSelectProject('b');
const selectedDetail=get(state.projectState).projectDetail;
extractionJobs[0].resolve(summary('late-result'));
await operation;
assert.equal(get(state.projectState).selectedProjectId,'b');
assert.equal(get(state.projectState).projectDetail,selectedDetail);
assert.deepEqual(get(state.projectState).openProjectIds,['a','b','late-result']);
assert.deepEqual(loaded,['a','b']);
app.$destroy();
clearMocks();
`));

test('Close others keeps the target workspace and never deletes saved cases or loads closing tabs', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');await state.handleSelectProject('b');
await state.projectController.adoptTemporaryProject(summary('temporary'),false);
await state.handleCloseTabs('a');
assert.deepEqual(get(state.projectState).openProjectIds,['a']);
assert.equal(get(state.projectState).selectedProjectId,'a');
assert.deepEqual(loaded,['a','b','a'],'the inactive kept tab loads once; closing tabs never load');
assert.deepEqual(deleted,['temporary']);
assert.deepEqual(get(state.projectState).projects.map(project=>project.meta.id),['a','b']);
app.$destroy();clearMocks();
`));

test('Close all checks drafts before any tab changes, while Close others can keep a dirty active tab', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');await state.handleSelectProject('b');
let focused=0;
app.$inject_state({activeView:{unsavedDraftMessage:()=> 'Save your note.',canAutoNavigate:()=>false,
 focusUnsavedDraft(){focused++;},snapshotSession:()=>null}});
await state.handleCloseTabs(null);
await state.handleCloseTabs('a');
assert.deepEqual(get(state.projectState).openProjectIds,['a','b']);
assert.equal(get(state.projectState).selectedProjectId,'b');assert.equal(focused,2);
await state.handleCloseTabs('b');
assert.deepEqual(get(state.projectState).openProjectIds,['b']);
assert.equal(get(state.projectState).selectedProjectId,'b');
assert.deepEqual(deleted,[]);assert.deepEqual(loaded,['a','b']);
app.$destroy();clearMocks();
`));

test('Close all retires the active tab first, drains temporary cleanup and never loads adjacent cases', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');await state.handleSelectProject('b');
await state.projectController.adoptTemporaryProject(summary('temporary'),false);
holdDeletes=true;
const operation=state.handleCloseTabs(null);
for(let i=0;i<4;i++)await new Promise(resolve=>setImmediate(resolve));
assert.equal(get(state.projectState).selectedProjectId,null);
assert.equal(app.$capture_state().tabCloseBusy,true);
await state.handleSelectProject('a');
assert.deepEqual(loaded,['a','b'],'navigation is held while a bulk close is running');
const closing=state.close.request();
await new Promise(resolve=>setImmediate(resolve));assert.equal(finished,0);
assert.equal(deleteJobs.length,1);deleteJobs[0].resolve();
await operation;await closing;
assert.deepEqual(get(state.projectState).openProjectIds,[]);
assert.equal(get(state.projectState).projectDetail,null);
assert.deepEqual(deleted,['temporary']);assert.deepEqual(loaded,['a','b']);
assert.equal(finished,1,'normal app close waits for the accepted batch');
assert.equal(app.$capture_state().tabCloseBusy,false);
app.$destroy();clearMocks();
`));

test('a failed active temporary-tab cleanup stops Close all and preserves the other tabs', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');await state.handleSelectProject('b');
await state.projectController.adoptTemporaryProject(summary('temporary'));
failDelete=true;
await state.handleCloseTabs(null);
assert.deepEqual(get(state.projectState).openProjectIds,['a','b','temporary']);
assert.equal(get(state.projectState).selectedProjectId,'temporary');
assert.deepEqual(get(state.projectState).temporaryProjectIds,['temporary']);
assert.equal(app.$capture_state().tabCloseBusy,false);
app.$destroy();clearMocks();
`));

test('Close all cancels a pending extraction and never reopens its late result', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');
const operation=extract();await new Promise(resolve=>setImmediate(resolve));
await state.handleCloseTabs(null);
assert.deepEqual(get(state.projectState).openProjectIds,[]);
extractionJobs[0].resolve(summary('cancelled-result'));await operation;
assert.deepEqual(get(state.projectState).openProjectIds,[]);
assert.deepEqual(deleted,['cancelled-result']);
app.$destroy();clearMocks();
`));

test('Close others preserves a kept source extraction completing during temporary-tab cleanup', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');
await state.projectController.adoptTemporaryProject(summary('old-result'),false);
const scan=extract();await new Promise(resolve=>setImmediate(resolve));
holdDeletes=true;
const batch=state.handleCloseTabs('a');
for(let i=0;i<4;i++)await new Promise(resolve=>setImmediate(resolve));
assert.equal(app.$capture_state().tabCloseBusy,true);
extractionJobs[0].resolve(summary('new-result'));await scan;
assert.deepEqual(get(state.projectState).openProjectIds,['a','new-result']);
assert.equal(get(state.projectState).selectedProjectId,'a');
deleteJobs[0].resolve();await batch;
assert.deepEqual(get(state.projectState).temporaryProjectIds,['new-result']);
assert.deepEqual(deleted,['old-result']);assert.deepEqual(loaded,['a']);
app.$destroy();clearMocks();
`));

test('a failed inactive temporary cleanup leaves the failed tab active and saved cases reopenable', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');await state.handleSelectProject('b');
await state.projectController.adoptTemporaryProject(summary('temporary'),false);
failDelete=true;
await state.handleCloseTabs(null);
assert.deepEqual(get(state.projectState).openProjectIds,['temporary']);
assert.equal(get(state.projectState).selectedProjectId,'temporary');
assert.deepEqual(get(state.projectState).projects.filter(item=>item.meta.id!=='temporary').map(item=>item.meta.id),['a','b']);
assert.deepEqual(deleted,['temporary']);
app.$destroy();clearMocks();
`));

test('Close right preserves the active left tab and an inactive context tab without extra loads', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');await state.handleSelectProject('b');
await state.projectController.adoptTemporaryProject(summary('right'),false);
await state.handleSelectProject('a');
const loads=[...loaded];
await state.handleCloseTabs('b',true);
assert.deepEqual(get(state.projectState).openProjectIds,['a','b']);
assert.equal(get(state.projectState).selectedProjectId,'a');
assert.deepEqual(loaded,loads);assert.deepEqual(deleted,['right']);
await state.handleCloseTabs('b',true);
assert.deepEqual(get(state.projectState).openProjectIds,['a','b']);
app.$destroy();clearMocks();
`));

test('Close right selects the context tab once when the active tab is on its right', () => runSvelteTest(appFixture + String.raw`
await state.handleSelectProject('a');await state.handleSelectProject('b');
await state.projectController.adoptTemporaryProject(summary('right'));
await state.handleCloseTabs('b',true);
assert.deepEqual(get(state.projectState).openProjectIds,['a','b']);
assert.equal(get(state.projectState).selectedProjectId,'b');
assert.deepEqual(loaded,['a','b','right','b']);
assert.deepEqual(deleted,['right']);
app.$destroy();clearMocks();
`));
