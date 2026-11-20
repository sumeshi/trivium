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
const loaded=[],extractionJobs=[];
let failDelete=false,finished=0;
mockIPC((command,args)=>{
  if(command==='list_projects') return [summary('a'),summary('b')];
  if(command==='load_project') {
    const id=args.request.projectId;
    loaded.push(id);
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
