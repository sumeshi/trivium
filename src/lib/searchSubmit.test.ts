import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('Enter flushes one pending search with the latest scope and rules without restarting unchanged results', () => runSvelteTest(String.raw`
globalThis.window={dispatchEvent(){}};
globalThis.document={dispatchEvent(){},activeElement:null,documentElement:{style:{setProperty(){}}}};
globalThis.Element=class {};globalThis.HTMLElement=class {};
const fs=(await import('node:fs/promises')).default;
const read=fs.readFile;
fs.readFile=async(...args)=>{
 let source=await read(...args);
 if(String(args[0]).replaceAll('\\','/').endsWith('/ProjectView.svelte')&&typeof source==='string') source=source.replace("import './project-view.css';",'');
 return source;
};
const {get}=await import('svelte/store');
const Project=await compileHeadless('../ProjectView');
const Search=await compileHeadless('SearchControls');
const timers=new Map();let nextTimer=0;
globalThis.setTimeout=(callback,delay)=>{const id=++nextTimer;timers.set(id,{callback,delay});return id;};
globalThis.clearTimeout=id=>timers.delete(id);
const pendingSearch=()=>[...timers.values()].filter(timer=>timer.delay===160);
const expireSearch=()=>{for(const [id,timer] of [...timers])if(timer.delay===160){timers.delete(id);timer.callback();}};
const requests=[],saves=[];
const backend={loadViewState:async()=>({status:'missing'}),saveViewState:async args=>{saves.push(args);},cancelRequest:async()=>0,
 queryProjectRows:async args=>{requests.push(args);return {offset:args.offset,rows:[],total_rows:100,total_filtered_rows:100,
  annotated_total:0,annotated_filtered:0,annotation_revision:0};}};
const detail={project:{meta:{id:'search-enter',name:'source.csv',source_file_name:'source.csv',total_records:100,flagged_records:0}},
 columns:['c1','c2'],columns_meta:[],column_kinds:{c1:'string',c2:'string'},column_max_chars:{},hidden_columns:[],iocs:[],color_rules:[]};
const project=await mountHeadless(Project,{projectDetail:detail,backend});
const settle=async()=>{for(let i=0;i<25;i++){await Promise.resolve();await tick();}};
await settle();
assert.equal(project.$capture_state().initialized,true);
const workspace=project.$capture_state().workspace;
globalThis.__workspace=workspace;
const search=await mountHeadless(Search);
search.$on('submit',project.$capture_state().submitSearch);
let prevented=0;
const enter=()=>search.$capture_state().handleSearchKeydown({key:'Enter',isComposing:false,keyCode:13,timeStamp:100,preventDefault(){prevented++;}});
const initial=requests.length;
workspace.search.set('pow');await settle();
assert.equal(pendingSearch().length,1);assert.equal(requests.length,initial,'typing retains its debounce');
workspace.search.set('powershell');workspace.searchColumns.set(['c2']);
workspace.flagFilter.set('critical');workspace.bookmarkedOnly.set(true);
workspace.addFilterCondition('c1','eq','confirmed');
workspace.sortKey.set('c2');workspace.sortDirection.set('desc');
enter();enter();await settle();
assert.equal(prevented,2);assert.equal(requests.length,initial+1,'two Enters consume one pending update');
assert.equal(pendingSearch().length,0,'the old debounce cannot submit again');
const submitted=requests.at(-1);
assert.equal(submitted.search,'powershell');assert.deepEqual(submitted.searchColumns,['c2']);
assert.equal(submitted.flagFilter,'critical');assert.equal(submitted.bookmarkedOnly,true);
assert.deepEqual(submitted.condition,get(workspace.conditionPayload));
assert.equal(submitted.sortKey,'c2');assert.equal(submitted.sortDirection,'desc');
assert.equal(workspace.snapshotHistory().entries.length,2,'the rule edit records one history entry');
expireSearch();await settle();assert.equal(requests.length,initial+1);

workspace.scrollTop.set(900);workspace.selectedRowIds.set(new Set([7]));
const history=workspace.snapshotHistory();
enter();await settle();
assert.equal(requests.length,initial+1,'unchanged Enter never forces another query');
assert.equal(get(workspace.scrollTop),900);assert.deepEqual([...get(workspace.selectedRowIds)],[7]);
assert.deepEqual(workspace.snapshotHistory(),history,'unchanged Enter keeps history and selection intact');
workspace.search.set('next');await settle();
assert.equal(requests.length,initial+1);assert.equal(pendingSearch().length,1);
expireSearch();await settle();
assert.equal(requests.length,initial+2);assert.equal(requests.at(-1).search,'next','automatic search still fires without Enter');

workspace.flagFilter.set('safe');await settle();
assert.equal(project.$capture_state().pendingFilters.resetScroll,true);
enter();project.$set({closing:true});await settle();
expireSearch();await settle();
assert.equal(requests.length,initial+2,'a close request blocks the pending Enter');
assert.equal(project.$capture_state().pendingFilters.resetScroll,true,'closing retains the pending view reset');
project.pauseForClose();await project.flushForClose();
assert.equal(saves.at(-1).state.logicalRow,0,'close saves the pending reset instead of the previous scrolled position');
assert.equal(saves.at(-1).state.flagFilter,'safe');
search.$destroy();project.$destroy();
`,20_000));

test('search Enter leaves IME confirmation alone on Chromium and WebKit', () => runSvelteTest(String.raw`
const {createWorkspaceController}=await import('./src/lib/workspaceController.ts');
const workspace=createWorkspaceController();globalThis.__workspace=workspace;
const Search=await compileHeadless('SearchControls');
const search=await mountHeadless(Search);
let submitted=0,prevented=0;
search.$on('submit',()=>submitted++);
const state=search.$capture_state();
const key=patch=>state.handleSearchKeydown({key:'Enter',isComposing:false,keyCode:13,timeStamp:100,preventDefault(){prevented++;},...patch});
key({isComposing:true});key({keyCode:229});
state.searchComposition.start();key({});
state.searchComposition.end({timeStamp:100});key({timeStamp:101});
assert.equal(submitted,0);assert.equal(prevented,0,'the confirming Enter stays available to the IME');
key({timeStamp:151});
assert.equal(submitted,1);assert.equal(prevented,1,'a subsequent intentional Enter submits immediately');
key({key:'Escape',timeStamp:200});assert.equal(submitted,1);
search.$destroy();workspace.dispose();
`));
