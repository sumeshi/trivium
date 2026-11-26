import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('the table waits for its first result before creating evidence rows, while known results retain uncached scroll slots', () => runSvelteTest(String.raw`
globalThis.document={dispatchEvent:()=>true,activeElement:null,documentElement:{style:{setProperty(){}}}};
globalThis.HTMLElement=class {};
const {createWorkspaceController}=await import('./src/lib/workspaceController.ts');
const {normalizeRow}=await import('./src/lib/components/project_view/state.ts');
const workspace=createWorkspaceController();
globalThis.__workspace=workspace;
workspace.projectDetail.set({project:{meta:{id:'restored-csv',total_records:5}},columns:['message'],columns_meta:[],
 hidden_columns:[],column_max_chars:{},column_kinds:{message:'string'},iocs:[],});
workspace.currentProjectId.set('restored-csv');
workspace.totalRows.set(5);workspace.totalFilteredRows.set(5);workspace.viewportHeight.set(160);
workspace.queryCountKnown.set(false);workspace.appliedResultLoading.set(true);
const Table=await compileHeadless('DataTable');
const table=await mountHeadless(Table);await tick();
let state=table.$capture_state();
assert.equal(state.totalHeight,160,'restoring scroll geometry does not require mounting placeholder rows');
assert.deepEqual(state.virtualRows,[],'unknown first results are not evidence rows');
assert.deepEqual(state.rowSlots,[],'loading and failed initial queries have no recycled controls');
const memo='Sender: <alice@example.org>\n"quoted"';
workspace.registerLoadedPage(0,0,[normalizeRow({row_index:0,cells:{message:{kind:'string',text:'source',truncated:false}},
 flag:'critical',memo,tags:['restored'],bookmarked:true})],0);
await tick();
assert.deepEqual(table.$capture_state().virtualRows,[],'a received cache stays hidden until the result is ready');
workspace.queryCountKnown.set(true);workspace.appliedResultLoading.set(false);await tick();
state=table.$capture_state();
assert.equal(state.virtualRows.length,5);
assert.equal(state.virtualRows[0].row.memo,memo);assert.equal(state.virtualRows[0].row.flag,'critical');
assert.equal(state.virtualRows[0].row.bookmarked,true);
assert.ok(state.virtualRows.slice(1).every(item=>item.row===null),'known results keep normal uncached-page slots');
workspace.queryCountKnown.set(false);workspace.rowsCache.set(new Map());workspace.positionToRowIndex.set(new Map());
workspace.queryError.set({page:0,message:'Invalid query'});await tick();
state=table.$capture_state();
assert.equal(state.totalHeight,160);assert.deepEqual(state.virtualRows,[]);assert.deepEqual(state.rowSlots,[]);
table.$destroy();workspace.dispose();
`));

test('visible evidence cells follow the restored view and visibility controls independently of metadata saves', () => runSvelteTest(String.raw`
globalThis.window={dispatchEvent(){}};
globalThis.document={dispatchEvent(){},addEventListener(){},removeEventListener(){},activeElement:null,documentElement:{style:{setProperty(){}}}};
globalThis.Element=class {};globalThis.HTMLElement=class {};
const fs=(await import('node:fs/promises')).default;
const read=fs.readFile;
fs.readFile=async(...args)=>{
 let source=await read(...args);
 if(String(args[0]).replaceAll('\\','/').endsWith('/ProjectView.svelte')&&typeof source==='string') source=source.replace("import './project-view.css';",'');
 return source;
};
const {get}=await import('svelte/store');
const {queueHiddenColumnWrite}=await import('./src/lib/hiddenColumnWrites.ts');
const Project=await compileHeadless('../ProjectView');
const Controls=await compileHeadless('FilterControls');
const settle=async()=>{for(let i=0;i<25;i++){await Promise.resolve();await tick();}};
for(const action of ['toggle','show-all','save-failure','restored-view']){
 let storedHidden=['c2'];
 let finishSave,failSave,saves=0;
 const saving=new Promise((resolve,reject)=>{finishSave=resolve;failSave=reject;});
 const requests=[];
 const saved={version:1,search:'',searchColumns:[],condition:{type:'group',id:'root',kind:'and',collapsed:false,children:[]},
  flagFilter:'all',bookmarkedOnly:false,hiddenColumns:action==='restored-view'?[]:['c2'],sortKey:null,sortDirection:'asc',columnWidths:{},logicalRow:0};
 const api={loadViewState:async()=>({status:'ok',state:saved}),saveViewState:async()=>{},cancelRequest:async()=>0,
  setHiddenColumns:({projectId,hiddenColumns})=>queueHiddenColumnWrite(projectId,hiddenColumns,async snapshot=>{
   saves++;await saving;storedHidden=snapshot;
  }),
  queryProjectRows:async args=>{requests.push(args);assert.ok(Array.isArray(args.pageColumns),'the page captures the requested presentation columns');
   return {offset:0,rows:[{row_index:0,
   cells:Object.fromEntries([['c1','visible'],['c2','restored source value']]
    .filter(([id])=>args.pageColumns.includes(id)).map(([id,text])=>[id,{kind:'string',text,truncated:false}])),
   flag:'',memo:'',tags:[],bookmarked:false}],total_rows:1,total_filtered_rows:1,
   annotated_total:0,annotated_filtered:0,annotation_revision:0};}};
 const detail={project:{meta:{id:'hidden-'+action,name:'source.csv',source_file_name:'source.csv',total_records:1,flagged_records:0}},
  columns:['c1','c2'],columns_meta:[],column_kinds:{c1:'string',c2:'string'},column_max_chars:{},
  hidden_columns:['c2'],iocs:[],color_rules:[]};
 const project=await mountHeadless(Project,{projectDetail:detail,backend:api});
 await settle();
 const workspace=project.$capture_state().workspace;
 assert.equal(get(workspace.queryCountKnown),true);
 if(action==='restored-view'){
  assert.deepEqual(requests[0].pageColumns,['c1','c2']);
  assert.equal(get(workspace.rowsCache).get(0).displayCache.c2,'restored source value');
  assert.equal(saves,0,'restoring presentation does not need a metadata write');
  project.$destroy();continue;
 }
 assert.deepEqual(requests[0].pageColumns,['c1']);
 assert.equal(get(workspace.rowsCache).get(0).cells.c2,undefined,'the opened page was fetched while c2 was hidden');
 workspace.toggleRowSelection(0);
 globalThis.__workspace=workspace;
 const controls=await mountHeadless(Controls);
 if(action!=='show-all') controls.$capture_state().toggleColumn('c2');
 else controls.$capture_state().setAllColumns(true);
 await settle();
 assert.deepEqual(storedHidden,['c2'],'the metadata write is still pending');
 assert.deepEqual(get(workspace.visibleColumns),['c1','c2']);
 assert.equal(get(workspace.rowsCache).get(0)?.displayCache.c2,'restored source value',
  action+': a visible source cell must replace the loading placeholder without another filter or tab change');
 assert.deepEqual([...get(workspace.selectedRowIds)],[0],'presentation changes keep the evidence selection');
 assert.deepEqual(requests.at(-1).pageColumns,['c1','c2']);
 if(action==='save-failure')failSave(Error('Metadata write failed'));else finishSave();
 await settle();
 assert.deepEqual(storedHidden,action==='save-failure'?['c2']:[]);
 assert.equal(get(workspace.rowsCache).get(0)?.displayCache.c2,'restored source value','save settlement cannot replace the requested page projection');
 controls.$destroy();project.$destroy();
}
`,20_000));
