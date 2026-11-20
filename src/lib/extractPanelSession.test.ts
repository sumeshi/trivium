import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('Extract drawer and inputs restore when returning to its project without leaking to another tab', () => runSvelteTest(String.raw`
const {createWorkspaceController}=await import('./src/lib/workspaceController.ts');
document.addEventListener=()=>{};
document.removeEventListener=()=>{};
const workspace=createWorkspaceController();
const detail=id=>({project:{meta:{id,name:id,total_records:2,hidden_columns:[]}},columns:['c1','c2'],
 columns_meta:[],column_kinds:{},column_max_chars:{},color_rules:[],iocs:[],hidden_columns:[],initial_rows:[]});
workspace.projectDetail.set(detail('source'));
globalThis.__workspace=workspace;
const Controls=await compileHeadless('FilterControls');
const saved={open:true,selectedColumns:['c2'],scope:'all'};
const source=await mountHeadless(Controls,{extractSession:saved});
assert.equal(source.$capture_state().activeTool,'extract');
assert.deepEqual(source.snapshotExtraction(),saved);
saved.selectedColumns.push('c1');
assert.deepEqual(source.snapshotExtraction().selectedColumns,['c2'],'restored controls own their selection');
const snapshot=source.snapshotExtraction();
snapshot.selectedColumns.push('c1');
assert.deepEqual(source.snapshotExtraction().selectedColumns,['c2'],'tab snapshots must be detached');
const retained=source.snapshotExtraction();
source.$destroy();

workspace.projectDetail.set(detail('result'));
const result=await mountHeadless(Controls);
assert.equal(result.$capture_state().activeTool,null);
assert.deepEqual(result.snapshotExtraction(),{open:false,selectedColumns:null,scope:'filtered'});
result.$destroy();

workspace.projectDetail.set(detail('source'));
const restored=await mountHeadless(Controls,{extractSession:retained});
assert.equal(restored.$capture_state().activeTool,'extract');
assert.deepEqual(restored.snapshotExtraction(),retained);
restored.$capture_state().closeTool(false);
assert.equal(restored.snapshotExtraction().open,false,'an explicitly closed drawer must stay closed');
restored.$destroy();
workspace.dispose();
`));

test('group controls and verified drilldown restore per tab while preserving Extract state', () => runSvelteTest(String.raw`
const {get}=await import('svelte/store');
const {createWorkspaceController}=await import('./src/lib/workspaceController.ts');
document.addEventListener=()=>{};document.removeEventListener=()=>{};
const workspace=createWorkspaceController();
const detail=id=>({project:{meta:{id,name:id,total_records:2,hidden_columns:[]}},columns:['host','user'],
 columns_meta:[],column_kinds:{host:'string',user:'string'},column_max_chars:{},color_rules:[],iocs:[],hidden_columns:[],initial_rows:[]});
workspace.projectDetail.set(detail('source'));
const path=['alpha','alice'].map(label=>({label,value:label,count:1,children:[]}));
workspace.filterByGroupPath(['host','user'],path);
globalThis.__workspace=workspace;
const Controls=await compileHeadless('FilterControls');
const groupSession={open:true,columns:['host','user'],granularities:{},sortBy:'count',sortDirection:'desc',filter:workspace.snapshotGroupFilter()};
const extractSession={open:false,selectedColumns:['user'],scope:'all'};
const source=await mountHeadless(Controls,{groupSession,extractSession});
assert.equal(source.$capture_state().activeTool,'groups');
assert.deepEqual(source.snapshotGrouping(),groupSession);
assert.deepEqual(source.snapshotExtraction(),extractSession);
const retained=source.snapshotGrouping();
retained.columns.push('changed');retained.filter.selection.path[0].label='changed';
assert.deepEqual(source.snapshotGrouping().columns,['host','user']);
assert.equal(get(workspace.groupSelection).path[0].label,'alpha');
const controls=source.snapshotGrouping(),tree=structuredClone(get(workspace.conditionTree));
source.$destroy();workspace.dispose();

const restoredWorkspace=createWorkspaceController();
restoredWorkspace.projectDetail.set(detail('source'));
restoredWorkspace.conditionTree.set(tree);
restoredWorkspace.restoreGroupFilter(controls.filter);
globalThis.__workspace=restoredWorkspace;
const restored=await mountHeadless(Controls,{groupSession:controls,extractSession});
assert.equal(restored.$capture_state().activeTool,'groups');
assert.deepEqual(restored.snapshotGrouping(),controls);
assert.deepEqual(restored.snapshotExtraction(),extractSession);
restoredWorkspace.filterByGroupPath(['host'],[path[0]]);
assert.equal(get(restoredWorkspace.groupSelection).path.length,1);
restored.$destroy();
restoredWorkspace.dispose();

const otherWorkspace=createWorkspaceController();
otherWorkspace.projectDetail.set(detail('another'));
globalThis.__workspace=otherWorkspace;
const other=await mountHeadless(Controls);
assert.equal(other.$capture_state().activeTool,null);
assert.deepEqual(other.snapshotGrouping().columns,[]);
assert.equal(get(otherWorkspace.groupSelection),null);
other.$destroy();otherWorkspace.dispose();
`));

test('Rules and Groups navigation preserves empty or selected state and restores destination focus', () => runSvelteTest(String.raw`
const {get}=await import('svelte/store');
const {createWorkspaceController}=await import('./src/lib/workspaceController.ts');
document.addEventListener=()=>{};document.removeEventListener=()=>{};
const focused=[];
const triggers=Object.fromEntries(['filters','analysis'].map(group=>['tool-group-'+group,{isConnected:true,focus:()=>focused.push(group)}]));
document.getElementById=id=>triggers[id]??null;
const Controls=await compileHeadless('FilterControls');
for(const selected of [false,true]){
 const workspace=createWorkspaceController();
 workspace.projectDetail.set({project:{meta:{id:'case',name:'case',total_records:2,hidden_columns:[]}},columns:['host','time','user'],
  columns_meta:[],column_kinds:{host:'string',time:'datetime',user:'string'},column_max_chars:{},color_rules:[],iocs:[],hidden_columns:[],initial_rows:[]});
 workspace.addFilterCondition('user','eq','manual');
 if(selected)workspace.filterByGroupPath(['host','time'],[
  {label:'alpha',value:'alpha',count:1,children:[]},
  {label:'2026-11-20',value:'2026-11-20',count:1,children:[],range:{start:'2026-11-20T00:00:00.000000001Z',end:'2026-11-21T00:00:00.000000001Z'}},
 ]);
 const groupSession={open:false,columns:selected?['host','time']:[],granularities:selected?{time:'day'}:{},
  sortBy:selected?'name':'count',sortDirection:selected?'asc':'desc',filter:workspace.snapshotGroupFilter()};
 const extractSession={open:false,selectedColumns:['user'],scope:'all'};
 const originalQuery=structuredClone(get(workspace.conditionPayload));
 globalThis.__workspace=workspace;
 const controls=await mountHeadless(Controls,{groupSession,extractSession});
 controls.$inject_state({drawerEl:{querySelector:selector=>{
  assert.equal(selector,'[aria-label="Close analysis panel"]');return {focus:()=>focused.push('panel')};
 }}});
 try{
  for(const [tool,category] of [['groups','analysis'],['conditions','filters'],['groups','analysis']]){
   const start=focused.length;
   await controls.$capture_state().openRelatedTool(tool);await tick();
   const state=controls.$capture_state();
   assert.equal(state.activeTool,tool);assert.equal(state.activeGroup,category);
   assert.equal(state.rememberedTools[category],tool);
   assert.equal(state.drawerTrigger,triggers['tool-group-'+category]);
   assert.deepEqual(controls.snapshotGrouping(),{...groupSession,open:tool==='groups'});
   assert.deepEqual(controls.snapshotExtraction(),extractSession);
   assert.deepEqual(get(workspace.conditionPayload),originalQuery,'navigation must not change Rules or the exact selected range');
   assert.equal(focused[start],'panel','navigation moves focus into the destination drawer');
   state.closeTool();await tick();
   assert.equal(focused.at(-1),category,'Close returns focus to the destination toolbar trigger');
  }
 }finally{controls.$destroy();workspace.dispose();}
}
`));
