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
 hidden_columns:[],column_max_chars:{},column_kinds:{message:'string'},iocs:[],initial_rows:[]});
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
