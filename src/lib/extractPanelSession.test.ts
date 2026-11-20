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
