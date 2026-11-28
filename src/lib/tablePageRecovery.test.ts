import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('the actual table waits for the first result and reloads unblocked visible pages once', () => runSvelteTest(String.raw`
  // Run the real reactive component/controller/query with no-op DOM rendering.
import { get } from 'svelte/store';
import { createWorkspaceController } from './src/lib/workspaceController.ts';
import { createWorkspaceQuery } from './src/lib/workspaceQuery.ts';
import { maxScrollTop } from './src/lib/virtualScroll.ts';
import { normalizeRow } from './src/lib/components/project_view/state.ts';
globalThis.document={dispatchEvent:()=>true,activeElement:null,documentElement:{style:{setProperty(){}}}};
globalThis.HTMLElement=class {};globalThis.Element=class {};
const Table=await compileHeadless('DataTable');
const settle=async()=>{for(let step=0;step<24;step++){await Promise.resolve();await tick();}};
{
 const offsets=[];let release;const held=new Promise(resolve=>{release=resolve;});
 const row=id=>({row_index:id,cells:{c1:{kind:'string',text:String(id)}},flag:'',memo:'',tags:[],bookmarked:false});
 const records=offset=>Array.from({length:Math.min(250,276-offset)},(_,i)=>row(offset+i));
 const filters=search=>({search,searchColumns:[],flag:'all',bookmarkedOnly:false,condition:null});
 const api={cancelRequest:async()=>0,queryProjectRows:async args=>{
  offsets.push(args.offset);if(args.offset===0)await held;
  return {offset:args.offset,rows:records(args.offset),total_rows:276,total_filtered_rows:276,annotated_total:0,annotated_filtered:0,annotation_revision:0};
 }};
 const workspace=createWorkspaceController();globalThis.__workspace=workspace;
 workspace.projectDetail.set({project:{meta:{id:'bootstrap',total_records:276}},columns:['c1'],columns_meta:[],column_max_chars:{c1:4},column_kinds:{c1:'string'},iocs:[],hidden_columns:[],});
 workspace.currentProjectId.set('bootstrap');workspace.backend.set(api);workspace.viewportHeight.set(800);
 const queries=createWorkspaceQuery(workspace,{backend:api,isReady:()=>true,restoreViewport:async()=>{},scopeChanged(){},summary(){},notify(){}});
 queries.initialize(filters(''));workspace.totalFilteredRows.set(276);workspace.queryCountKnown.set(true);
 workspace.registerLoadedPage(0,0,records(0).map(normalizeRow));workspace.registerLoadedPage(1,250,records(250).map(normalizeRow));
 const table=await mountHeadless(Table,{ensureRangeLoaded:queries.ensureRangeLoaded});
 const applying=queries.applyFilters(filters('keep'),true,true);await settle();
 assert.deepEqual(offsets,[0],'only the explicit first request may run while the new result count is unknown');
 assert.equal(get(workspace.queryCountKnown),false);assert.equal(table.$capture_state().rowSlots.length,0,'a held first response must not publish placeholder rows');
 release();await applying;await settle();
 assert.equal(get(workspace.queryCountKnown),true);assert.deepEqual(offsets,[0,250],'normal visible-range prefetch resumes after the first result');
 table.$destroy();workspace.dispose();
}
for(const succeeds of [false,true]){
 const offsets=[];let finish;const pending=new Promise((resolve,reject)=>{finish=()=>succeeds?resolve({entry:note,revision:1,flaggedRecords:0}):reject(Error('disk full'));});
 const row=id=>({row_index:id,cells:{c1:{kind:'string',text:String(id)}},flag:'',memo:'',tags:[],bookmarked:false});
 const cells=Object.fromEntries(Array.from({length:1259},(_,i)=>['c'+i,{kind:'string',text:'x'.repeat(256),truncated:false}]));
 const records=Array.from({length:26},(_,i)=>({...row(250+i),cells}));
 const note={flag:'',memo:'n'.repeat(10*1024),tags:[],bookmarked:false};
 const api={updateFlag:()=>pending,cancelRequest:async()=>0,queryProjectRows:async args=>{
  offsets.push(args.offset);return {offset:args.offset,rows:args.offset===250?records.map((r,i)=>i===0&&succeeds?{...r,...note}:r):[row(0)],
   total_rows:276,total_filtered_rows:276,annotated_total:0,annotated_filtered:0,annotation_revision:succeeds?1:0};
 }};
 const workspace=createWorkspaceController();globalThis.__workspace=workspace;
 workspace.projectDetail.set({project:{meta:{id:'case',total_records:276}},columns:['c1'],columns_meta:[],column_max_chars:{c1:4},column_kinds:{c1:'string'},iocs:[],hidden_columns:[],});
 workspace.currentProjectId.set('case');workspace.backend.set(api);
 const queries=createWorkspaceQuery(workspace,{backend:api,isReady:()=>true,restoreViewport:async()=>{},scopeChanged(){},summary(){},notify(){}});
 queries.initialize({search:'',searchColumns:[],flag:'all',bookmarkedOnly:false,condition:null});
 workspace.queryCountKnown.set(true);workspace.totalFilteredRows.set(276);
 const height=get(workspace.rowHeight);workspace.viewportHeight.set(25*height);workspace.scrollTop.set(maxScrollTop(276,25*height,height));
 const cached=records.map(normalizeRow);workspace.registerLoadedPage(0,0,[normalizeRow(row(0))]);workspace.registerLoadedPage(1,250,cached);
 const table=await mountHeadless(Table,{ensureRangeLoaded:queries.ensureRangeLoaded});
 const saving=workspace.saveAnnotation('case',cached[0],{memo:note.memo}).catch(()=>false);
 workspace.viewportHeight.set(28*height);await settle();
 assert.equal(workspace.currentVisiblePage(),0);assert.deepEqual(offsets,[]);
 finish();await saving;await settle();
 assert.deepEqual(offsets.slice().sort((a,b)=>a-b),[0,250],'a settled edit must reload both currently visible pages');
 assert.equal(get(workspace.largePages).has(1),succeeds);
 assert.equal(get(workspace.rowsCache).has(250),!succeeds);
 const slots=table.$capture_state().virtualRows.filter(item=>item.position>=250);
 assert.ok(slots.length>0,'the adjacent page remains inside the rendered range');
 assert.ok(succeeds||slots.every(item=>item.row),'a recovered page must not remain blank/loading');
 await settle();assert.equal(offsets.length,2,'a genuinely oversized stored page remains blocked without repeated reads');
 table.$destroy();workspace.dispose();
}
`, 15_000));
