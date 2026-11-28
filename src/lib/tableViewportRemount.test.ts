import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('remounted table bodies replace observed nodes and clamp retained horizontal positions', () => runSvelteTest(String.raw`
import { get } from 'svelte/store';
import { flushSync } from 'svelte';
import { createWorkspaceController } from './src/lib/workspaceController.ts';
globalThis.document={dispatchEvent(){},activeElement:null,documentElement:{style:{setProperty(){}}}};
globalThis.HTMLElement=class {};globalThis.Element=class {};
const frames=new Map();let frameId=0;
globalThis.requestAnimationFrame=callback=>{frames.set(++frameId,callback);return frameId;};
globalThis.cancelAnimationFrame=id=>frames.delete(id);
const settle=async()=>{for(let step=0;step<6;step++)await tick();};
const flushFrames=()=>{for(const [id,callback] of [...frames]){frames.delete(id);callback();}};
const body=(width,maximum)=>{
 let left=0;
 return {isConnected:true,clientWidth:width,clientHeight:320,scrollTop:0,
  get scrollLeft(){return left;},set scrollLeft(value){left=Math.max(0,Math.min(maximum,value));}};
};
const Table=await compileHeadless('DataTable');
const fixture=async()=>{
 const workspace=createWorkspaceController();globalThis.__workspace=workspace;
 const columns=Array.from({length:120},(_,index)=>'c'+index);
 workspace.projectDetail.set({project:{meta:{id:'wide',total_records:8}},columns,columns_meta:[],
  hidden_columns:[],column_max_chars:{},column_kinds:{},iocs:[]});
 workspace.currentProjectId.set('wide');workspace.totalRows.set(8);workspace.totalFilteredRows.set(8);
 workspace.queryCountKnown.set(true);workspace.scrollLeft.set(2000);
 const old=body(1000,20_000);old.scrollLeft=2000;
 workspace.bodyScrollEl.set(old);workspace.headerScrollEl.set({scrollLeft:2000});
 const table=await mountHeadless(Table);
 const observed=new Set([old]),unobserved=[];
 const observer={observe(node){observed.add(node);},unobserve(node){observed.delete(node);unobserved.push(node);},disconnect(){observed.clear();}};
 // Browser setup is deferred by the shared headless harness. Seed its initial
 // mounted observation, then exercise the real reactive remount transitions.
 table.$inject_state({resizeObserver:observer,observedScrollEl:old});await settle();
 return {workspace,columns,table,old,observed,unobserved};
};
{
 const {workspace,columns,table,old,observed,unobserved}=await fixture();
 old.isConnected=false;workspace.bodyScrollEl.set(null);workspace.headerScrollEl.set(null);
 workspace.totalFilteredRows.set(0);await settle();
 workspace.hiddenColumns.set(new Set(columns.slice(6)));await settle();
 const fresh=body(1254,0),header={scrollLeft:0},footer={scrollLeft:0};
 table.$inject_state({horizontalScrollEl:footer});workspace.headerScrollEl.set(header);
 workspace.bodyScrollEl.set(fresh);workspace.totalFilteredRows.set(8);await settle();
 assert.ok(unobserved.includes(old),'the detached viewport must stop being observed');
 assert.deepEqual([...observed],[fresh],'the replacement viewport must receive resize observations');
 assert.equal(get(workspace.tableWidth),1254,'the replacement viewport owns current geometry');
 assert.equal(get(workspace.scrollLeft),0,'publish browser clamping after restoring the retained position');
 assert.equal(header.scrollLeft,0);assert.equal(footer.scrollLeft,0);
 assert.deepEqual(table.$capture_state().renderedColumns.map(column=>column.id),columns.slice(0,6));
 table.$destroy();workspace.dispose();
}
{
 const {workspace,table,old}=await fixture();
 old.scrollLeft=2300;table.$capture_state().queueScroll(old);
 assert.equal(frames.size,1);
 old.isConnected=false;flushSync(()=>workspace.bodyScrollEl.set(null));
 const fresh=body(1100,20_000),header={scrollLeft:0};workspace.headerScrollEl.set(header);
 flushSync(()=>workspace.bodyScrollEl.set(fresh));await settle();flushFrames();
 assert.equal(fresh.scrollLeft,2000,'a still-valid saved offset returns on the new viewport');
 assert.equal(header.scrollLeft,2000);assert.equal(get(workspace.scrollLeft),2000);
 assert.equal(frames.size,0,'an old viewport cannot publish a delayed scroll frame');
 table.$destroy();workspace.dispose();
}
for(const action of ['wheel','newer-restore','replacement']){
 const {workspace,table,old,observed}=await fixture();
 old.isConnected=false;flushSync(()=>workspace.bodyScrollEl.set(null));
 const fresh=body(1100,20_000);flushSync(()=>workspace.bodyScrollEl.set(fresh));
 let current=fresh;
 if(action==='replacement'){
  current=body(900,20_000);flushSync(()=>workspace.bodyScrollEl.set(current));
 }
 current.scrollLeft=350;
 if(action==='newer-restore')workspace.scrollLeft.set(350);
 else table.$capture_state().queueScroll(current);
 await settle();flushFrames();await settle();
 assert.equal(current.scrollLeft,350,action+': an older asynchronous restore cannot undo new movement');
 assert.equal(get(workspace.scrollLeft),350,action+': only the current body publishes its position');
 assert.deepEqual([...observed],[current]);
 if(action==='replacement')assert.equal(fresh.scrollLeft,0,'a replaced body must not receive the stale restore');
 table.$destroy();workspace.dispose();
}
`));
