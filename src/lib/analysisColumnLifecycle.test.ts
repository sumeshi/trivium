import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('clearing a datetime column retires results, pending responses and queued timeline scans', () => runSvelteTest(String.raw`
const flush=async()=>{for(let i=0;i<5;i++){await Promise.resolve();await tick();}};
const delay=()=>new Promise(done=>setTimeout(done,150));
for(const name of ['TimePanel','TimelinePanel']){
 const pending=[];let cancelled=0;
 const read=()=>new Promise((resolve,reject)=>pending.push({resolve,reject}));
 const api={timeAnalysis:read,histogram:read};
 globalThis.__annotations=writable({});
 globalThis.__workspace={backend:writable(api),
  projectDetail:writable({project:{meta:{id:'case'}},columns:['timestamp']}),
  columnLabels:writable(new Map()),
  appliedFilters:writable({search:'',searchColumns:[],flag:'all',bookmarkedOnly:false,condition:null}),
  filterByRange(){throw Error('Retiring analysis must not change filters.');},
  createReadScope:()=>({run:(_key,_api,work)=>work('request'),cancel(){cancelled++;},cancelAll(){cancelled++;},dispose(){}})};
 const Panel=await compileHeadless(name);
 const panel=await mountHeadless(Panel,{open:true,column:'timestamp'});
 const state=()=>panel.$capture_state();
 const field=name==='TimePanel'?'analysis':'histogram';
 const result=name==='TimePanel'
  ? {column:'timestamp',gaps:[],sessions:[],bursts:[],missing:0,invalid:0}
  : {column:'timestamp',buckets:[{start:'2026-11-18T00:00:00Z',end:'2026-11-19T00:00:00Z',count:1}],full_range:null,outside:0,missing:0,invalid:0};
 if(name==='TimelinePanel'){
  panel.$set({column:''});await flush();await delay();
  assert.equal(pending.length,0,'A cleared selection must not start the previously queued scan.');
  panel.$set({column:'timestamp'});await flush();await delay();
 }
 assert.equal(pending.length,1);
 panel.$set({column:''});await flush();
 const cancelledAtClear=cancelled;
 pending[0].resolve(result);await flush();
 assert.equal(state()[field],null,'An old response cannot repopulate an unselected column.');
 assert.equal(state().loading,false);
 assert.equal(state().error,'');
 assert.ok(cancelledAtClear>0);
 // Reselecting the same column is a new request, not an already-loaded key.
 panel.$set({column:'timestamp'});await flush();if(name==='TimelinePanel')await delay();
 assert.equal(pending.length,2);
 pending[1].resolve(result);await flush();
 assert.deepEqual(state()[field],result);
 panel.$set({column:''});await flush();
 assert.equal(state()[field],null,'Already displayed results must also clear.');
 panel.$set({column:'timestamp'});await flush();if(name==='TimelinePanel')await delay();
 assert.equal(pending.length,3);
 panel.$set({column:''});await flush();
 pending[2].reject(Error('Old scan failed'));await flush();
 assert.equal(state()[field],null);
 assert.equal(state().error,'','An old failure must not appear under the unselected column.');
 assert.equal(state().loading,false);
 panel.$destroy();
}
`, 15_000));

test('column stats reloads a numeric histogram after visiting a text column', () => runSvelteTest(String.raw`
const flush=async()=>{for(let i=0;i<5;i++){await Promise.resolve();await tick();}};
for(const settled of [true,false]){
 const pending=[];let cancelled=0;
 const api={
  columnStats:async({column})=>({column,kind:column==='amount'?'int':'string',top:[],rare:[]}),
  histogram:({column})=>new Promise(resolve=>pending.push({column,resolve})),
 };
 globalThis.__annotations=writable({});
 globalThis.__workspace={backend:writable(api),
  projectDetail:writable({project:{meta:{id:'case'}},columns:['amount','message'],column_kinds:{amount:'int',message:'string'}}),
  columnLabels:writable(new Map()),
  appliedFilters:writable({search:'',searchColumns:[],flag:'all',bookmarkedOnly:false,condition:null}),
  createReadScope:()=>({run:(_key,_api,work)=>work('request'),cancel(){cancelled++;},cancelAll(){cancelled++;},dispose(){}})};
 const Panel=await compileHeadless('StatsPanel');
 const panel=await mountHeadless(Panel,{open:true,column:'amount'});
 const state=()=>panel.$capture_state();
 const result=count=>({column:'amount',buckets:[{start:'1',end:'2',count}],missing:0,invalid:0,outside:0});
 await flush();
 assert.equal(pending.length,1);
 if(settled){pending[0].resolve(result(1));await flush();assert.deepEqual(state().histogram,result(1));}
 const cancelledBefore=cancelled;
 panel.$set({column:'message'});await flush();
 assert.equal(state().histogram,null,'A text column retires the numeric histogram.');
 assert.equal(state().histogramLoading,false,'Retired numeric work must not leave a loading state.');
 assert.ok(cancelled>cancelledBefore);
 panel.$set({column:'amount'});await flush();
 assert.equal(pending.length,2,'Returning to the numeric column must request a fresh histogram.');
 assert.equal(pending[1].column,'amount');
 if(!settled){
  pending[0].resolve(result(99));await flush();
  assert.equal(state().histogram,null,'A retired response must not populate the reselected column.');
  assert.equal(state().histogramLoading,true,'The replacement histogram remains pending.');
 }
 pending[1].resolve(result(2));await flush();
 assert.deepEqual(state().histogram,result(2));
 assert.equal(state().histogramLoading,false);
 panel.$destroy();
}
`));
