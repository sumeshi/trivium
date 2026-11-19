import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('Show all clears the timeline range and survives delayed applied filters before selecting again', () => runSvelteTest(String.raw`
const {get}=await import('svelte/store');
const {createWorkspaceController}=await import('./src/lib/workspaceController.ts');
const {presetCondition}=await import('./src/lib/stores/condition.ts');
const {parseTimelineNanos}=await import('./src/lib/timelineRange.ts');
const flush=async()=>{for(let i=0;i<6;i++){await Promise.resolve();await tick();}};
const settle=async()=>{await new Promise(done=>setTimeout(done,160));await flush();};
const workspace=createWorkspaceController();
const oldStart='1970-01-01T00:00:00.000000001Z';
const oldEnd='1970-01-01T00:00:00.000000002Z';
const start='2026-11-19T00:00:00.000000001Z';
const end='2026-11-19T00:00:00.000000009Z';
const bucket=(min,max,count)=>({start:min,end:max,count,end_inclusive:true,
 flags:{safe:0,suspicious:count,critical:0,unflagged:0}});
const manual={kind:'or',children:[
 {column:'Host',op:'eq',values:['WS01'],negated:false},
 {column:'EventID',op:'eq',values:['4624'],negated:false}]};
const scope={search:'review',searchColumns:['Host'],flag:'suspicious',bookmarkedOnly:true,condition:manual};
const requests=[];
let emptyNext=false,holdNext=false,release;
const hasRange=node=>!!node&&('column'in node?node.column==='Timestamp'&&node.op==='ge':node.children.some(hasRange));
const resultFor=args=>({column:args.column,kind:'datetime',missing:0,invalid:0,
 buckets:hasRange(args.condition)?[bucket(start,end,2)]:args.focus?[bucket(start,end,2)]:[bucket(oldStart,oldEnd,1),bucket(start,end,2)],
 full_range:hasRange(args.condition)?{min:start,max:end}:{min:oldStart,max:end},
 outside:!hasRange(args.condition)&&args.focus?1:0,auto_focused:!hasRange(args.condition)&&args.focus});
workspace.backend.set({histogram:args=>{
 requests.push(structuredClone(args));
 if(holdNext){holdNext=false;return new Promise(resolve=>{release=()=>resolve(resultFor(args));});}
 if(emptyNext){emptyNext=false;return Promise.resolve({...resultFor(args),buckets:[],full_range:null,outside:0,auto_focused:false});}
 return Promise.resolve(resultFor(args));
}});
workspace.projectDetail.set({project:{meta:{id:'case-a'}},columns:['Timestamp','OtherTime','Host','EventID'],columns_meta:[],
 column_kinds:{Timestamp:'datetime',OtherTime:'datetime'},column_max_chars:{},hidden_columns:[],iocs:[],initial_rows:[]});
workspace.currentProjectId.set('case-a');
workspace.conditionTree.set(presetCondition(manual));
workspace.appliedFilters.set(structuredClone(scope));
globalThis.__workspace=workspace;
globalThis.__annotations=writable({});
const Panel=await compileHeadless('TimelinePanel');
const panel=await mountHeadless(Panel,{open:true,column:'Timestamp'});
const state=()=>panel.$capture_state();
const publish=()=>workspace.appliedFilters.update(current=>({...current,condition:structuredClone(get(workspace.conditionPayload))}));
try{
 await settle();
 assert.equal(requests.length,1);
 assert.equal(state().histogram.outside,1,'initial focus may omit an old outlier');
 state().selectBuckets(0,0);await flush();
 assert.ok(hasRange(get(workspace.conditionPayload)));
 publish();await settle();
 assert.deepEqual(state().currentRange,state().fullRange,'the graph can already cover the entire filtered range');
 assert.equal(state().histogram.outside,0);
 state().showAll();await flush();
 assert.deepEqual(get(workspace.conditionPayload),manual,'Show all preserves the complete manual OR expression');
 assert.ok(hasRange(get(workspace.appliedFilters).condition),'the table has not published its next applied query yet');
 await new Promise(done=>setTimeout(done,20));
 publish();await settle();
 const full=requests.at(-1);
 assert.equal(full.focus,false,'the asynchronous filter reset must not re-enable dense-period focus');
 assert.equal(full.range,null);
 assert.deepEqual(full.condition,manual);
 assert.equal(full.search,scope.search);assert.deepEqual(full.searchColumns,scope.searchColumns);
 assert.equal(full.flagFilter,scope.flag);assert.equal(full.bookmarkedOnly,scope.bookmarkedOnly);
 assert.equal(state().histogram.outside,0);
 assert.deepEqual(state().histogram.full_range,{min:oldStart,max:end});
 assert.equal(state().histogram.buckets[0].start,oldStart);
 assert.equal(state().autoFocus,false);
 // A second selection creates a new guarded range even after Show all.
 state().selectBuckets(1,1);await flush();
 assert.ok(hasRange(get(workspace.conditionPayload)));
 const timeGroup=get(workspace.conditionPayload).children.at(-1);
 const limits=timeGroup.children.slice(1).map(part=>parseTimelineNanos(part.values[0]));
 assert.deepEqual(limits,[parseTimelineNanos(start),parseTimelineNanos(end)],'nanosecond bounds remain exact');
 assert.equal(limits[1]-limits[0],8n);
 emptyNext=true;publish();await settle();
 assert.equal(state().histogram.buckets.length,0,'a zero-event result can still be reset');
 state().showAll();publish();await settle();
 assert.deepEqual(get(workspace.conditionPayload),manual);
 assert.equal(requests.at(-1).focus,false);
 assert.equal(state().histogram.buckets[0].start,oldStart);
 // Changing columns retires a pending older request and resets its view intent.
 holdNext=true;state().zoom(.8);await settle();
 assert.equal(typeof release,'function');
 state().showAll();await flush();
 panel.$set({column:'OtherTime'});await flush();await settle();
 assert.equal(state().histogram,null);
 release();await settle();
 assert.equal(requests.at(-1).column,'OtherTime');
 assert.equal(requests.at(-1).focus,true,'full-period preference belongs to its selected datetime column');
 assert.equal(requests.at(-1).range,null);
 assert.equal(state().histogram.column,'OtherTime');
 state().showAll();await settle();
 workspace.projectDetail.update(detail=>({...detail,project:{meta:{id:'case-b'}}}));await settle();
 assert.equal(requests.at(-1).projectId,'case-b');
 assert.equal(requests.at(-1).focus,true,'another investigation starts with its own initial view');
 assert.equal(state().error,'');
}finally{panel.$destroy();workspace.dispose();}
`,15_000));
