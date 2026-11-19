import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('row detail IOC actions use loaded raw fields, close first, and reject stale owners', () => runSvelteTest(String.raw`
globalThis.document={dispatchEvent:()=>true,activeElement:null};globalThis.HTMLElement=class {};
const {get}=await import('svelte/store');
globalThis.__annotations=writable({});
const projectDetail=writable({project:{meta:{id:'case'}},columns:['c1','c2']});
const expandedCell=writable(null);let reads=0;
const full=(text,kind='string')=>({text,kind,truncated:false,length:text===null?null:text.length});
const row=(value)=>({row_index:91,cells:{c1:full(value),c2:full('')},tags:[],flag:'',bookmarked:false});
const target=(value)=>({projectId:'case',rowIndex:91,columnId:'c1',preview:'display is not a source',row:row(value)});
globalThis.__workspace={backend:writable({
 getCell:()=>{reads++;throw Error('complete detail values must not be fetched');},
 locateProjectRow:async()=>({position:0,total_filtered_rows:1,total_rows:1})}),
 projectDetail,expandedCell,columnLabels:writable(new Map([['c1','duplicate'],['c2','duplicate']])),hiddenColumns:writable(new Set()),
 addFilterCondition(){throw Error('IOC creation must not filter or annotate');},
 createReadScope:()=>({run:(_key,api,request)=>request('detail-read'),cancelAll(){},dispose(){}}),
 appliedFilters:writable({search:'',searchColumns:[],flag:'all',bookmarkedOnly:false,condition:null}),
 sortKey:writable(null),sortDirection:writable('asc'),viewGeneration:writable(0),totalFilteredRows:writable(1)};
const Dialog=await compileHeadless('ExpandedCellDialog');
for(const [kind,value] of [['string',''],['string','  -x:y|z "quoted" C:\\Temp\\  '],['array','["quoted","array"]'],['object','{"object":"value"}']]){
 const source=target(value);source.row.cells.c1.kind=kind;expandedCell.set(source);
 const dialog=await mountHeadless(Dialog);await tick();const events=[];
 dialog.$on('iocRule',event=>{assert.equal(get(expandedCell),null,'close before dispatch');events.push(event.detail);});
 dialog.$capture_state().createIocField('c1');
 assert.equal(dialog.$capture_state().$expandedCell,null,'close row details before creating the IOC draft');
 assert.deepEqual(events,[{projectId:'case',columnId:'c1',value}]);dialog.$destroy();
}
for(const kind of ['null','missing']){
 const source=target(null);source.row.cells.c1=full(null,kind);expandedCell.set(source);
 const dialog=await mountHeadless(Dialog);await tick();const events=[];
 dialog.$on('iocRule',event=>events.push(event.detail));dialog.$capture_state().createIocField('c1');
 assert.deepEqual(events,[]);dialog.$destroy();expandedCell.set(null);
}
assert.equal(reads,0,'loaded field actions do not start additional reads');
for(const outcome of ['success','failed','missing','truncated']){
 let resolve,reject;const pending=new Promise((yes,no)=>{resolve=yes;reject=no;});
 globalThis.__workspace.backend.update(api=>({...api,getCell:()=>{reads++;return pending;}}));
 const source=target('preview only');source.row.cells.c1.truncated=true;expandedCell.set(source);
 const dialog=await mountHeadless(Dialog);await tick();const events=[];
 dialog.$on('iocRule',event=>events.push(event.detail));dialog.$capture_state().createIocField('c1');
 assert.deepEqual(events,[],'pending previews cannot become IOC values');
 const exact='  resolved "quotes" C:\\Temp\\ | -literal:field\n'+'x'.repeat(300);
 if(outcome==='failed')reject(Error('read failed'));
 else resolve({...full(exact,outcome==='missing'?'missing':'string'),truncated:outcome==='truncated'});
 await new Promise(done=>setImmediate(done));await tick();dialog.$capture_state().createIocField('c1');
 assert.deepEqual(events,outcome==='success'?[{projectId:'case',columnId:'c1',value:exact}]:[],outcome);
 dialog.$destroy();expandedCell.set(null);
}
for(const stale of ['closed','destroyed','project','row']){
 projectDetail.set({project:{meta:{id:'case'}},columns:['c1','c2']});expandedCell.set(target('original'));
 const dialog=await mountHeadless(Dialog);await tick();const events=[];
 dialog.$on('iocRule',event=>events.push(event.detail));const create=dialog.$capture_state().createIocField;
 if(stale==='closed')dialog.$capture_state().closeCell();
 if(stale==='destroyed')dialog.$destroy();
 if(stale==='project')projectDetail.set({project:{meta:{id:'other'}},columns:['c1','c2']});
 if(stale==='row')expandedCell.set({...target('replacement'),rowIndex:92});
 create('c1');assert.deepEqual(events,[],stale);
 if(stale!=='destroyed')dialog.$destroy();expandedCell.set(null);
}
assert.equal(reads,4,'only the four unresolved detail fields were fetched; IOC actions add no reads');
`));
