import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('a completed copy notifies and closes only its still-mounted context menu', () => runSvelteTest(String.raw`
globalThis.document={dispatchEvent:()=>true,activeElement:null,body:{append(){}},execCommand:()=>false,
 createElement:()=>({style:{},select(){},remove(){}})};
globalThis.HTMLElement=class {};
globalThis.__workspace={backend:writable(null),projectDetail:writable({project:{meta:{id:'case'}}}),
 openCell(){},addFilterCondition(){},createReadScope:()=>({dispose(){}})};
const Menu=await compileHeadless('CellContextMenu');
const props={projectId:'case',rowIndex:1,columnId:'c1',preview:'exact old value',truncated:false,kind:'string',row:{},x:0,y:0};
console.error=()=>{};let copies=0;
for(const destroyed of [false,true])for(const failed of [false,true]){
 let release;
 const held=new Promise((done,reject)=>{release=()=>failed?reject(Error('Permission denied')):done();});
 Object.defineProperty(globalThis,'navigator',{configurable:true,value:{clipboard:{writeText:text=>{
  assert.equal(text,'exact old value');copies++;return held;
 }}}});
 const menu=await mountHeadless(Menu,props);
 let current='old-menu';const notifications=[];
 menu.$on('close',()=>{current=null;});menu.$on('notify',event=>notifications.push(event.detail.message));
 const copying=menu.$capture_state().copyValue();await Promise.resolve();
 if(destroyed){menu.$destroy();current='replacement-menu';}
 release();await copying;
 assert.equal(current,destroyed?'replacement-menu':null);
 assert.deepEqual(notifications,destroyed?[]:[failed?'Failed to copy cell value.':'Copied cell value.']);
 if(!destroyed)menu.$destroy();
}
assert.equal(copies,4);
for(const [kind,preview,expected] of [['missing','',null],['string','',''],['null','null','null']]){
 const written=[];
 Object.defineProperty(globalThis,'navigator',{configurable:true,value:{clipboard:{writeText:async text=>written.push(text)}}});
 const menu=await mountHeadless(Menu,{...props,kind,preview});
 let closed=false;const notifications=[];
 menu.$on('close',()=>{closed=true;});menu.$on('notify',event=>notifications.push(event.detail.message));
 await menu.$capture_state().copyValue();
 assert.deepEqual(written,expected===null?[]:[expected]);
 assert.equal(closed,expected!==null);
 assert.deepEqual(notifications,expected===null?[]:['Copied cell value.']);
 menu.$destroy();
}
`));

test('cell IOC drafts preserve raw values and require an owned complete response', () => runSvelteTest(String.raw`
globalThis.document={dispatchEvent:()=>true,activeElement:null};globalThis.HTMLElement=class {};
console.error=()=>{};
const projectDetail=writable({project:{meta:{id:'case'}}});let read;
globalThis.__workspace={backend:writable({getCell:args=>read(args)}),projectDetail,
 openCell(){throw Error('must not open details');},addFilterCondition(){throw Error('must not filter or annotate');},
 createReadScope:()=>({run:(_key,api,request)=>request('owned-read'),dispose(){}})};
const Menu=await compileHeadless('CellContextMenu');
const props={projectId:'case',rowIndex:91,columnId:'c000002',preview:'preview only',truncated:false,kind:'string',row:{},x:0,y:0};
for(const [kind,value] of [['string',''],['string','  -literal:a|b "quoted" C:\\Temp\\  '],
 ['array','["a|b", "C:\\Temp"]'],['object','{"name":"quoted value"}']]){
 read=()=>{throw Error('complete cells must not be fetched');};
 const menu=await mountHeadless(Menu,{...props,kind,preview:value});const events=[];
 menu.$on('close',()=>events.push('close'));menu.$on('iocRule',event=>events.push(event.detail));
 await menu.$capture_state().createIocRule();
 assert.deepEqual(events,['close',{projectId:'case',columnId:'c000002',value}]);
 menu.$destroy();
}
for(const kind of ['null','missing']){
 const menu=await mountHeadless(Menu,{...props,kind});const events=[];
 menu.$on('iocRule',event=>events.push(event.detail));await menu.$capture_state().createIocRule();
 assert.deepEqual(events,[]);menu.$destroy();
}
for(const outcome of ['success','empty','failed','missing','null','truncated','closed','destroyed','other-project','other-row']){
 projectDetail.set({project:{meta:{id:'case'}}});let resolve,reject;
 const pending=new Promise((yes,no)=>{resolve=yes;reject=no;});let requests=0;
 read=args=>{requests++;assert.deepEqual(args,{projectId:'case',rowIndex:91,columnId:'c000002',requestId:'owned-read'});return pending;};
 const menu=await mountHeadless(Menu,{...props,truncated:true});const events=[],notices=[];
 menu.$on('iocRule',event=>events.push(event.detail));menu.$on('notify',event=>notices.push(event.detail));
 const creating=menu.$capture_state().createIocRule();assert.equal(requests,1);
 if(outcome==='closed')menu.$capture_state().close();
 if(outcome==='destroyed')menu.$destroy();
 if(outcome==='other-project')projectDetail.set({project:{meta:{id:'other'}}});
 if(outcome==='other-row'){menu.$set({rowIndex:92});await tick();}
 const exact='  full "quoted" value C:\\Temp\\ | -x:y\n'+'z'.repeat(300);
 if(outcome==='failed')reject(Error('read failed'));
 else resolve({text:outcome==='empty'?'':exact,kind:outcome==='null'?'null':outcome==='missing'?'missing':'string',
  truncated:outcome==='truncated',length:exact.length});
 await creating;
 assert.deepEqual(events,['success','empty'].includes(outcome)?[{projectId:'case',columnId:'c000002',value:outcome==='empty'?'':exact}]:[],outcome);
 assert.equal(notices.length,['failed','missing','null','truncated'].includes(outcome)?1:0,outcome);
 if(outcome!=='destroyed')menu.$destroy();
}
`));
