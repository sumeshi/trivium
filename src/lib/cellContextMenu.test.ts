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
