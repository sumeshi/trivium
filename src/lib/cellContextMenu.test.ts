import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

test('a completed copy notifies and closes only its still-mounted context menu', () => {
  // Run the actual compiled action and Svelte destruction callbacks. Rendering
  // is unused here; a no-op fragment avoids adding a DOM test dependency.
  const script = String.raw`
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { compile, preprocess } from 'svelte/compiler';
import sveltePreprocess from 'svelte-preprocess';
import { writable } from 'svelte/store';
import { set_current_component } from 'svelte/internal';
const filename=resolve('src/lib/components/project_view/CellContextMenu.svelte');
let source=await fs.readFile(filename,'utf8');
source=source.replace(/import \{ useWorkspace \} from [^;]+;/, 'const useWorkspace = () => globalThis.__workspace;');
const processed=await preprocess(source,sveltePreprocess({typescript:true}),{filename});
let code=compile(processed.code,{filename,generate:'dom',dev:true}).js.code;
code=code.replace(/(['"])([^'"]+)\1/g,(original,quote,name)=>{
 if(name==='svelte'||name.startsWith('svelte/'))return JSON.stringify(import.meta.resolve(name));
 if(['../../ipcValues','../../clipboard'].includes(name))return JSON.stringify(pathToFileURL(resolve('src/lib',name.split('/').at(-1)+'.ts')).href);
 return original;
});
globalThis.document={dispatchEvent:()=>true,activeElement:null,body:{append(){}},execCommand:()=>false,
 createElement:()=>({style:{},select(){},remove(){}})};
globalThis.HTMLElement=class {};
globalThis.__workspace={backend:writable(null),projectDetail:writable({project:{meta:{id:'case'}}}),
 openCell(){},addFilterCondition(){},createReadScope:()=>({dispose(){}})};
const {default: Menu}=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
const props={projectId:'case',rowIndex:1,columnId:'c1',preview:'exact old value',truncated:false,kind:'string',row:{},x:0,y:0};
console.error=()=>{};let copies=0;
for(const destroyed of [false,true])for(const failed of [false,true]){
 let release;
 const held=new Promise((done,reject)=>{release=()=>failed?reject(Error('Permission denied')):done();});
 Object.defineProperty(globalThis,'navigator',{configurable:true,value:{clipboard:{writeText:text=>{
  assert.equal(text,'exact old value');copies++;return held;
 }}}});
 set_current_component({$$:{root:{},context:new Map()}});
 const menu=new Menu({$$inline:true,props});
 set_current_component(null);menu.$$.fragment={d(){},p(){}};
 let current='old-menu';const notifications=[];
 menu.$on('close',()=>{current=null;});menu.$on('notify',event=>notifications.push(event.detail.message));
 const copying=menu.$capture_state().copyValue();await Promise.resolve();
 if(destroyed){menu.$destroy();current='replacement-menu';}
 release();await copying;
 assert.equal(current,destroyed?'replacement-menu':null);
 assert.deepEqual(notifications,destroyed?[]:[failed?'Failed to copy cell value.':'Copied cell value.']);
 if(!destroyed)menu.$destroy();
}
assert.equal(copies,4);console.log('COPY_LIFECYCLE_COMPLETE');
`;
  const result = spawnSync(process.execPath, ['--conditions=browser', '--experimental-strip-types', '--input-type=module'], {
    cwd: fileURLToPath(new URL('../../', import.meta.url)), input: script, encoding: 'utf8', timeout: 10_000, maxBuffer: 1_000_000,
  });
  const error = result.stderr?.replace(/data:text\/javascript;base64,[A-Za-z0-9+/=]+/g, '<compiled context menu>');
  assert.equal(result.status, 0, result.error?.message ?? error);
  assert.equal(result.stdout.trim(), 'COPY_LIFECYCLE_COMPLETE', 'All deferred-copy assertions must finish');
});
