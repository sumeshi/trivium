import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

test('clearing a datetime column retires results, pending responses and queued timeline scans', () => {
  // Exercise the actual Svelte reactive blocks and lifecycle without rendering
  // the chart or adding a DOM dependency. Native tests cover the rendered UI.
  const script = String.raw`
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { compile, preprocess } from 'svelte/compiler';
import sveltePreprocess from 'svelte-preprocess';
import { writable } from 'svelte/store';
import { tick } from 'svelte';
import { set_current_component } from 'svelte/internal';
globalThis.document={dispatchEvent:()=>true};
const flush=async()=>{for(let i=0;i<5;i++){await Promise.resolve();await tick();}};
const delay=()=>new Promise(done=>setTimeout(done,150));
async function compilePanel(name){
 const filename=resolve('src/lib/components/project_view',name+'.svelte');
 let source=await fs.readFile(filename,'utf8');
 source=source.replace(/import \{ useWorkspace \} from [^;]+;/,'const useWorkspace=()=>globalThis.__workspace;');
 source=source.replace(/import (\w+) from ['"][^'"]+\.svelte['"];?/g,'const $1=class {};');
 source=source.replace(/import \{ annotationVersions \} from [^;]+;/,'const annotationVersions=globalThis.__annotations;');
 const processed=await preprocess(source,sveltePreprocess({typescript:true}),{filename});
 let code=compile(processed.code,{filename,generate:'dom',dev:true,css:'external'}).js.code;
 code=code.replace(/(['"])([^'"]+)\1/g,(original,quote,name)=>{
  if(name==='svelte'||name.startsWith('svelte/'))return JSON.stringify(import.meta.resolve(name));
  if(name.startsWith('../'))return JSON.stringify(pathToFileURL(resolve(filename,'..',name+'.ts')).href);
  return original;
 });
 assert.match(code,/instance,\s*create_fragment,/);
 code=code.replace(/instance,\s*create_fragment,/g,'instance, null,');
 return (await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'))).default;
}
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
 const Panel=await compilePanel(name);
 set_current_component({$$:{root:{},context:new Map()}});
 const panel=new Panel({$$inline:true,props:{open:true,column:'timestamp'}});
 set_current_component(null);
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
console.log('ANALYSIS_COLUMN_LIFECYCLE_COMPLETE');
`;
  const result = spawnSync(process.execPath, ['--conditions=browser', '--experimental-strip-types', '--input-type=module'], {
    cwd: fileURLToPath(new URL('../../', import.meta.url)), input: script, encoding: 'utf8', timeout: 15_000, maxBuffer: 1_000_000,
  });
  const error = result.stderr?.replace(/data:text\/javascript;base64,[A-Za-z0-9+/=]+/g, '<compiled analysis panel>');
  assert.equal(result.status, 0, result.error?.message ?? error);
  assert.equal(result.stdout.trim(), 'ANALYSIS_COLUMN_LIFECYCLE_COMPLETE');
});
