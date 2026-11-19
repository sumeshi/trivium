import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

test('the actual table reloads an unblocked adjacent visible page once without a scroll', () => {
  // Run the real reactive component/controller/query with no-op DOM rendering.
  const script = String.raw`
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { compile, preprocess } from 'svelte/compiler';
import sveltePreprocess from 'svelte-preprocess';
import { tick } from 'svelte';
import { get } from 'svelte/store';
import { set_current_component } from 'svelte/internal';
import { createWorkspaceController } from './src/lib/workspaceController.ts';
import { createWorkspaceQuery } from './src/lib/workspaceQuery.ts';
import { maxScrollTop } from './src/lib/virtualScroll.ts';
import { normalizeRow } from './src/lib/components/project_view/state.ts';
const filename=resolve('src/lib/components/project_view/DataTable.svelte');
let source=await fs.readFile(filename,'utf8');
source=source.replace(/import \{ useWorkspace \} from [^;]+;/, 'const useWorkspace = () => globalThis.__workspace;');
source=source.replace(/import (\w+) from ['"][^'"]+\.svelte['"];?/g,'const $1 = null;');
const processed=await preprocess(source,sveltePreprocess({typescript:true}),{filename});
let code=compile(processed.code,{filename,generate:'dom',dev:true,css:'external'}).js.code;
assert.match(code,/instance,\s*create_fragment/);code=code.replace(/instance,\s*create_fragment/,'instance, null');
code=code.replace(/(['"])([^'"]+)\1/g,(original,quote,name)=>{
 if(name==='svelte'||name.startsWith('svelte/'))return JSON.stringify(import.meta.resolve(name));
 if(name.startsWith('./')||name.startsWith('../'))return JSON.stringify(pathToFileURL(resolve(dirname(filename),name+(name.endsWith('.ts')?'':'.ts'))).href);
 return original;
});
globalThis.document={dispatchEvent:()=>true,activeElement:null,documentElement:{style:{setProperty(){}}}};
globalThis.HTMLElement=class {};globalThis.Element=class {};
const {default: Table}=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
const settle=async()=>{for(let step=0;step<24;step++){await Promise.resolve();await tick();}};
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
 workspace.projectDetail.set({project:{meta:{id:'case',total_records:276}},columns:['c1'],columns_meta:[],column_max_chars:{c1:4},column_kinds:{c1:'string'},iocs:[],hidden_columns:[],initial_rows:[]});
 workspace.currentProjectId.set('case');workspace.backend.set(api);
 const queries=createWorkspaceQuery(workspace,{backend:api,isReady:()=>true,restoreViewport:async()=>{},scopeChanged(){},summary(){},notify(){}});
 queries.initialize({search:'',searchColumns:[],flag:'all',bookmarkedOnly:false,condition:null});
 workspace.queryCountKnown.set(true);workspace.totalFilteredRows.set(276);
 const height=get(workspace.rowHeight);workspace.viewportHeight.set(25*height);workspace.scrollTop.set(maxScrollTop(276,25*height,height));
 const cached=records.map(normalizeRow);workspace.registerLoadedPage(0,0,[normalizeRow(row(0))]);workspace.registerLoadedPage(1,250,cached);
 set_current_component({$$:{root:{},context:new Map()}});
 const table=new Table({$$inline:true,props:{ensureRangeLoaded:queries.ensureRangeLoaded}});
 set_current_component(null);table.$$.fragment={d(){},p(){}};
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
console.log('TABLE_RECOVERY_COMPLETE');
`;
  const result = spawnSync(process.execPath, ['--conditions=browser', '--experimental-strip-types', '--input-type=module'], {
    cwd: fileURLToPath(new URL('../../', import.meta.url)), input: script, encoding: 'utf8', timeout: 15_000, maxBuffer: 1_000_000,
  });
  const error = result.stderr?.replace(/data:text\/javascript;base64,[A-Za-z0-9+/=]+/g, '<compiled table>');
  assert.equal(result.status, 0, result.error?.message ?? error);
  assert.equal(result.stdout.trim(), 'TABLE_RECOVERY_COMPLETE', 'Every deferred recovery assertion must finish');
});
