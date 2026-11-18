import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

test('pending note saves block automatic navigation while normal close can drain them', () => {
  // Exercise the actual compiled dialog actions without a DOM dependency.
  const script = String.raw`
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
import {compile,preprocess} from 'svelte/compiler';
import sveltePreprocess from 'svelte-preprocess';
import {writable} from 'svelte/store';
import {set_current_component} from 'svelte/internal';
const filename=resolve('src/lib/components/project_view/MemoEditorDialog.svelte');
let source=await fs.readFile(filename,'utf8');
source=source.replace(/import \{ useWorkspace \} from [^;]+;/,'const useWorkspace=()=>globalThis.__workspace;');
const processed=await preprocess(source,sveltePreprocess({typescript:true}),{filename});
let code=compile(processed.code,{filename,generate:'dom',dev:true,css:'external'}).js.code;
code=code.replace(/(['"])([^'"]+)\1/g,(original,quote,name)=>{
 if(name==='svelte'||name.startsWith('svelte/'))return JSON.stringify(import.meta.resolve(name));
 if(['../../keyboard','../../memoAnnotation'].includes(name))return JSON.stringify(pathToFileURL(resolve('src/lib',name.split('/').at(-1)+'.ts')).href);
 return original;
});
globalThis.document={dispatchEvent:()=>true};
console.error=()=>{};
const {default:Dialog}=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
for(const success of [false,true]){
 let settle;
 const held=new Promise((resolve,reject)=>{settle=()=>success?resolve(true):reject(Error('Storage unavailable'));});
 const row={row_index:0,flag:'',memo:'',tags:[],bookmarked:false};
 globalThis.__workspace={memoEditor:writable({projectId:'case',rowIndex:0,row}),backend:writable({}),
  currentProjectId:writable('case'),saveAnnotation:()=>held};
 set_current_component({$$:{root:{},context:new Map()}});
 const dialog=new Dialog({$$inline:true,props:{}});
 set_current_component(null);dialog.$$.fragment={d(){},p(){}};
 assert.equal(dialog.hasUncommittedChanges(),false,'An untouched editor does not block activation');
 dialog.$inject_state({memoDraft:'Evidence retained for retry'});
 assert.equal(dialog.hasUncommittedChanges(),true);
 assert.equal(dialog.hasUnsavedChanges(),true);
 const saving=dialog.$capture_state().saveMemo();
 assert.equal(dialog.hasUncommittedChanges(),true,'Keep the draft owner while its save is pending');
 assert.equal(dialog.hasUnsavedChanges(),false,'A normal close must drain this accepted save');
 settle();await saving;
 assert.equal(dialog.hasUncommittedChanges(),!success);
 assert.equal(dialog.hasUnsavedChanges(),!success);
 if(!success){
  assert.equal(dialog.$capture_state().memoDraft,'Evidence retained for retry');
  assert.equal(dialog.$capture_state().memoError,'Failed to update note.');
 }
 dialog.$destroy();
}
console.log('NOTE_NAVIGATION_COMPLETE');
`;
  const result = spawnSync(process.execPath, ['--conditions=browser', '--experimental-strip-types', '--input-type=module'], {
    cwd: fileURLToPath(new URL('../../', import.meta.url)), input: script, encoding: 'utf8', timeout: 10_000, maxBuffer: 1_000_000,
  });
  const error = result.stderr?.replace(/data:text\/javascript;base64,[A-Za-z0-9+/=]+/g, '<compiled note editor>');
  assert.equal(result.status, 0, result.error?.message ?? error);
  assert.equal(result.stdout.trim(), 'NOTE_NAVIGATION_COMPLETE');
});
