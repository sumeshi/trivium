import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('pending note saves block automatic navigation while normal close can drain them', () => runSvelteTest(String.raw`
console.error=()=>{};
const Dialog=await compileHeadless('MemoEditorDialog');
for(const success of [false,true]){
 let settle;
 const held=new Promise((resolve,reject)=>{settle=()=>success?resolve(true):reject(Error('Storage unavailable'));});
 const row={row_index:0,flag:'',memo:'',tags:[],bookmarked:false};
 globalThis.__workspace={memoEditor:writable({projectId:'case',rowIndex:0,row}),backend:writable({}),
  currentProjectId:writable('case'),saveAnnotation:()=>held};
 const dialog=await mountHeadless(Dialog);
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
`));
