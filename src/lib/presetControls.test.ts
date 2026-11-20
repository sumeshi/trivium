import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('GUI preset save captures filters and explicit default layout before the file picker resolves', () => runSvelteTest(String.raw`
globalThis.window={};
const {mockIPC,clearMocks}=await import('@tauri-apps/api/mocks');
const {get}=await import('svelte/store');
const {createBackend}=await import('./src/lib/backend.ts');
const {createWorkspaceController}=await import('./src/lib/workspaceController.ts');
let finishPicker, saved;
mockIPC((command,args)=>{
 if(command==='plugin:dialog|save') return new Promise(resolve=>{finishPicker=resolve;});
 assert.equal(command,'save_preset'); saved=args.payload;
});
const workspace=createWorkspaceController();
workspace.backend.set(createBackend());
workspace.currentProjectId.set('case');
workspace.projectDetail.set({project:{meta:{id:'case',name:'case',source_file_name:'case.csv'}},columns:['a','b']});
workspace.search.set('beacon'); workspace.searchColumns.set(['b']);
workspace.flagFilter.set('safe|critical'); workspace.bookmarkedOnly.set(true);
globalThis.__workspace=workspace;
const Presets=await compileHeadless('PresetControls');
const widths=new Map([['a',100],['b',200]]);
const panel=await mountHeadless(Presets,{columnWidths:widths,defaultColumnWidths:new Map(widths)});
const pending=panel.$capture_state().savePreset();
assert.equal(typeof finishPicker,'function');
workspace.search.set('later edit'); workspace.searchColumns.set(['a']);
workspace.flagFilter.set('none'); workspace.bookmarkedOnly.set(false);
workspace.columnOrder.set(['b','a']); widths.set('a',400);
finishPicker('/chosen/preset.json'); await pending;
assert.deepEqual(saved.filters,{search:'beacon',searchColumns:['b'],flagFilter:'safe|critical',bookmarkedOnly:true});
assert.deepEqual(saved.columnOrder,[],'a newly saved default order explicitly restores source order');
assert.deepEqual(saved.columnWidths,{},'default widths explicitly reset a destination with custom widths');
assert.equal(saved.destination,'/chosen/preset.json');
assert.equal(saved.allowOverwrite,true);
assert.equal(get(workspace.search),'later edit','saving never reapplies the captured state');
panel.$destroy(); workspace.dispose(); clearMocks();
`));
