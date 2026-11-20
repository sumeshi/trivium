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

test('preset operations wait for the actual project view to finish restoring its saved state', () => runSvelteTest(String.raw`
globalThis.window={dispatchEvent(){}};
globalThis.document={dispatchEvent(){},activeElement:null,documentElement:{style:{setProperty(){}}}};
globalThis.Element=class {};globalThis.HTMLElement=class {};
const fs=(await import('node:fs/promises')).default;
const read=fs.readFile;
fs.readFile=async(...args)=>{
 let source=await read(...args);
 if(String(args[0]).endsWith('/ProjectView.svelte')&&typeof source==='string') source=source.replace("import './project-view.css';",'');
 return source;
};
const {mockIPC,clearMocks}=await import('@tauri-apps/api/mocks');
const {get}=await import('svelte/store');
let release,pickers=0,loads=0,saved=[];
const restoring=new Promise(resolve=>{release=resolve;});
const api={loadViewState:()=>restoring,saveViewState:async()=>{},cancelRequest:async()=>0,
 savePreset:async payload=>{saved.push(payload);},setHiddenColumns:async()=>{},
 loadPreset:async()=>{loads++;return {condition:null,hiddenColumns:[],sortKey:null,sortDirection:'asc',unknown_columns:[],
  filters:{search:'loaded-preset',searchColumns:['c1'],flagFilter:'critical',bookmarkedOnly:true}};},
 queryProjectRows:async()=>({offset:0,rows:[],total_rows:1,total_filtered_rows:0,annotated_total:0,annotated_filtered:0,annotation_revision:0})};
const detail={project:{meta:{id:'case',name:'case',source_file_name:'case.csv',total_records:1,flagged_records:0}},
 columns:['c1'],columns_meta:[{id:'c1',label:'Message',source:'Message',order:0}],column_kinds:{c1:'string'},
 column_max_chars:{c1:20},hidden_columns:[],iocs:[],initial_rows:[],color_rules:[]};
const Project=await compileHeadless('../ProjectView');
const project=await mountHeadless(Project,{projectDetail:detail,backend:api});
const settle=async()=>{for(let i=0;i<20;i++){await Promise.resolve();await tick();}};
await settle();
const state=project.$capture_state(),workspace=state.workspace;
assert.equal(project.$capture_state().initialized,false);
const original={...state.view.buildViewState(),search:'saved-original'};
globalThis.__workspace=workspace;
mockIPC(command=>{assert.ok(['plugin:dialog|save','plugin:dialog|open'].includes(command));pickers++;return '/chosen/preset.json';});
const Presets=await compileHeadless('PresetControls');
const panel=await mountHeadless(Presets,{ready:project.$capture_state().initialized});
await panel.$capture_state().savePreset();
await panel.$capture_state().applyPreset();
assert.equal(pickers,0,'an uninitialized view cannot open a preset picker');
assert.equal(loads,0);assert.deepEqual(saved,[],'temporary defaults cannot escape into a saved file');
assert.equal(get(workspace.search),'');
release({status:'ok',state:original});await settle();
assert.equal(project.$capture_state().initialized,true);
assert.equal(get(workspace.search),'saved-original');
panel.$set({ready:project.$capture_state().initialized});await tick();
await panel.$capture_state().applyPreset();await settle();
assert.equal(pickers,1);assert.equal(loads,1);
assert.equal(get(workspace.search),'loaded-preset');
assert.deepEqual(get(workspace.searchColumns),['c1']);
assert.equal(get(workspace.flagFilter),'critical');assert.equal(get(workspace.bookmarkedOnly),true);
await panel.$capture_state().savePreset();
assert.equal(pickers,2);assert.equal(saved.length,1);
assert.deepEqual(saved[0].filters,{search:'loaded-preset',searchColumns:['c1'],flagFilter:'critical',bookmarkedOnly:true});
panel.$destroy();project.$destroy();clearMocks();
`,20_000));
