import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

const fixture = String.raw`
const {createWorkspaceController}=await import('./src/lib/workspaceController.ts');
const workspace=createWorkspaceController();
workspace.projectDetail.set({project:{meta:{id:'source',name:'source',source_file_name:'source.csv',total_records:4}},
 columns:['first','second'],columns_meta:[],column_kinds:{},column_max_chars:{},
 color_rules:[],iocs:[],hidden_columns:['second'],});
workspace.queryCountKnown.set(true);
globalThis.__workspace=workspace;
globalThis.innerHeight=900;
const Extract=await compileHeadless('ExtractPanel');
const panel=await mountHeadless(Extract);
`;

test('Extract column popover dismisses independently and preserves its selected columns', () => runSvelteTest(fixture + String.raw`
let triggerFocus=0,inputFocus=0;
const picker={closest:()=>({getBoundingClientRect:()=>({bottom:800})}),getBoundingClientRect:()=>({bottom:200})};
panel.$inject_state({columnPicker:picker,columnTrigger:{focus:()=>triggerFocus++},columnInput:{focus:()=>inputFocus++}});
await tick();
let state=panel.$capture_state();
assert.deepEqual(state.selected,['first','second'],'default extraction includes hidden data columns');
await state.toggleColumns();
assert.equal(panel.$capture_state().columnMenuOpen,true);
assert.equal(inputFocus,1);
state.toggleColumn('first');
await tick();
assert.deepEqual(panel.$capture_state().selectedColumns,['second']);
state.outside({composedPath:()=>[picker]});
assert.equal(panel.$capture_state().columnMenuOpen,true,'internal checkbox clicks keep the picker open');
let prevented=0,stopped=0;
state.escape({key:'Escape',preventDefault:()=>prevented++,stopPropagation:()=>stopped++});
assert.equal(panel.$capture_state().columnMenuOpen,false);
assert.equal(triggerFocus,1);
assert.equal(prevented,1);
assert.equal(stopped,1,'Escape is consumed before the parent drawer handler');
assert.deepEqual(panel.$capture_state().selectedColumns,['second']);
await state.toggleColumns();
state.outside({composedPath:()=>[]});
assert.equal(panel.$capture_state().columnMenuOpen,false);
panel.$destroy();workspace.dispose();
`));

test('Extract dispatches only selected columns and closes the popover without changing its scope', () => runSvelteTest(fixture + String.raw`
const format={schemaVersion:1,id:'ipv4',name:'IPv4',pattern:'[0-9.]+'};
let request=null;
panel.$on('extract',event=>request=event.detail.request);
panel.$set({selectedColumns:['second'],scope:'all'});
panel.$inject_state({columnMenuOpen:true});
await tick();
panel.$capture_state().run(format);
assert.deepEqual(request.columns,['second']);
assert.equal(request.scope,'all');
assert.equal(request.condition,null);
assert.equal(request.search,'');
assert.equal(panel.$capture_state().columnMenuOpen,false);
panel.$set({pending:{requestId:'pending'}});
await tick();
request=null;
panel.$capture_state().run(format);
assert.equal(request,null,'pending extraction prevents another scan');
panel.$destroy();workspace.dispose();
`));
