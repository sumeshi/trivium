import assert from 'node:assert/strict';
import { test } from 'node:test';
import { columnKindLabel } from './columnTypes.ts';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('column kinds distinguish inferred types and missing metadata', () => {
  const kinds = ['string', 'int', 'float', 'datetime', 'null', undefined] as const;
  assert.deepEqual(kinds.map(columnKindLabel), ['Text', 'Int', 'Float', 'Datetime', 'Null', 'Unknown']);
});

test('only inferred datetime columns expose and dispatch the ISO display toggle', () => runSvelteTest(String.raw`
const {readFile}=await import('node:fs/promises');
const {dirname,extname,resolve}=await import('node:path');
const {pathToFileURL}=await import('node:url');
const {compile,preprocess}=await import('svelte/compiler');
const {default:sveltePreprocess}=await import('svelte-preprocess');
const filename=resolve('src/lib/components/project_view/ColumnContextMenu.svelte');
const source=await preprocess(await readFile(filename,'utf8'),sveltePreprocess({typescript:true}),{filename});
let code=compile(source.code,{filename,generate:'ssr',css:'external'}).js.code;
code=code.replace(/((?:^|\n)import(?:\s+[^;]+?\s+from)?\s*)(['"])([^'"]+)\2/g,(_all,prefix,_quote,specifier)=>{
 const url=specifier.startsWith('.')?pathToFileURL(resolve(dirname(filename),specifier+(extname(specifier)?'':'.ts'))).href:import.meta.resolve(specifier);
 return prefix+JSON.stringify(url);
});
const {default:RenderedMenu}=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
const Menu=await compileHeadless('ColumnContextMenu');
const props={columnId:'c1',label:'Evidence',x:0,y:0};
for(const kind of ['string','int','float','null',undefined,'datetime']){
 for(const displayFormat of ['original','iso']){
  const html=RenderedMenu.render({...props,columnKind:kind,displayFormat}).html;
  const labels={string:'Text',int:'Int',float:'Float',datetime:'Datetime',null:'Null',unknown:'Unknown'};
  const kindLabel=labels[kind??'unknown'];
  assert.match(html,/class="column-menu-name[^\"]*">Evidence<\/span>/);
  const typeMarkup=html.match(/class="column-menu-type[^\"]*"[^>]*data-column-kind="([^\"]+)"[^>]*>([^<]*)</);
  assert.deepEqual(typeMarkup?.slice(1),[kind??'unknown','['+kindLabel+']']);
  assert.match(html,new RegExp('data-column-kind="'+(kind??'unknown')+'"'));
  assert.equal((html.match(/role="menuitem"/g)??[]).length,kind==='datetime'?6:5,kind);
  assert.equal(html.includes(displayFormat==='iso'?'Show original values':'Show ISO datetime'),kind==='datetime',kind);
  for(const text of ['Search this column','Add rule for this column','Group by this column','Hide this column','Freeze columns through this column'])assert.ok(html.includes(text),text);
  const menu=await mountHeadless(Menu,{...props,columnKind:kind,displayFormat});
  const events=[];menu.$on('format',event=>events.push(event.detail));
  menu.$capture_state().toggleDisplayFormat();
  assert.deepEqual(events,kind==='datetime'?[{columnId:'c1',format:displayFormat==='iso'?'original':'iso'}]:[],kind);
  menu.$destroy();
 }
}
// A menu left open while metadata changes must not retain an old format action.
const menu=await mountHeadless(Menu,{...props,columnKind:'datetime',displayFormat:'iso'});
const events=[];menu.$on('format',event=>events.push(event.detail));
menu.$set({columnKind:'string'});await tick();
menu.$capture_state().toggleDisplayFormat();assert.deepEqual(events,[]);
menu.$set({columnKind:'datetime'});await tick();
menu.$capture_state().toggleDisplayFormat();assert.deepEqual(events,[{columnId:'c1',format:'original'}]);
menu.$destroy();
`));

test('restored ISO preferences only format inferred datetime cells without changing saved state', () => runSvelteTest(String.raw`
const {readFile}=await import('node:fs/promises');
const {dirname,extname,resolve}=await import('node:path');
const {pathToFileURL}=await import('node:url');
const {get}=await import('svelte/store');
const {compile,preprocess}=await import('svelte/compiler');
const {default:sveltePreprocess}=await import('svelte-preprocess');
const columnFormats=writable({c1:'iso'});
globalThis.__workspace={columnFormats,search:writable(''),setFlag(){},editMemo(){},openCell(){},handleCellKeydown(){},toggleRowSelection(){}};
const filename=resolve('src/lib/components/project_view/TableRow.svelte');
let source=await readFile(filename,'utf8');
source=source.replace(/import \{ useWorkspace \} from [^;]+;/,'const useWorkspace=()=>globalThis.__workspace;')
 .replace(/import RowTagsMenu from [^;]+;/,'const RowTagsMenu=null;');
const processed=await preprocess(source,sveltePreprocess({typescript:true}),{filename});
let code=compile(processed.code,{filename,generate:'ssr',css:'external'}).js.code;
code=code.replace(/((?:^|\n)import(?:\s+[^;]+?\s+from)?\s*)(['"])([^'"]+)\2/g,(_all,prefix,_quote,specifier)=>{
 const url=specifier.startsWith('.')?pathToFileURL(resolve(dirname(filename),specifier+(extname(specifier)?'':'.ts'))).href:import.meta.resolve(specifier);
 return prefix+JSON.stringify(url);
});
const {default:Row}=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
const raw='2026-11-19T09:00:00.123456789+09:00';
const row={row_index:0,cells:{c1:{kind:'string',text:raw,truncated:false}},displayCache:{c1:raw},previewCache:{c1:raw},
 flag:'',memo:'',tags:[],bookmarked:false};
const props={row,position:0,projectId:'case',columns:[{id:'c1',index:0,frozen:false}],columnCount:1,gridTemplate:'',
 frozenOffsets:new Map(),frozenPan:0,lastFrozen:null,focusColumn(){},openCellMenu(){},columnTab(){}};
const text=html=>html.match(/class="cell-text">([^<]*)</)?.[1].trim();
for(const kind of ['string','int','float','null',undefined,'datetime']){
 const html=Row.render({...props,columnKinds:kind?{c1:kind}:{}}).html;
 assert.equal(text(html),kind==='datetime'?'2026-11-19T00:00:00.123456789Z':raw,kind);
 assert.deepEqual(get(columnFormats),{c1:'iso'},'unsupported saved presentation stays untouched');
 assert.equal(row.cells.c1.text,raw,'source evidence is never reformatted');
}
columnFormats.set({});
assert.equal(text(Row.render({...props,columnKinds:{c1:'datetime'}}).html),raw,'original display remains original');
`));
