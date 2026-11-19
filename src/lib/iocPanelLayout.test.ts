import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('the compact IOC panel selects among many rules without losing drafts or changing accepted operations', () => runSvelteTest(String.raw`
const {get}=await import('svelte/store');
const {createIocEditor}=await import('./src/lib/iocEditor.ts');
const raw='  "quoted" | -value:\\path\n'+'z'.repeat(400);
const entries=Array.from({length:60},(_,index)=>({flag:index%2?'suspicious':'critical',tag:'tag-'+index,query:'host:ws'+index}));
entries[58]={flag:'critical',tag:'literal',query:'',cell:{column:'c2',value:raw}};
const detail=writable({project:{meta:{id:'case-a'}},columns:['c1','c2'],iocs:entries});
const calls=[];let changed=0,release;
const api={saveIocs:async args=>calls.push(['save',structuredClone(args)]),
 importIocs:async()=>[{flag:'safe',tag:'imported',query:'host:imported'}],
 bulkApply:async args=>{calls.push(['apply',structuredClone(args)]);return new Promise(resolve=>{release=()=>resolve({updated:2});});},
 bulkUndo:async args=>{calls.push(['undo',args]);return {updated:2};}};
const editor=createIocEditor({backend:()=>api,project:()=>get(detail),annotationsChanged:()=>changed++,saved(){}});
editor.initialize('case-a',entries);
globalThis.__workspace={iocEditor:editor,projectDetail:detail,currentProjectId:writable('case-a'),columnLabels:writable(new Map([['c1','Host'],['c2','Message']]))};
const Panel=await compileHeadless('IocRulesPanel');
const panel=await mountHeadless(Panel);const state=()=>panel.$capture_state();
const flush=async()=>{for(let i=0;i<5;i++){await Promise.resolve();await tick();}};
try{
 await flush();assert.equal(state().activeIndex,0);assert.equal(state().activeEntry.query,'host:ws0');
 await panel.focusRule(get(editor.state).entries[45].id);
 assert.equal(state().activeIndex,45);
 editor.update(state().activeEntry.id,'query','host:edited');editor.update(state().activeEntry.id,'tag','unsaved');await flush();
 await panel.focusRule(get(editor.state).entries[5].id);
 assert.equal(get(editor.state).entries[45].query,'host:edited');assert.equal(calls.length,0,'selection never saves or applies');
 await panel.focusRule(get(editor.state).entries[58].id);
 assert.equal(state().activeEntry.cell.value,raw);
 assert.ok(state().summary(state().activeEntry).length<190,'the list preview stays bounded while the editor retains full text');
 const applying=editor.apply(state().activeEntry.id);await flush();assert.equal(state().busy,true);
 await panel.focusRule(get(editor.state).entries[45].id);
 editor.update(state().activeEntry.id,'tag','blocked');assert.equal(get(editor.state).entries[45].tag,'unsaved');
 release();await applying;await flush();
 assert.deepEqual(calls[0][1].condition,{column:'c2',op:'eq',values:[raw],negated:false});assert.equal(changed,1);
 state().add();await flush();assert.equal(state().activeIndex,60);
 editor.update(state().activeEntry.id,'query','host:new');await flush();
 assert.equal(await editor.save(),true);await flush();
 assert.equal(state().activeIndex,60,'saving regenerates IDs without jumping to the first rule');assert.equal(state().activeEntry.query,'host:new');
 editor.update(state().activeEntry.id,'query','discarded');editor.discard();await flush();
 assert.equal(state().activeIndex,60);assert.equal(state().activeEntry.query,'host:new');
 state().remove(state().activeEntry.id);await flush();assert.equal(state().activeIndex,59);
 state().remove(get(editor.state).entries[0].id);await flush();assert.equal(state().activeIndex,58);assert.equal(state().activeEntry.query,'host:ws59');
 assert.equal(await editor.save(),true);await flush();assert.equal(state().activeEntry.query,'host:ws59');
 await editor.undo();assert.equal(changed,2);
 assert.equal(await editor.importFile('/rules.csv'),true);await flush();
 assert.equal(state().activeIndex,0);assert.equal(state().activeEntry.query,'host:imported');
 await panel.focusRule('no-longer-present');assert.equal(state().activeEntry.query,'host:imported');
 editor.update(state().activeEntry.id,'query','|');await flush();assert.ok(state().activeProblem);
 assert.equal(await editor.apply(state().activeEntry.id),false,'invalid selected rules cannot apply');
}finally{panel.$destroy();editor.dispose();}
`,15_000));
