import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

const tabsFixture = String.raw`
document.getElementById=()=>null;
const summary=id=>({meta:{id,name:id,source_file_name:id+'.csv'}});
const Tabs=await compileHeadless('../app/ProjectTabs');
const tabs=await mountHeadless(Tabs,{projects:['a','b','c'].map(summary),selectedProjectId:'a'});
const emitted=[];
for(const name of ['select','close','closeOthers','closeRight','closeAll'])tabs.$on(name,event=>{
 if(name!=='select')assert.equal(tabs.$capture_state().menuState,null,'dismiss before starting a close operation');
 emitted.push([name,event.detail]);
});
let focusCount=0;
const trigger={isConnected:true,getBoundingClientRect:()=>({left:24,bottom:42}),focus(){focusCount++;}};
const pointer=()=>({currentTarget:trigger,clientX:170,clientY:27,preventDefault(){},stopPropagation(){}});
const key=(value,shiftKey=false)=>({key:value,shiftKey,currentTarget:trigger,preventDefault(){},stopPropagation(){}});
`;

test('tab context actions target the clicked inactive tab without selecting it', () => runSvelteTest(tabsFixture + String.raw`
for(const [action,name,detail] of [['close','close','b'],['others','closeOthers','b'],['right','closeRight','b'],['all','closeAll',null]]){
 tabs.$capture_state().openMenu(pointer(),'b');
 assert.equal(tabs.$capture_state().selectedProjectId,'a');
 assert.deepEqual(emitted,[],'opening an inactive tab menu must not select or close a project');
 const state=tabs.$capture_state();
 assert.equal(state.menuState.projectId,'b');assert.equal(state.menuState.x,170);assert.equal(state.menuState.y,27);
 state.runMenuAction(action);
 assert.deepEqual(emitted,[[name,detail]]);
 assert.equal(tabs.$capture_state().selectedProjectId,'a');
 emitted.length=0;
}
tabs.$capture_state().openMenu(pointer(),'c');
tabs.$capture_state().runMenuAction('right');assert.deepEqual(emitted,[],'the last tab has nothing to its right');
tabs.$set({projects:[summary('a')]});await tick();
tabs.$capture_state().openMenu(pointer(),'a');
tabs.$capture_state().runMenuAction('others');
assert.deepEqual(emitted,[],'Close others is unavailable for a single tab');
tabs.$capture_state().openMenu(pointer(),'a');
tabs.$capture_state().runMenuAction('right');assert.deepEqual(emitted,[]);
tabs.$capture_state().openMenu(pointer(),'a');
tabs.$capture_state().runMenuAction('all');
assert.deepEqual(emitted,[['closeAll',null]]);
tabs.$destroy();
`));

test('keyboard context menus preserve selection and retire stale or busy tab actions', () => runSvelteTest(tabsFixture + String.raw`
for(const event of [key('ContextMenu'),key('F10',true)]){
 tabs.$capture_state().navigate(event,1);
 const state=tabs.$capture_state();
 assert.equal(state.menuState.projectId,'b');assert.equal(state.menuState.x,24);assert.equal(state.menuState.y,42);
 assert.deepEqual(emitted,[]);
 state.closeMenu(true);
}
assert.equal(focusCount,2,'Escape-style dismissal restores the context trigger');
tabs.$capture_state().openMenu(pointer(),'b');
tabs.$set({projects:[summary('a')]});await tick();
assert.equal(tabs.$capture_state().menuState,null,'a closed context target must not remain actionable');
tabs.$capture_state().runMenuAction('all');assert.deepEqual(emitted,[]);
tabs.$set({projects:['a','b'].map(summary)});await tick();
tabs.$capture_state().openMenu(pointer(),'b');
tabs.$set({busy:true});await tick();
assert.equal(tabs.$capture_state().menuState,null);
const busy=tabs.$capture_state();
busy.openMenu(pointer(),'b');busy.runMenuAction('all');busy.selectProject('b');busy.closeProject('a');
busy.navigate(key('Delete'),0);busy.navigate(key('ArrowRight'),0);
assert.equal(tabs.$capture_state().menuState,null);assert.deepEqual(emitted,[]);
tabs.$set({busy:false});await tick();
tabs.$capture_state().navigate(key('Delete'),0);
assert.deepEqual(emitted,[['close','a']],'the existing Delete shortcut stays available after the batch');
tabs.$destroy();
`));

test('the tab menu clamps to the viewport and supports focus, disabled actions and dismissal', () => runSvelteTest(String.raw`
globalThis.Node=class {};
globalThis.innerWidth=300;globalThis.innerHeight=200;
document.activeElement=null;
const Menu=await compileHeadless('../app/ProjectTabContextMenu');
const menu=await mountHeadless(Menu,{label:'inactive.csv',x:295,y:195,canCloseOthers:false});
const events=[];
menu.$on('close',event=>events.push(['close',event.detail]));
menu.$on('action',event=>events.push(['action',event.detail]));
let focuses=0;
const first={focus(){document.activeElement=first;focuses++;}},last={focus(){document.activeElement=last;focuses++;}};
const inside=new Node(),outside=new Node();
const element={isConnected:true,getBoundingClientRect:()=>({width:180,height:100}),
 contains:target=>target===inside,querySelector:()=>first,querySelectorAll:()=>[first,last]};
menu.$inject_state({menu:element});
await menu.$capture_state().placeMenu();
assert.equal(menu.$capture_state().left,112);assert.equal(menu.$capture_state().top,92);
assert.equal(document.activeElement,first);
let prevented=0,stopped=0;
const key=value=>({key:value,preventDefault(){prevented++;},stopPropagation(){stopped++;}});
for(const [value,expected] of [['ArrowDown',last],['ArrowDown',first],['End',last],['Home',first]]){
 menu.$capture_state().keydown(key(value));assert.equal(document.activeElement,expected);
}
assert.equal(prevented,4);assert.equal(stopped,4);
menu.$capture_state().keydown(key('Escape'));
assert.deepEqual(events.pop(),['close',{restoreFocus:true}]);assert.equal(prevented,5);
menu.$capture_state().keydown(key('Tab'));
assert.deepEqual(events.pop(),['close',{restoreFocus:true}]);assert.equal(prevented,5,'Tab retains normal focus traversal');
menu.$capture_state().outside({target:inside});menu.$capture_state().scrolling({target:inside});
assert.deepEqual(events,[]);
menu.$capture_state().outside({target:outside});menu.$capture_state().scrolling({target:outside});
assert.deepEqual(events,[['close',{restoreFocus:false}],['close',{restoreFocus:false}]]);events.length=0;
menu.$capture_state().choose('others');menu.$capture_state().choose('right');assert.deepEqual(events,[]);
menu.$capture_state().choose('close');menu.$capture_state().choose('all');
assert.deepEqual(events,[['action','close'],['action','all']]);events.length=0;
menu.$set({canCloseRight:true});await tick();menu.$capture_state().choose('right');
assert.deepEqual(events,[['action','right']]);events.length=0;
menu.$set({disabled:true});await tick();
menu.$capture_state().choose('close');menu.$capture_state().choose('right');menu.$capture_state().choose('all');assert.deepEqual(events,[]);
const atDestroy=focuses;menu.$destroy();
await menu.$capture_state().placeMenu();assert.equal(focuses,atDestroy,'an unmounted menu must never steal focus');
`));
