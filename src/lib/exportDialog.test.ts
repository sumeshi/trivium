import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

const fixture = String.raw`
const focused=[];
globalThis.HTMLElement=class {
 constructor(name){this.name=name;this.isConnected=true;this.disabled=false;this.hidden=false;this.tabIndex=0;}
 focus(options){document.activeElement=this;focused.push({name:this.name,options});}
 getClientRects(){return this.hidden?[]:[{}];}
};
const trigger=new HTMLElement('trigger');
document.activeElement=trigger;
const close=new HTMLElement('close'),all=new HTMLElement('all'),current=new HTMLElement('current'),
 selected=new HTMLElement('selected'),cancel=new HTMLElement('cancel'),submit=new HTMLElement('export');
const controls=[close,all,current,selected,cancel,submit];
selected.disabled=true;
const element=new HTMLElement('dialog');
element.contains=target=>controls.includes(target);
element.querySelectorAll=selector=>selector.includes(':not(:disabled)')?controls.filter(item=>!item.disabled):controls;
const snapshot={search:'evidence',searchColumns:['c1'],condition:null,flagFilter:'all',bookmarkedOnly:false,
 sortKey:null,sortDirection:'asc',rowIndices:[4,9],totalRows:100,totalFilteredRows:10,selectedCount:2,visibleColumns:['c1']};
const original=structuredClone(snapshot);
const Dialog=await compileHeadless('ExportDialog');
const dialog=await mountHeadless(Dialog,{snapshot});
const events=[];
dialog.$on('close',()=>events.push(['close']));
dialog.$on('export',event=>events.push(['export',event.detail]));
const key=(value,extra={})=>({key:value,code:value,prevented:0,stopped:0,
 preventDefault(){this.prevented++;},stopPropagation(){this.stopped++;},...extra});
`;

test('export modal starts on a safe control and restores only a connected trigger', () => runSvelteTest(fixture + String.raw`
dialog.$inject_state({dialogEl:element,closeButton:close});await tick();
assert.equal(typeof dialog.$capture_state().mountDialog,'function','the modal owns initial focus');
dialog.$capture_state().mountDialog();
assert.equal(document.activeElement,close);
assert.deepEqual(focused.at(-1),{name:'close',options:{preventScroll:true}});
dialog.$destroy();
assert.equal(document.activeElement,trigger);
assert.deepEqual(focused.at(-1),{name:'trigger',options:{preventScroll:true}});
const count=focused.length;dialog.$capture_state().mountDialog();
assert.equal(focused.length,count,'a destroyed modal must not steal focus');

const second=await mountHeadless(Dialog,{snapshot});
second.$inject_state({dialogEl:element,closeButton:close});await tick();
second.$capture_state().mountDialog();trigger.isConnected=false;
const before=focused.length;second.$destroy();
assert.equal(focused.length,before,'a removed tab or trigger must not receive focus');
`));

test('export Tab boundaries stay inside while radio navigation remains native', () => runSvelteTest(fixture + String.raw`
dialog.$inject_state({dialogEl:element,closeButton:close});await tick();
const keyboard=dialog.$capture_state().keyboard;
assert.equal(typeof keyboard,'function','the modal owns its keyboard focus loop');
document.activeElement=close;
const reverse=key('Unidentified',{code:'Tab',keyCode:9,shiftKey:true});keyboard(reverse);
assert.equal(document.activeElement,submit);assert.equal(reverse.prevented,1);assert.equal(reverse.stopped,1);
const forward=key('Tab');keyboard(forward);
assert.equal(document.activeElement,close);assert.equal(forward.prevented,1);
const legacyReverse=key('Unidentified',{code:'',keyCode:9,shiftKey:true});keyboard(legacyReverse);
assert.equal(document.activeElement,submit);assert.equal(legacyReverse.prevented,1);
document.activeElement=current;
const tab=key('Tab');keyboard(tab);
assert.equal(tab.prevented,0,'normal Tab inside the radio groups uses the browser tab order');
const arrow=key('ArrowRight');keyboard(arrow);
assert.equal(arrow.prevented,0,'native radio arrow-key changes must not be suppressed');
assert.equal(arrow.stopped,1,'background shortcuts cannot receive dialog keys');
submit.disabled=true;cancel.hidden=true;document.activeElement=current;
const skip=key('Tab');keyboard(skip);
assert.equal(document.activeElement,close,'disabled and hidden trailing controls are excluded');
assert.equal(skip.prevented,1);
document.activeElement=trigger;
const outside=key('Tab');keyboard(outside);
assert.equal(document.activeElement,close,'a stray outside focus returns to the modal');
assert.equal(outside.prevented,1);
for(const control of controls)control.disabled=true;
const empty=key('Tab');keyboard(empty);
assert.equal(document.activeElement,element,'the dialog itself is the fallback when no controls can focus');
assert.equal(empty.prevented,1);
dialog.$destroy();
`));

test('export Escape cancels without bubbling and a stray Enter never exports', () => runSvelteTest(fixture + String.raw`
dialog.$inject_state({dialogEl:element,closeButton:close});await tick();
const keyboard=dialog.$capture_state().keyboard;
assert.equal(typeof keyboard,'function');
const enter=key('Enter');keyboard(enter);
assert.deepEqual(events,[]);assert.equal(enter.prevented,0);assert.equal(enter.stopped,1);
const escape=key('Escape');keyboard(escape);
assert.deepEqual(events,[['close']]);assert.equal(escape.prevented,1);assert.equal(escape.stopped,1);
dialog.$destroy();
`));

test('export keeps the captured scope, column and annotation options when submitted', () => runSvelteTest(fixture + String.raw`
dialog.$capture_state().submit();
assert.deepEqual(events.pop(),['export',{scope:'filtered',useVisibleColumns:false,includeAnnotations:true}]);
dialog.$inject_state({scope:'selected',columnMode:'visible',includeAnnotations:false});await tick();
dialog.$capture_state().submit();
assert.deepEqual(events.pop(),['export',{scope:'selected',useVisibleColumns:true,includeAnnotations:false}]);
dialog.$inject_state({scope:'all',columnMode:'all',includeAnnotations:true});await tick();
dialog.$capture_state().submit();
assert.deepEqual(events.pop(),['export',{scope:'all',useVisibleColumns:false,includeAnnotations:true}]);
assert.deepEqual(snapshot,original,'focus and option changes must not mutate the applied-query snapshot');
dialog.$destroy();
`));
