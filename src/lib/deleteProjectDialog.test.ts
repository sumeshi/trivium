import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('delete confirmation never treats a stray Enter as approval and Escape only cancels', () => runSvelteTest(String.raw`
globalThis.HTMLElement=class {};
const Dialog=await compileHeadless('../app/DeleteProjectDialog');
const dialog=await mountHeadless(Dialog,{name:'evidence.csv'});
const events=[];
dialog.$on('cancel',()=>events.push('cancel'));
dialog.$on('confirm',()=>events.push('confirm'));
const key=dialog.$capture_state().keyboard;
let stopped=0,prevented=0;
key({key:'Enter',code:'Enter',stopPropagation(){stopped++;},preventDefault(){prevented++;}});
assert.deepEqual(events,[]);
assert.equal(stopped,1);
key({key:'Escape',code:'Escape',stopPropagation(){stopped++;},preventDefault(){prevented++;}});
assert.deepEqual(events,['cancel']);
assert.equal(prevented,1);
dialog.$destroy();
`));

test('delete confirmation traps keyboard focus including WebKit Shift+Tab', () => runSvelteTest(String.raw`
globalThis.HTMLElement=class {};
const Dialog=await compileHeadless('../app/DeleteProjectDialog');
const dialog=await mountHeadless(Dialog,{name:'evidence.csv'});
const focus=[];
const first={focus(){focus.push('cancel');}},last={focus(){focus.push('delete');}};
dialog.$inject_state({dialog:{querySelectorAll:()=>[first,last]}});
await tick();
const key=dialog.$capture_state().keyboard;
let prevented=0;
document.activeElement=first;
key({key:'Unidentified',code:'Tab',shiftKey:true,stopPropagation(){},preventDefault(){prevented++;}});
document.activeElement=last;
key({key:'Tab',code:'Tab',shiftKey:false,stopPropagation(){},preventDefault(){prevented++;}});
assert.deepEqual(focus,['delete','cancel']);
assert.equal(prevented,2);
dialog.$destroy();
`));
