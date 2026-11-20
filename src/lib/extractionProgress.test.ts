import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('extraction feedback skips quick jobs, keeps one timer per scan and preserves cancellation', () => runSvelteTest(String.raw`
const Progress=await compileHeadless('../app/ExtractionProgress');
const timers=new Map();let nextTimer=0,cancelled=0;
globalThis.setTimeout=(callback,delay)=>{assert.equal(delay,150);const id=++nextTimer;timers.set(id,callback);return id;};
globalThis.clearTimeout=id=>timers.delete(id);
const pending=id=>({requestId:id,sourceName:'source.csv',request:{projectId:'source'},jobId:null,done:0,total:null,message:'Scanning…'});
const progress=await mountHeadless(Progress,{pending:pending('quick')});
progress.$on('cancel',()=>cancelled++);
assert.equal(progress.$capture_state().visible,false);
assert.equal(timers.size,1);
const first=progress.$capture_state().timer;
progress.$set({pending:{...pending('quick'),done:10,total:20}});await tick();
assert.equal(progress.$capture_state().timer,first,'progress updates must not restart the appearance delay');
assert.equal(nextTimer,1);
progress.$set({pending:null});await tick();
assert.equal(progress.$capture_state().visible,false,'quick jobs never reveal their status');
assert.equal(timers.size,0);

progress.$set({pending:pending('long')});await tick();
const second=progress.$capture_state().timer,show=timers.get(second);
timers.delete(second);show();await tick();
assert.equal(progress.$capture_state().visible,true);
progress.$set({pending:{...pending('long'),done:12,total:30}});await tick();
assert.equal(progress.$capture_state().visible,true);
assert.equal(timers.size,0,'visible progress updates must not schedule another timer');
progress.$capture_state().cancel();assert.equal(cancelled,1);
progress.$set({pending:null});await tick();
assert.equal(progress.$capture_state().visible,false);
progress.$set({pending:pending('destroyed')});await tick();
assert.equal(timers.size,1);
progress.$destroy();assert.equal(timers.size,0,'destroying the app must retire the pending reveal');
`));
