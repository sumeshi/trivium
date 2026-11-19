import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('fitting a column ends any drag and keeps manual resize keyboard controls', () => runSvelteTest(String.raw`
const listeners=new Map();let cancelled=0,frame;
globalThis.window={addEventListener:(name,handler)=>listeners.set(name,handler),removeEventListener:name=>listeners.delete(name)};
globalThis.requestAnimationFrame=callback=>{frame=callback;return 7;};
globalThis.cancelAnimationFrame=id=>{assert.equal(id,7);cancelled++;frame=null;};
const Handle=await compileHeadless('ColumnResizeHandle');
const handle=await mountHeadless(Handle,{label:'Evidence',width:180});
const events=[];handle.$on('fit',()=>events.push('fit'));handle.$on('resize',event=>events.push(event.detail.width));
const mouse={button:0,clientX:30,preventDefault(){},stopPropagation(){}};
try{
 handle.$capture_state().begin(mouse);
 handle.$capture_state().move({...mouse,clientX:80});
 assert.equal(typeof frame,'function');
 handle.$capture_state().fit(mouse);
 assert.deepEqual(events,['fit']);assert.equal(cancelled,1);assert.equal(listeners.size,0);
 handle.$capture_state().finish();assert.deepEqual(events,['fit'],'a late mouseup must not overwrite the fitted width');
 let prevented=0;
 const key=key=>({key,altKey:false,ctrlKey:false,metaKey:false,shiftKey:false,preventDefault(){prevented++;},stopPropagation(){}});
 handle.$capture_state().keydown(key('Enter'));
 assert.deepEqual(events,['fit','fit']);
 handle.$capture_state().keydown(key('ArrowRight'));
 assert.deepEqual(events,['fit','fit',190]);
 handle.$capture_state().keydown({...key('ArrowLeft'),shiftKey:true});
 assert.equal(events.at(-1),179);
 assert.equal(prevented,3);
}finally{handle.$destroy();}
`));
