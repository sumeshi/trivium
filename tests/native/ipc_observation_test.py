"""The native observers read each IPC body once and hold the decoded value."""
import json
import shutil
import subprocess
import unittest

from row_navigation import WebView
from workspace_ui import WorkspaceView


def instrument_source(owner):
    class Capture:
        def script(self, source):
            self.source = source
            return True
    capture = Capture()
    owner.instrument(capture)
    return capture.source


@unittest.skipUnless(shutil.which("node"), "Observer semantics require the repository's Node runtime")
class IpcObservationTests(unittest.TestCase):
    def check(self, owner, body):
        source = instrument_source(owner)
        driver = r"""
const assert=require('node:assert/strict'),input=JSON.parse(require('node:fs').readFileSync(0,'utf8'));
global.window=global;global.addEventListener=()=>{};
let reads=0,calls=0,status='ok',failure=null;
let value={rows:[{row_index:7,cells:{time:'2024-01-01T00:00:00.123456789Z'}}],total_rows:9,total_filtered_rows:9,position:7};
global.fetch=async()=>{
 calls++;if(failure)throw failure;
 const response=new Response(JSON.stringify(value),{headers:{'Tauri-Response':status}}),json=response.json.bind(response);
 response.clone=()=>{throw Error('An IPC response must not be cloned');};
 response.json=(...args)=>{reads++;return json(...args);};return response;
};
new Function(input.source)();
const call=(name,payload={projectId:'p1'})=>fetch(`ipc://localhost/${name}`,{body:JSON.stringify({payload})});
const tick=()=>new Promise(resolve=>setImmediate(resolve));
(async()=>{__BODY__})().catch(error=>{console.error(error);process.exitCode=1;});
""".replace("__BODY__", body)
        completed = subprocess.run([shutil.which("node"), "-e", driver],
                                   input=json.dumps({"source": source}), text=True,
                                   capture_output=True, timeout=15)
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_both_observers_forward_before_the_only_decode_and_keep_error_stages(self):
        for owner, trace in [(WebView, '__navigationIpc'), (WorkspaceView, '__workspaceIpc')]:
            with self.subTest(observer=owner.__name__):
                self.check(owner, r"""
const response=await call('query_project_rows');assert.equal(reads,0);
assert.equal(window.TRACE[0].decode_started,undefined);
assert.deepEqual(await response.json(),value);assert.equal(reads,1);
const entry=window.TRACE[0];assert.equal(entry.result.rows,1);
assert.ok(entry.started<=entry.fetch_received&&entry.fetch_received<=entry.decode_started&&entry.decode_started<=entry.decode_done);
status='error';value='Cancelled.';
assert.equal(await(await call('query_project_rows')).json(),'Cancelled.');
assert.equal(window.TRACE.at(-1).error,'Cancelled.');assert.equal(reads,2);
failure=Error('transport unavailable');await assert.rejects(call('query_project_rows'),/transport unavailable/);
assert.ok(window.TRACE.at(-1).fetch_failed>=0);assert.equal(reads,2);
""".replace('TRACE', trace))

    def test_workspace_dispatch_and_decoded_response_holds_remain_distinct(self):
        self.check(WorkspaceView, r"""
window.__workspaceBeforeCommit={name:'update_flag',projectId:'p1',rowIndex:7,ready:false};
const pending=call('update_flag',{projectId:'p1',row_index:7});await tick();
assert.equal(calls,0);assert.equal(window.__workspaceBeforeCommit.ready,true);
assert.equal(window.__workspaceIpc[0].dispatched,false);
window.__workspaceBeforeCommit.release();window.__workspaceBeforeCommit.release();
const response=await pending;assert.equal(calls,1);assert.equal(reads,0);
assert.deepEqual(await response.json(),value);delete window.__workspaceBeforeCommit;
window.__workspaceHold={name:'query_project_rows',projectId:'p1',search:'keep',offset:250,total:9,ready:false};
const held=await call('query_project_rows',{projectId:'p1',search:'keep',offset:250});
let settled=false;const decoded=held.json().then(result=>{settled=true;return result;});await tick();
assert.equal(window.__workspaceHold.ready,true);assert.equal(settled,false);
assert.equal(window.__workspaceHold.result.rows,1);
assert.equal(window.__workspaceIpc.at(-1).decode_done!==undefined,true);
window.__workspaceHold.release();assert.deepEqual(await decoded,value);assert.equal(reads,2);
delete window.__workspaceHold;
const histogram=await(await call('histogram')).json();histogram.rows.length=0;
assert.equal(window.__workspaceIpc.at(-1).result.rows.length,1,'The histogram trace retains the native snapshot');
""")

    def test_navigation_lookup_holds_the_real_decoded_position(self):
        self.check(WebView, r"""
window.__navigationHold={ready:false};
const response=await call('locate_project_row');assert.equal(reads,0);
let settled=false;const result=response.json().then(value=>{settled=true;return value;});await tick();
assert.equal(window.__navigationHold.ready,true);assert.equal(settled,false);
assert.equal(window.__navigationIpc.at(-1).result.position,7);
window.__navigationHold.release();assert.deepEqual(await result,value);assert.equal(reads,1);
""")


if __name__ == '__main__':
    unittest.main()
