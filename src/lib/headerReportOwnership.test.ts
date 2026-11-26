import { test } from 'node:test';
import { runSvelteTest } from '../../tests/ui/headlessSvelte.ts';

test('an open import report closes on case changes without closing on a same-case refresh', () => runSvelteTest(String.raw`
const Header=await compileHeadless('../app/Header');
const report={encoding:'utf-8',bom:false,hasHeaders:true,sourceRecords:2,importedRecords:1,
 skippedRecords:1,repairedRecords:0,generatedColumns:0,unreadableTail:false,issueCount:1,issues:[]};
const detail=(id,name=id)=>({project:{meta:{id,name,import_report:structuredClone(report)}}});
let exports=0;
const header=await mountHeadless(Header,{projectDetail:detail('case-a'),backend:{
 exportImportReport:async()=>{exports++;throw Error('Changing cases must not export a report');}
}});
await tick();
header.$inject_state({importReportOpen:true});await tick();
assert.equal(header.$capture_state().importReportOpen,true);
header.$set({projectDetail:detail('case-a','refreshed same case')});await tick();
assert.equal(header.$capture_state().importReportOpen,true,'refreshing the same case retains the open report');
header.$set({projectDetail:detail('case-b')});await tick();
assert.equal(header.$capture_state().importReportOpen,false,'a background import cannot retarget an open report');
header.$inject_state({importReportOpen:true});await tick();
header.$set({projectDetail:null});await tick();
assert.equal(header.$capture_state().importReportOpen,false,'clearing the active case closes its report');
header.$set({projectDetail:detail('case-a')});await tick();
assert.equal(header.$capture_state().importReportOpen,false,'loading another case cannot reopen a previous report');
assert.equal(exports,0);
assert.deepEqual(header.$capture_state().projectDetail.project.meta.import_report,report);
header.$destroy();
`));
