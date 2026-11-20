import assert from "node:assert/strict";
import { test } from "node:test";
import { appendGroupPath, groupDisplayValue, groupPathKey, groupRowsPage, selectedGroupGranularities, sortGroupTree } from "./grouping.ts";
import type { GroupTreeNode } from "./types";
import type { EditorGroup } from "./stores/condition";
import { runSvelteTest } from "../../tests/ui/headlessSvelte.ts";
import { get } from "svelte/store";
import { createConditionState, presetCondition } from "./stores/condition.ts";

const node = (label: string, children: GroupTreeNode[] = [], value: unknown = label): GroupTreeNode =>
  ({ label, children, value, count: 1 });

test("group ordering sorts siblings by name or count without flattening or changing source values", () => {
  const nodes = [
    { ...node("beta", [{ ...node("z"), count: 3 }, node("a")]), count: 4 },
    { ...node("alpha", [node("9007199254740993", [], 9007199254740992)]), count: 2 },
    { ...node("gamma"), count: 2 },
  ];
  const original = structuredClone(nodes);
  const labels = (by: "name" | "count", direction: "asc" | "desc") =>
    sortGroupTree(nodes, by, direction).map(part => part.label);
  assert.deepEqual(labels("name", "asc"), ["alpha", "beta", "gamma"]);
  assert.deepEqual(labels("name", "desc"), ["gamma", "beta", "alpha"]);
  assert.deepEqual(labels("count", "asc"), ["alpha", "gamma", "beta"]);
  assert.deepEqual(labels("count", "desc"), ["beta", "alpha", "gamma"]);
  const sorted = sortGroupTree(nodes, "count", "desc");
  assert.deepEqual(sorted[0].children.map(part => part.label), ["z", "a"]);
  const originalPath = [nodes[1], nodes[1].children[0]];
  const sortedPath = [sorted[1], sorted[1].children[0]];
  assert.equal(groupPathKey(sortedPath), groupPathKey(originalPath));
  const root: EditorGroup = { type: "group", id: "root", kind: "and", collapsed: false, children: [] };
  const filtered = appendGroupPath(root, ["parent", "large"], sortedPath);
  assert.equal(filtered.children[1].type === "condition" && filtered.children[1].value, "9007199254740993");
  assert.deepEqual(nodes, original);
});

test("sorting retains null, empty and whitespace groups and exact calendar ranges", () => {
  const day = { ...node("2026-11-14"), range: { start: "2026-11-14T00:00:00Z", end: "2026-11-15T00:00:00Z" } };
  const nodes = [node("", [], null), day, node(""), node(" ")];
  const sorted = sortGroupTree(nodes, "name", "asc");
  assert.deepEqual(sorted.map(groupDisplayValue), ["(empty string)", "(missing / null)", '" "', "2026-11-14"]);
  assert.equal(new Set(sorted.map(part => groupPathKey([part]))).size, 4);
  assert.deepEqual(sorted.at(-1)?.range, day.range);
  assert.deepEqual(sortGroupTree(nodes, "name", "desc").map(groupDisplayValue), sorted.map(groupDisplayValue).reverse());
});

test("sorting precedes pagination and preserves expanded paths across sibling reordering", () => {
  const root = node("parent", Array.from({ length: 205 }, (_, index) =>
    ({ ...node(`child-${String(index).padStart(3, "0")}`), count: index + 1 })));
  const expanded = new Set([groupPathKey([root])]);
  const sorted = sortGroupTree([root], "count", "desc");
  const first = groupRowsPage(sorted, expanded, 0);
  const second = groupRowsPage(sorted, expanded, 1);
  const last = groupRowsPage(sorted, expanded, 2);
  assert.equal(first.rows[1].node.label, "child-204");
  assert.equal(second.rows[0].node.label, "child-105");
  assert.equal(last.rows.at(-1)?.node.label, "child-000");
  const rows = [first, second, last].flatMap(part => part.rows);
  assert.equal(rows.length, 206);
  assert.equal(new Set(rows.map(part => part.key)).size, 206);
  assert.deepEqual(second.rows[0].path.map(part => part.label), ["parent", "child-105"]);
});

test("legacy group columns use exact defaults and retain explicit time granularity choices", () => {
  const columns = ["__proto__", "constructor", "toString", "timestamp"];
  assert.deepEqual(selectedGroupGranularities(columns, {}), ["exact", "exact", "exact", "exact"]);
  const settings = JSON.parse('{"__proto__":"day","constructor":"hour","toString":"minute"}');
  assert.deepEqual(selectedGroupGranularities(columns, settings), ["day", "hour", "minute", "exact"]);
  assert.deepEqual(JSON.parse(JSON.stringify(selectedGroupGranularities(columns, settings))), ["day", "hour", "minute", "exact"]);
  assert.deepEqual(selectedGroupGranularities([...columns].reverse(), settings), ["exact", "minute", "hour", "day"]);
});

test("group keys preserve null, empty, whitespace and embedded separators", () => {
  const paths = [[node("", [], null)], [node("")], [node(" ")],
    [node("a\u0001b")], [node("a"), node("b")]];
  assert.equal(new Set(paths.map(groupPathKey)).size, paths.length);
  assert.equal(groupDisplayValue(node("", [], null)), "(missing / null)");
  assert.equal(groupDisplayValue(node("")), "(empty string)");
  assert.equal(groupDisplayValue(node("  ")), '"  "');
  assert.equal(groupDisplayValue(node("NaN", [], null)), "NaN");
});

test("group pages include expanded ancestors only and clamp after collapse", () => {
  const children = Array.from({ length: 205 }, (_, index) => node(`child-${index}`));
  const root = node("parent", children);
  const open = new Set([groupPathKey([root])]);
  const page = groupRowsPage([root, node("other")], open, 1);
  assert.equal(page.rows.length, 100);
  assert.equal(page.total, 207);
  assert.equal(page.rows[0].node.label, "child-99");
  assert.deepEqual(page.rows[0].path.map((part) => part.label), ["parent", "child-99"]);
  const collapsed = groupRowsPage([root, node("other")], new Set(), 2);
  assert.equal(collapsed.page, 0);
  assert.equal(collapsed.pages, 1);
  assert.deepEqual(collapsed.rows.map((row) => row.node.label), ["parent", "other"]);
});

test("nested drilldown narrows an existing OR without losing exact values", () => {
  const root: EditorGroup = {
    type: "group", id: "root", kind: "or", collapsed: false,
    children: [{ type: "condition", id: "old", column: "x", op: "eq", value: "yes", negated: false }],
  };
  let id = 0;
  const next = appendGroupPath(root, ["parent", "child", "absent"],
    [node("a,b"), node("9007199254740993", [], 9007199254740992), node("", [], null)], () => `id-${++id}`);
  assert.equal(next.kind, "and");
  assert.equal(next.children[0].type === "group" && next.children[0].kind, "or");
  assert.deepEqual(next.children.slice(1).map((row) => row.type === "condition" ? [row.column, row.op, row.value] : []),
    [["parent", "eq", "a,b"], ["child", "eq", "9007199254740993"], ["absent", "ne", ""], ["absent", "regex", "\\A\\z"]]);
  assert.equal(root.children.length, 1);
});

test("empty group paths leave existing conditions untouched", () => {
  const root: EditorGroup = { type: "group", id: "root", kind: "and", collapsed: false, children: [] };
  assert.equal(appendGroupPath(root, [], []), root);
  const next = appendGroupPath(root, ["x"], [node("")]);
  assert.equal(next.children[0].type === "condition" && next.children[0].value, "");
  assert.equal(next.children[0].type === "condition" && next.children[0].op, "eq");
  const special = appendGroupPath(root, ["x"], [node("NaN", [], null)]);
  assert.equal(special.children.length, 1);
  assert.equal(special.children[0].type === "condition" && special.children[0].value, "NaN");
});

test("calendar drilldown adds an exact half-open range after excluding blank timestamps", () => {
  const root: EditorGroup = { type: "group", id: "root", kind: "and", collapsed: false, children: [] };
  const day = { ...node("2026-11-14"), range: { start: "2026-11-14T00:00:00Z", end: "2026-11-15T00:00:00Z" } };
  const next = appendGroupPath(root, ["host", "timestamp"], [node("host-a"), day]);
  assert.deepEqual(next.children.map((row) => row.type === "condition" ? [row.column, row.op, row.value] : []), [
    ["host", "eq", "host-a"], ["timestamp", "regex", "\\S"],
    ["timestamp", "ge", day.range.start], ["timestamp", "lt", day.range.end],
  ]);
  assert.equal(root.children.length, 0);
  assert.deepEqual(day.range, { start: "2026-11-14T00:00:00Z", end: "2026-11-15T00:00:00Z" });
});

test("time ranges narrow an existing OR group without replacing its filters", () => {
  const root: EditorGroup = { type: "group", id: "root", kind: "or", collapsed: false,
    children: [{ type: "condition", id: "keep", column: "message", op: "contains", value: "logon", negated: false }] };
  const hour = { ...node("2026-11-14T23:00Z"), range: { start: "2026-11-14T23:00:00Z", end: "2026-11-15T00:00:00Z" } };
  let id = 0;
  const next = appendGroupPath(root, ["timestamp"], [hour], () => `time-${++id}`);
  assert.equal(next.kind, "and");
  assert.equal(next.children[0].type === "group" && next.children[0].kind, "or");
  assert.deepEqual(next.children.slice(1).map((row) => row.type === "condition" ? row.op : "group"), ["regex", "ge", "lt"]);
  assert.equal(root.children.length, 1);
});

test("removing the last group level retires errors, pending replies and displayed groups", () => runSvelteTest(String.raw`
const flush=async()=>{for(let i=0;i<5;i++){await Promise.resolve();await tick();}};
const pending=[];
globalThis.__annotations=writable({});
globalThis.__workspace={backend:writable({groupTree:()=>new Promise((resolve,reject)=>pending.push({resolve,reject}))}),
 projectDetail:writable({project:{meta:{id:'case'}},columns:['timestamp'],column_kinds:{timestamp:'datetime'}}),
 columnLabels:writable(new Map()),conditionTree:writable({type:'group',id:'root',kind:'and',children:[]}),
 appliedFilters:writable({search:'',searchColumns:[],flag:'all',bookmarkedOnly:false,condition:null}),
 groupSelection:writable(null),filterByGroupPath(){},clearGroupFilter(){},
 createReadScope:()=>({run:(_key,_api,work)=>work('request'),cancel(){},cancelAll(){},dispose(){}})};
const Panel=await compileHeadless('GroupingPanel');
const panel=await mountHeadless(Panel,{groupColumns:['timestamp'],granularities:{timestamp:'day'}});
const state=()=>panel.$capture_state();
try{
 await flush();assert.equal(pending.length,1);
 pending[0].reject(Error('Invalid timestamp in the selected level.'));await flush();
 assert.equal(state().error,'Invalid timestamp in the selected level.');
 // The remove-level button writes the remaining columns directly.
 panel.$set({groupColumns:[]});await flush();
 assert.equal(state().error,'','An ungrouped panel must not retain a failure for removed criteria.');
 assert.equal(state().tree,null);assert.equal(state().loading,false);
 panel.$set({groupColumns:['timestamp']});await flush();assert.equal(pending.length,2);
 panel.$set({groupColumns:[]});await flush();
 pending[1].reject(Error('Old request failed.'));await flush();
 assert.equal(state().error,'','A retired request cannot republish an error.');
 panel.$set({groupColumns:['timestamp']});await flush();assert.equal(pending.length,3);
 pending[2].resolve({columns:['timestamp'],nodes:[{label:'2026-11-19',value:'2026-11-19',count:1,children:[]}],total:1});await flush();
 assert.equal(state().tree.total,1);
 panel.$set({groupColumns:[]});await flush();
 assert.equal(state().tree,null);assert.equal(state().error,'');assert.equal(state().loading,false);
}finally{panel.$destroy();}
`));

test("repeated group selection is a no-op and Ungroup restores pre-existing OR rules", () => {
  const state = createConditionState();
  const original = { kind: "or" as const, children: [
    { column: "gate", op: "eq" as const, values: ["keep"], negated: false },
    { column: "gate", op: "eq" as const, values: ["other"], negated: false },
  ] };
  state.conditionTree.set(presetCondition(original)!);
  state.filterByGroupPath(["host", "user"], [node("alpha"), node("a,b")]);
  const selected = get(state.conditionTree);
  let publications = 0;
  const unsubscribe = state.conditionTree.subscribe(() => publications++);
  for (let index = 0; index < 5; index++) state.filterByGroupPath(["host", "user"], [node("alpha"), node("a,b")]);
  assert.equal(get(state.conditionTree), selected);
  assert.equal(publications, 1, "unchanged selections do not publish another query/history update");
  assert.equal(selected.children.length, 3);
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [original,
    { column: "host", op: "eq", values: ["alpha"], negated: false },
    { column: "user", op: "eq", values: ["a,b"], negated: false },
  ] });
  state.clearGroupFilter();
  assert.deepEqual(get(state.conditionPayload), original);
  unsubscribe();
});

test("a new group path replaces its own constraints and retains later manual filters", () => {
  const state = createConditionState();
  state.filterByValue("gate", "keep");
  state.filterByGroupPath(["host", "user"], [node("alpha"), node("alice")]);
  state.filterByValue("message", "manual");
  state.filterByGroupPath(["host", "user"], [node("beta"), node("bob")]);
  const conditions = get(state.conditionTree).children as import("./stores/condition").EditorCondition[];
  assert.deepEqual(conditions.map(row => [row.column, row.value]), [
    ["gate", "keep"], ["host", "beta"], ["user", "bob"], ["message", "manual"],
  ]);
  state.moveNode(conditions[2].id, 1);
  state.clearGroupFilter();
  assert.deepEqual((get(state.conditionTree).children as typeof conditions).map(row => [row.column, row.value]), [
    ["gate", "keep"], ["message", "manual"],
  ]);
});

test("existing exact rules and restored paths are not duplicated or claimed by grouping", () => {
  const state = createConditionState();
  const original = { column: "host", op: "eq" as const, values: ["alpha"], negated: false };
  state.conditionTree.set(presetCondition(original)!);
  state.filterByGroupPath(["host", "user"], [node("alpha"), node("alice")]);
  assert.equal(get(state.conditionTree).children.length, 2);
  state.clearGroupFilter();
  assert.deepEqual(get(state.conditionPayload), original);
  state.filterByGroupPath(["user"], [node("alice")]);
  const saved = structuredClone(get(state.conditionTree));
  state.conditionTree.set(saved);
  state.filterByGroupPath(["user"], [node("alice")]);
  assert.equal(get(state.conditionTree).children.length, 2);
  state.clearGroupFilter();
  assert.deepEqual(get(state.conditionTree), saved, "restored Rules are preserved without proof of control ownership");
});

test("editing a group constraint converts it into a user rule that Ungroup preserves", () => {
  const state = createConditionState();
  state.filterByGroupPath(["host"], [node("alpha")]);
  state.patchCondition(get(state.conditionTree).children[0].id, { value: "manually edited" });
  const edited = structuredClone(get(state.conditionTree));
  state.clearGroupFilter();
  assert.deepEqual(get(state.conditionTree), edited);
  state.filterByGroupPath(["host"], [node("beta")]);
  assert.equal(get(state.conditionTree).children.length, 2);
  state.clearGroupFilter();
  assert.deepEqual(get(state.conditionTree), edited);
});

test("managed rule IDs identify only predicates added by Groups and conversion preserves their exact query", () => {
  const state = createConditionState();
  state.addFilterCondition("event", "eq", "1");
  const manualId = get(state.conditionTree).children[0].id;
  const date = { ...node("2026-11-19"), range: {
    start: "2026-11-19T00:00:00.000000001Z", end: "2026-11-20T00:00:00.000000001Z",
  } };
  state.filterByGroupPath(["event", "created", "host"], [node("1"), date, node("", [], null)]);
  const owned = state.snapshotGroupFilter()!;
  assert.deepEqual(get(state.groupRuleIds), owned.ids);
  assert.equal(get(state.groupRuleIds).includes(manualId), false, "deduplicated manual rules must stay editable");
  const tree = structuredClone(get(state.conditionTree));
  const payload = structuredClone(get(state.conditionPayload));
  state.makeGroupRulesEditable();
  assert.deepEqual(get(state.groupRuleIds), []);
  assert.equal(get(state.groupSelection), null);
  assert.equal(state.snapshotGroupFilter(), null);
  assert.deepEqual(get(state.conditionTree), tree, "Make rule keeps IDs, nanosecond ranges and null predicates intact");
  assert.deepEqual(get(state.conditionPayload), payload);
  state.clearGroupFilter();
  assert.deepEqual(get(state.conditionTree), tree, "Ungroup cannot remove converted ordinary rules");

  state.filterByGroupPath(["message"], [node("new group")]);
  state.clearGroupFilter();
  assert.deepEqual(get(state.conditionTree), tree, "later group navigation still preserves the converted path");
});

test("editing, deleting or changing the root operator retires managed-rule presentation without deleting other Rules", () => {
  for (const change of ["edit-owned", "remove-owned", "edit-manual", "remove-manual", "root-or"] as const) {
    const state = createConditionState();
    state.addFilterCondition("event", "eq", "1");
    const manualId = get(state.conditionTree).children[0].id;
    state.filterByGroupPath(["event", "host"], [node("1"), node("alpha")]);
    const ownedId = state.snapshotGroupFilter()!.ids[0];
    if (change === "edit-owned") state.patchCondition(ownedId, { value: "edited host" });
    else if (change === "remove-owned") state.removeNode(ownedId);
    else if (change === "edit-manual") state.patchCondition(manualId, { value: "2" });
    else if (change === "remove-manual") state.removeNode(manualId);
    else state.setGroupKind(get(state.conditionTree).id, "or");
    const edited = structuredClone(get(state.conditionTree));
    assert.deepEqual(get(state.groupRuleIds), [], change);
    assert.equal(get(state.groupSelection), null, change);
    state.clearGroupFilter();
    assert.deepEqual(get(state.conditionTree), edited, `${change}: retiring management cannot remove remaining Rules`);
  }
});

test("editing or removing a deduplicated manual predicate retires the full group path without deleting Rules", () => {
  for (const change of ["edit", "remove"] as const) {
    const state = createConditionState();
    state.addFilterCondition("event", "eq", "1");
    const manualId = get(state.conditionTree).children[0].id;
    state.filterByGroupPath(["event", "host"], [node("1"), node("alpha")]);
    const snapshot = state.snapshotGroupFilter()!;
    assert.equal(snapshot.ids.length, 1, "the matching manual event predicate is not owned by grouping");
    assert.equal(get(state.groupSelection)?.path[0].label, "1");
    if (change === "edit") state.patchCondition(manualId, { value: "2" });
    else state.removeNode(manualId);
    const manual = structuredClone(get(state.conditionTree));
    assert.equal(get(state.groupSelection), null, change);
    assert.equal(state.snapshotGroupFilter(), null, change);
    state.clearGroupFilter();
    assert.deepEqual(get(state.conditionTree), manual, "retiring ownership cannot delete manual or formerly managed Rules");

    const restored = createConditionState();
    restored.conditionTree.set(structuredClone(manual));
    restored.restoreGroupFilter(snapshot);
    assert.equal(get(restored.groupSelection), null, "matching owned IDs cannot restore a stale full path");
    restored.clearGroupFilter();
    assert.deepEqual(get(restored.conditionTree), manual);
  }
});

test("selected group metadata keeps exact context without retaining result branches", () => {
  const state = createConditionState();
  const range = { start: "2026-11-19T00:00:00.000000001Z", end: "2026-11-20T00:00:00.000000001Z" };
  const date = { ...node("2026-11-19", [node("unused result")]), range };
  const path = [date, node("", [node("another unused result")], null)];
  state.filterByGroupPath(["created", "host"], path);
  const selected = get(state.groupSelection)!;
  assert.deepEqual(selected.columns, ["created", "host"]);
  assert.deepEqual(selected.path.map(part => part.children), [[], []]);
  assert.deepEqual(selected.path[0].range, range);
  assert.notEqual(selected.path[0].range, range);
  assert.equal(groupDisplayValue(selected.path[1]), "(missing / null)");
  state.patchCondition(get(state.conditionTree).children[0].id, { value: "manual edit" });
  assert.equal(get(state.groupSelection), null, "manual edits retire the managed breadcrumb");
});

test("a tab can restore verified group ownership without changing or reclaiming edited Rules", () => {
  const original = { kind: "or" as const, children: [
    { column: "gate", op: "eq" as const, values: ["keep"], negated: false },
    { column: "gate", op: "eq" as const, values: ["other"], negated: false },
  ] };
  const source = createConditionState();
  source.conditionTree.set(presetCondition(original)!);
  source.filterByGroupPath(["host", "user"], [node("alpha"), node("alice")]);
  const savedTree = structuredClone(get(source.conditionTree));
  const savedFilter = source.snapshotGroupFilter()!;
  savedFilter.selection.path[0].label = "mutated snapshot";
  assert.equal(get(source.groupSelection)?.path[0].label, "alpha");
  const verified = source.snapshotGroupFilter()!;
  const restored = createConditionState();
  restored.conditionTree.set(structuredClone(savedTree));
  const before = get(restored.conditionTree);
  restored.restoreGroupFilter(verified);
  assert.equal(get(restored.conditionTree), before, "restoring ownership never mutates investigation Rules");
  assert.deepEqual(get(restored.groupSelection), get(source.groupSelection));
  assert.deepEqual(get(restored.groupRuleIds), verified.ids);
  restored.filterByGroupPath(["host"], [node("alpha")]);
  assert.deepEqual(get(restored.conditionPayload), { kind: "and", children: [original,
    { column: "host", op: "eq", values: ["alpha"], negated: false },
  ] });
  restored.clearGroupFilter();
  assert.deepEqual(get(restored.conditionPayload), original);

  const changed = createConditionState();
  changed.conditionTree.set(structuredClone(savedTree));
  changed.patchCondition(verified.ids[0], { value: "edited after capture" });
  const edited = structuredClone(get(changed.conditionTree));
  changed.restoreGroupFilter(verified);
  assert.equal(get(changed.groupSelection), null);
  assert.deepEqual(get(changed.groupRuleIds), [], "unverified snapshots cannot make manual Rules read-only");
  changed.clearGroupFilter();
  assert.deepEqual(get(changed.conditionTree), edited, "mismatched snapshots cannot claim manual Rules");
});

test("Ungroup removes one level and owned filter at a time while local Back keeps the hierarchy", () => runSvelteTest(String.raw`
const {get,derived}=await import('svelte/store');
const {createConditionState,presetCondition}=await import('./src/lib/stores/condition.ts');
const conditions=createConditionState();
const original={kind:'or',children:[{column:'gate',op:'eq',values:['keep'],negated:false},{column:'gate',op:'eq',values:['other'],negated:false}]};
conditions.conditionTree.set(presetCondition(original));
const columns=['host','user','event'];
const path=['alpha','alice','9007199254740993'].map(label=>({label,value:label,count:1,children:[]}));
const flush=async()=>{for(let i=0;i<8;i++){await Promise.resolve();await tick();}};
globalThis.__annotations=writable({});
globalThis.__workspace={...conditions,backend:writable({groupTree:async request=>({columns:request.groupColumns,nodes:[],total:1})}),
 projectDetail:writable({project:{meta:{id:'case'}},columns,column_kinds:{host:'string',user:'string',event:'int'}}),
 columnLabels:writable(new Map()),appliedFilters:derived(conditions.conditionPayload,condition=>({search:'',searchColumns:[],flag:'all',bookmarkedOnly:false,condition})),
 createReadScope:()=>({run:(_key,_api,work)=>work('request'),cancel(){},cancelAll(){},dispose(){}})};
const Panel=await compileHeadless('GroupingPanel');
const panel=await mountHeadless(Panel,{groupColumns:[...columns]});
const state=()=>panel.$capture_state();
const expected=length=>length?{kind:'and',children:[original,...columns.slice(0,length).map((column,index)=>({column,op:'eq',values:[path[index].label],negated:false}))]}:original;
try{
 await flush();assert.equal(state().sortBy,'count');assert.equal(state().sortDirection,'desc');
 state().applyPath(path);await flush();
 for(const length of [2,1,0]){
  state().ungroup();await flush();
  assert.deepEqual(state().groupColumns,columns.slice(0,length));
  assert.deepEqual(get(conditions.conditionPayload),expected(length));
  assert.equal(get(conditions.groupSelection)?.path.length??0,length);
 }
 assert.equal(state().tree,null);
 panel.$set({groupColumns:[...columns]});await flush();state().applyPath(path);await flush();
 for(const length of [2,1,0]){
  state().selectPath(length);await flush();
  assert.deepEqual(state().groupColumns,columns,'local Back must retain configured group levels');
  assert.deepEqual(get(conditions.conditionPayload),expected(length));
 }
 state().applyPath(path);await flush();
 panel.$destroy();
 const restored=await mountHeadless(Panel,{groupColumns:[...columns]});await flush();
 assert.equal(get(conditions.groupSelection).path.length,3,'closing the drawer must not discard the managed path');
 restored.$capture_state().selectPath(1);await flush();
 assert.deepEqual(get(conditions.conditionPayload),expected(1));
 restored.$destroy();
}finally{conditions.clearGroupFilter();}
`));

test("history restores exact group Rules on drawer reopen until a later explicit hierarchy change", () => runSvelteTest(String.raw`
const {get,derived}=await import('svelte/store');
const {createConditionState,presetCondition}=await import('./src/lib/stores/condition.ts');
const {createViewHistory}=await import('./src/lib/stores/viewHistory.ts');
const conditions=createConditionState();
const original={kind:'or',children:[{column:'gate',op:'eq',values:['keep'],negated:false},{column:'gate',op:'eq',values:['other'],negated:false}]};
conditions.conditionTree.set(presetCondition(original));
conditions.addFilterCondition('tenant','eq','manual');
const manual=structuredClone(get(conditions.conditionPayload));
const columns=['host','user','event'];
const path=['alpha','alice','9007199254740993'].map(label=>({label,value:label,count:1,children:[]}));
const history=createViewHistory();history.setHistoryProject('case');history.resetHistory(get(conditions.conditionTree),0);
const record=()=>history.recordAppliedCondition(0,get(conditions.conditionTree),0,conditions.snapshotGroupFilter());
const flush=async()=>{for(let i=0;i<8;i++){await Promise.resolve();await tick();}};
globalThis.__annotations=writable({});
globalThis.__workspace={...conditions,backend:writable({groupTree:async request=>({columns:request.groupColumns,nodes:[],total:1})}),
 projectDetail:writable({project:{meta:{id:'case'}},columns:[...columns,'gate','tenant'],column_kinds:{host:'string',user:'string',event:'int'}}),
 columnLabels:writable(new Map()),appliedFilters:derived(conditions.conditionPayload,condition=>({search:'',searchColumns:[],flag:'all',bookmarkedOnly:false,condition})),
 createReadScope:()=>({run:(_key,_api,work)=>work('request'),cancel(){},cancelAll(){},dispose(){}})};
const Panel=await compileHeadless('GroupingPanel');let panel=await mountHeadless(Panel,{groupColumns:[...columns]});
try{
 await flush();panel.$capture_state().applyPath(path);await flush();record();
 const selected=structuredClone(get(conditions.conditionPayload));
 panel.$capture_state().ungroup();await flush();record();
 assert.deepEqual(panel.$capture_state().groupColumns,columns.slice(0,2));
 history.goBack();const restored=get(history.historyRestore);
 conditions.conditionTree.set(structuredClone(restored.tree));conditions.restoreGroupFilter(restored.groupFilter);await flush();
 assert.deepEqual(get(conditions.conditionPayload),selected);
 panel.$destroy();panel=await mountHeadless(Panel,{groupColumns:columns.slice(0,2)});await flush();
 assert.deepEqual(get(conditions.conditionPayload),selected,'mounting with shorter configured levels must preserve exact historical Rules');
 assert.equal(get(conditions.groupSelection).path.length,3);
 panel.$capture_state().ungroup();await flush();
 assert.deepEqual(panel.$capture_state().groupColumns,['host']);
 assert.equal(get(conditions.groupSelection).path.length,1);
 conditions.clearGroupFilter();await flush();
 assert.deepEqual(get(conditions.conditionPayload),manual,'only an explicit hierarchy change removes owned suffixes; manual Rules survive');
}finally{panel.$destroy();}
`));

test("group selection, Ungroup and hierarchy edits synchronize through the actual panel", () => runSvelteTest(String.raw`
const {get,derived}=await import('svelte/store');
const {createConditionState,presetCondition}=await import('./src/lib/stores/condition.ts');
const conditions=createConditionState();
const original={column:'gate',op:'eq',values:['keep'],negated:false};
conditions.conditionTree.set(presetCondition(original));
const path=[{label:'alpha',value:'alpha',count:1,children:[]}];
const requests=[];
const flush=async()=>{for(let i=0;i<8;i++){await Promise.resolve();await tick();}};
globalThis.__annotations=writable({});
globalThis.__workspace={...conditions,backend:writable({groupTree:async request=>{requests.push(request);return {columns:request.groupColumns,nodes:path,total:1};}}),
 projectDetail:writable({project:{meta:{id:'case'}},columns:['host','time'],column_kinds:{host:'string',time:'datetime'}}),
 columnLabels:writable(new Map()),appliedFilters:derived(conditions.conditionPayload,condition=>({search:'',searchColumns:[],flag:'all',bookmarkedOnly:false,condition})),
 goBack(){},canGoBack:writable(false),
 createReadScope:()=>({run:(_key,_api,work)=>work('request'),cancel(){},cancelAll(){},dispose(){}})};
const Panel=await compileHeadless('GroupingPanel');
const panel=await mountHeadless(Panel,{groupColumns:['host']});
const state=()=>panel.$capture_state();
try{
 await flush();state().applyPath(path);await flush();
 assert.equal(requests.length,2);
 for(let i=0;i<5;i++){state().applyPath(path);await flush();}
 assert.equal(requests.length,2,'Repeated clicks must not reload the same group results');
 assert.equal(get(conditions.conditionTree).children.length,2);
 state().ungroup();await flush();
 assert.deepEqual(get(conditions.conditionPayload),original);
 assert.deepEqual(state().groupColumns,[]);assert.equal(state().tree,null);
 panel.$set({groupColumns:['host']});await flush();state().applyPath(path);await flush();
 panel.$set({groupColumns:['time'],granularities:{time:'day'}});await flush();
 assert.deepEqual(get(conditions.conditionPayload),original,'Changed group levels retire the former path');
 assert.deepEqual(state().groupColumns,['time']);
}finally{panel.$destroy();}
`));
