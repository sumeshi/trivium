import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import type { Backend, UpdateFlagArgs, UpdateFlagResult } from "./backend.ts";
import type { FlagEntry, LoadProjectResponse, ProjectRow } from "./types.ts";
import { queueAnnotationWrite } from "./annotationWrites.ts";
import { normalizeRow } from "./components/project_view/state.ts";
import { createWorkspaceState } from "./workspaceState.ts";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
async function settle() { for (let i = 0; i < 12; i++) await Promise.resolve(); }
const baseline: FlagEntry = { flag: "", memo: "Sender: <alice@example.org>", tags: ["review"], bookmarked: true };
function row(id = 2, entry = baseline): ProjectRow {
  return { row_index: id, cells: { c1: { kind: "string", text: String(id) } }, ...entry };
}
function setup(id: string, entries = new Map([[2, { ...baseline }]])) {
  const errors: string[] = [];
  const workspace = createWorkspaceState((message) => errors.push(message));
  const detail = { project: { meta: { id } } } as LoadProjectResponse;
  workspace.projectDetail.set(detail);
  workspace.currentProjectId.set(id);
  const gates: ReturnType<typeof deferred<void>>[] = [];
  let revision = 0;
  workspace.backend.set({ updateFlag: (args: UpdateFlagArgs) => {
    const gate = deferred();
    gates.push(gate);
    return queueAnnotationWrite(id, async () => {
      await gate.promise;
      const { projectId: _, rowIndex, ...patch } = args;
      const entry = { ...(entries.get(rowIndex) ?? baseline), ...patch };
      entries.set(rowIndex, entry);
      return { entry, revision: ++revision,
        flaggedRecords: [...entries.values()].filter((entry) => Boolean(entry.flag)).length };
    });
  } } as Backend);
  workspace.registerLoadedPage(0, 0, [...entries].map(([id, entry]) => normalizeRow(row(id, entry))));
  const initialCount = [...entries.values()].filter((entry) => Boolean(entry.flag)).length;
  workspace.applyAnnotationCounts(initialCount, initialCount);
  return { workspace, entries, errors, gates };
}
function assertAnnotation(state: ReturnType<typeof setup>, flag: string, count: number) {
  const cached = get(state.workspace.rowsCache).get(2)!;
  assert.equal(cached.flag, flag);
  assert.equal(cached.memo, baseline.memo);
  assert.deepEqual(cached.tags, baseline.tags);
  assert.equal(cached.bookmarked, baseline.bookmarked);
  assert.equal(get(state.workspace.flaggedCount), count);
  assert.equal(get(state.workspace.filteredFlaggedCount), count);
}

for (const firstSucceeds of [true, false]) {
  for (const lastSucceeds of [true, false]) {
    test(`rapid flag saves reconcile confirmed values: first=${firstSucceeds}, last=${lastSucceeds}`, async () => {
      const state = setup(`flag-${firstSucceeds}-${lastSucceeds}`);
      const clicked = get(state.workspace.rowsCache).get(2)!;
      const first = state.workspace.setFlag(clicked, "safe");
      const last = state.workspace.setFlag(clicked, "critical");
      assertAnnotation(state, "critical", 1);
      if (firstSucceeds) state.gates[0].resolve(); else state.gates[0].reject(new Error("disk full"));
      await first;
      assertAnnotation(state, "critical", 1);
      if (lastSucceeds) state.gates[1].resolve(); else state.gates[1].reject(new Error("disk full"));
      await last;
      const expected = lastSucceeds ? "critical" : firstSucceeds ? "safe" : "";
      assert.equal(state.entries.get(2)?.flag, expected);
      assertAnnotation(state, expected, expected ? 1 : 0);
      assert.equal(state.errors.length, Number(!firstSucceeds) + Number(!lastSucceeds));
      for (const message of state.errors) assert.match(message, /Failed to save flag/);
      state.workspace.dispose();
    });
  }
}

test("failed toggle-off restores a preceding successful flag, including counts", async () => {
  const state = setup("flag-toggle");
  const clicked = get(state.workspace.rowsCache).get(2)!;
  const first = state.workspace.setFlag(clicked, "safe");
  const last = state.workspace.setFlag(clicked, "safe");
  assertAnnotation(state, "", 0);
  state.gates[0].resolve(); await first;
  assertAnnotation(state, "", 0);
  state.gates[1].reject(new Error("read-only")); await last;
  assertAnnotation(state, "safe", 1);
  state.workspace.dispose();
});

for (const noteFirst of [true, false]) {
  for (const flagSucceeds of [true, false]) {
    for (const noteSucceeds of [true, false]) {
      test(`note/flag saves share confirmed fields: noteFirst=${noteFirst}, flag=${flagSucceeds}, note=${noteSucceeds}`, async () => {
        const state = setup(`note-${noteFirst}-${flagSucceeds}-${noteSucceeds}`);
        const clicked = get(state.workspace.rowsCache).get(2)!;
        const draft = { memo: "<CommandLine>whoami</CommandLine>", tags: ["changed"], bookmarked: false };
        const saveNote = () => state.workspace.saveAnnotation(get(state.workspace.currentProjectId)!, clicked, draft)
          .then(() => true, () => false);
        let note: Promise<boolean>, flag: Promise<void>;
        if (noteFirst) { note = saveNote(); flag = state.workspace.setFlag(clicked, "critical"); }
        else { flag = state.workspace.setFlag(clicked, "critical"); note = saveNote(); }
        assert.equal(get(state.workspace.rowsCache).get(2)?.memo, draft.memo);
        for (let index = 0; index < 2; index++) {
          const isNote = noteFirst ? index === 0 : index === 1;
          const success = isNote ? noteSucceeds : flagSucceeds;
          if (success) state.gates[index].resolve(); else state.gates[index].reject(new Error("save failed"));
          await settle();
        }
        await Promise.all([note!, flag!]);
        const cached = get(state.workspace.rowsCache).get(2)!;
        const persisted = state.entries.get(2)!;
        assert.equal(cached.flag, flagSucceeds ? "critical" : "");
        assert.equal(cached.memo, noteSucceeds ? draft.memo : baseline.memo);
        assert.deepEqual(cached.tags, noteSucceeds ? draft.tags : baseline.tags);
        assert.equal(cached.bookmarked, noteSucceeds ? draft.bookmarked : baseline.bookmarked);
        assert.equal(cached.memo, persisted.memo);
        assert.equal(cached.flag, persisted.flag);
        assert.equal(get(state.workspace.flaggedCount), flagSucceeds ? 1 : 0);
        assert.equal(get(state.workspace.filteredFlaggedCount), flagSucceeds ? 1 : 0);
        state.workspace.dispose();
      });
    }
  }
}

test("counts include other rows' pending flags while successes publish committed counts", async () => {
  const state = setup("flag-cross-row", new Map([[2, { ...baseline }], [3, { ...baseline }]]));
  const first = state.workspace.setFlag(get(state.workspace.rowsCache).get(2)!, "safe");
  const last = state.workspace.setFlag(get(state.workspace.rowsCache).get(3)!, "critical");
  assert.equal(get(state.workspace.flaggedCount), 2);
  state.gates[0].resolve(); await first;
  assert.equal(get(state.workspace.flaggedCount), 2);
  assert.equal(get(state.workspace.filteredFlaggedCount), 2);
  state.gates[1].reject(new Error("save failed")); await last;
  assert.equal(get(state.workspace.rowsCache).get(2)?.flag, "safe");
  assert.equal(get(state.workspace.rowsCache).get(3)?.flag, "");
  assert.equal(get(state.workspace.flaggedCount), 1);
  assert.equal(get(state.workspace.filteredFlaggedCount), 1);
  state.workspace.dispose();
});

test("page publication retains pending flags, counts, and the confirmed rollback baseline", async () => {
  const state = setup("flag-page-pending");
  const first = state.workspace.setFlag(get(state.workspace.rowsCache).get(2)!, "safe");
  const last = state.workspace.setFlag(get(state.workspace.rowsCache).get(2)!, "critical");
  state.workspace.applyAnnotationCounts(0, 0);
  state.workspace.registerLoadedPage(0, 0, [normalizeRow(row())]);
  assertAnnotation(state, "critical", 1);
  state.gates[0].reject(new Error("save failed")); await first;
  state.gates[1].reject(new Error("save failed")); await last;
  assertAnnotation(state, "", 0);
  state.workspace.dispose();
});

test("a failed save after a filter change adjusts project counts without subtracting from new results", async () => {
  const state = setup("flag-scope-change");
  const save = state.workspace.setFlag(get(state.workspace.rowsCache).get(2)!, "critical");
  state.workspace.appliedFilters.set({ ...get(state.workspace.appliedFilters), search: "new scope" });
  state.workspace.applyAnnotationCounts(0, 0);
  assert.equal(get(state.workspace.flaggedCount), 1);
  assert.equal(get(state.workspace.filteredFlaggedCount), 0);
  state.gates[0].reject(new Error("save failed")); await save;
  assert.equal(get(state.workspace.flaggedCount), 0);
  assert.equal(get(state.workspace.filteredFlaggedCount), 0);
  state.workspace.dispose();
});

for (const success of [true, false]) {
  for (const change of ["dispose", "project", "reload"] as const) {
    test(`a late ${success ? "success" : "failure"} cannot update ${change} state or notify another screen`, async () => {
      const state = setup(`flag-late-${change}-${success}`);
      const saving = state.workspace.setFlag(get(state.workspace.rowsCache).get(2)!, "critical");
      if (change === "dispose") state.workspace.dispose();
      else {
        state.workspace.invalidateViewRequests();
        if (change === "project") {
          state.workspace.projectDetail.set({ project: { meta: { id: "other" } } } as LoadProjectResponse);
          state.workspace.currentProjectId.set("other");
        }
        state.workspace.rowsCache.set(new Map([[2, normalizeRow(row(2, { ...baseline, flag: "suspicious" }))]]));
        state.workspace.flaggedCount.set(42);
      }
      if (success) state.gates[0].resolve(); else state.gates[0].reject(new Error("save failed"));
      await saving;
      if (change === "dispose") assert.equal(get(state.workspace.rowsCache).size, 0);
      else {
        assert.equal(get(state.workspace.rowsCache).get(2)?.flag, "suspicious");
        assert.equal(get(state.workspace.flaggedCount), 42);
      }
      assert.deepEqual(state.errors, []);
      state.workspace.dispose();
    });
  }
}

test("settled annotation baselines are released so a later external annotation becomes the next baseline", async () => {
  const state = setup("flag-baseline-release");
  const first = state.workspace.setFlag(get(state.workspace.rowsCache).get(2)!, "safe");
  state.gates[0].resolve(); await first;
  const external = { ...baseline, flag: "suspicious" };
  state.workspace.registerLoadedPage(0, 0, [normalizeRow(row(2, external))]);
  const last = state.workspace.setFlag(get(state.workspace.rowsCache).get(2)!, "critical");
  state.gates[1].reject(new Error("save failed")); await last;
  assertAnnotation(state, "suspicious", 1);
  state.workspace.dispose();
});

test("recreating the same filter object preserves pending filtered counts without a refresh", async () => {
  const state = setup("flag-same-scope");
  const save = state.workspace.setFlag(get(state.workspace.rowsCache).get(2)!, "critical");
  state.workspace.appliedFilters.set(structuredClone(get(state.workspace.appliedFilters)));
  state.workspace.applyAnnotationCounts(0, 0);
  assert.equal(get(state.workspace.filteredFlaggedCount), 1);
  state.gates[0].resolve(); await save;
  assertAnnotation(state, "critical", 1);
  assert.equal(get(state.workspace.annotationRefreshRequested), 0);
  state.workspace.dispose();
});

for (const succeeds of [true, false]) {
  test(`a changed result containing the pending row has correct filtered counts after ${succeeds ? "success" : "failure"}`, async () => {
    const state = setup(`flag-new-included-${succeeds}`);
    const save = state.workspace.setFlag(get(state.workspace.rowsCache).get(2)!, "critical");
    state.workspace.appliedFilters.set({ ...get(state.workspace.appliedFilters), search: "2" });
    state.workspace.rowsCache.set(new Map());
    state.workspace.positionToRowIndex.set(new Map());
    // Production publishes fresh rows before reconciling committed counts.
    state.workspace.registerLoadedPage(0, 0, [normalizeRow(row())]);
    state.workspace.applyAnnotationCounts(0, 0);
    assertAnnotation(state, "critical", 1);
    if (succeeds) state.gates[0].resolve(); else state.gates[0].reject(new Error("save failed"));
    await save;
    assertAnnotation(state, succeeds ? "critical" : "", succeeds ? 1 : 0);
    assert.equal(get(state.workspace.annotationRefreshRequested), 0);
    state.workspace.dispose();
  });
}

for (const externalFlag of ["critical", ""]) {
  test(`fresh page becomes the rollback baseline after an external ${externalFlag || "clear"} while a note is pending`, async () => {
    const state = setup(`flag-external-${externalFlag || "clear"}`);
    const clicked = get(state.workspace.rowsCache).get(2)!;
    const flag = state.workspace.setFlag(clicked, "safe");
    const note = state.workspace.saveAnnotation(get(state.workspace.currentProjectId)!, clicked, { memo: "draft" })
      .then(() => true, () => false);
    state.gates[0].resolve(); await flag;
    const external = { ...baseline, flag: externalFlag, memo: "external note", tags: ["bulk"], bookmarked: false };
    state.entries.set(2, external);
    state.workspace.registerLoadedPage(0, 0, [normalizeRow(row(2, external))]);
    state.workspace.applyAnnotationCounts(externalFlag ? 1 : 0, externalFlag ? 1 : 0);
    assert.equal(get(state.workspace.rowsCache).get(2)?.memo, "draft");
    assert.equal(get(state.workspace.rowsCache).get(2)?.flag, externalFlag);
    assert.deepEqual(get(state.workspace.rowsCache).get(2)?.tags, ["bulk"]);
    state.gates[1].reject(new Error("save failed")); await note;
    const cached = get(state.workspace.rowsCache).get(2)!;
    assert.equal(cached.flag, externalFlag);
    assert.equal(cached.memo, external.memo);
    assert.deepEqual(cached.tags, external.tags);
    assert.equal(cached.bookmarked, false);
    assert.equal(get(state.workspace.flaggedCount), externalFlag ? 1 : 0);
    assert.equal(get(state.workspace.filteredFlaggedCount), externalFlag ? 1 : 0);
    state.workspace.dispose();
  });
}

for (const externalFlag of ["critical", ""]) {
  test(`a delayed revision-1 save cannot replace a newer revision-2 ${externalFlag || "clear"} page baseline`, async () => {
    const state = setup(`flag-delayed-revision-${externalFlag || "clear"}`);
    const oldReply = deferred<UpdateFlagResult>(), noteReply = deferred<UpdateFlagResult>();
    let calls = 0;
    state.workspace.backend.set({ ...get(state.workspace.backend)!,
      updateFlag: () => (++calls === 1 ? oldReply.promise : noteReply.promise) });
    const clicked = get(state.workspace.rowsCache).get(2)!;
    const saving = state.workspace.setFlag(clicked, "safe");
    const note = state.workspace.saveAnnotation(get(state.workspace.currentProjectId)!, clicked, { memo: "draft" })
      .then(() => true, () => false);
    // The first save committed Safe at revision 1, but its IPC reply is held.
    // A separate process then changes the persisted annotation at revision 2.
    const external = { ...baseline, flag: externalFlag, memo: "external revision 2" };
    state.entries.set(2, external);
    state.workspace.registerLoadedPage(0, 0, [normalizeRow(row(2, external))], 2);
    state.workspace.applyAnnotationCounts(externalFlag ? 1 : 0, externalFlag ? 1 : 0, 2);
    oldReply.resolve({ entry: { ...baseline, flag: "safe" }, revision: 1, flaggedRecords: 1 });
    await saving;
    assert.equal(get(state.workspace.rowsCache).get(2)?.flag, externalFlag);
    assert.equal(get(state.workspace.rowsCache).get(2)?.memo, "draft");
    assert.equal(get(state.workspace.flaggedCount), externalFlag ? 1 : 0);
    assert.equal(get(state.workspace.filteredFlaggedCount), externalFlag ? 1 : 0);
    noteReply.reject(new Error("disk full")); await note;
    assert.equal(get(state.workspace.rowsCache).get(2)?.flag, externalFlag);
    assert.equal(get(state.workspace.rowsCache).get(2)?.memo, external.memo);
    assert.equal(get(state.workspace.flaggedCount), externalFlag ? 1 : 0);
    assert.equal(get(state.workspace.filteredFlaggedCount), externalFlag ? 1 : 0);
    assert.equal(get(state.workspace.annotationRefreshRequested), 1);
    state.workspace.dispose();
  });
}
