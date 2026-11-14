import assert from "node:assert/strict";
import { test } from "node:test";
import { iocBulkArgs, isIocFlag, normalizeIocFlag, requireIocFlag } from "./iocRules.ts";
import { applyBulkAnnotation } from "./bulkAnnotation.ts";
import type { BulkAnnotateArgs } from "./backend";
import type { BulkCount } from "./types";
import type { IocEntry } from "./types";

const rule: IocEntry = { flag: "critical", tag: " C2 ", query: " host:evil | beacon " };

test("IOC Apply uses all data and the normal search language, with a non-destructive annotation patch", () => {
  const args = iocBulkArgs("case", rule);
  assert.equal(args.target, "filtered"); assert.equal(args.search, "host:evil | beacon");
  assert.deepEqual(args.searchColumns, []); assert.equal(args.condition, null);
  assert.equal(args.flagFilter, "all"); assert.equal(args.bookmarkedOnly, false);
  assert.equal(args.flag, "critical"); assert.deepEqual(args.add_tags, ["C2"]);
  for (const field of ["memo", "tags", "bookmarked", "remove_tags", "rowIndices", "previewToken"]) {
    assert.equal(Object.hasOwn(args, field), false, `existing ${field} must not be overwritten`);
  }
});

test("direct IOC Apply freezes the rule and commits once with its internally checked target", async () => {
  let release!: (value: BulkCount) => void;
  const waiting = new Promise<BulkCount>(resolve => { release = resolve; });
  const draft = { ...rule };
  let written: BulkAnnotateArgs | null = null;
  const operation = applyBulkAnnotation({
    bulkPreview: payload => { assert.equal(payload.projectId, "case"); assert.equal(payload.search, "host:evil | beacon"); return waiting; },
    bulkUpdate: async payload => { written = payload; return { updated: 2 }; },
  }, iocBulkArgs("case", draft), { isCurrent: () => true });
  draft.flag = "safe"; draft.query = "different"; draft.tag = "different";
  release({ count: 2, previewToken: "checked-rule" });
  assert.deepEqual(await operation, { updated: 2 });
  assert.ok(written);
  assert.equal((written as BulkAnnotateArgs).previewToken, "checked-rule");
  assert.equal((written as BulkAnnotateArgs).projectId, "case");
  assert.equal((written as BulkAnnotateArgs).search, "host:evil | beacon");
  assert.equal((written as BulkAnnotateArgs).flag, "critical");
  assert.deepEqual((written as BulkAnnotateArgs).add_tags, ["C2"]);
});

test("IOC rules cannot annotate everything with an empty query or silently choose a flag", () => {
  assert.throws(() => iocBulkArgs("case", { ...rule, query: "  " }), /query/);
  assert.throws(() => iocBulkArgs("case", { ...rule, flag: "" as IocEntry["flag"] }), /flag/);
  assert.equal(Object.hasOwn(iocBulkArgs("case", { ...rule, tag: " " }), "add_tags"), false);
});

test("loading IOC flags preserves invalid values and normalizes only known severities and aliases", () => {
  for (const [stored, expected] of [
    [" SAFE ", "safe"], [" Suspicious ", "suspicious"], ["CRITICAL", "critical"],
    ["◯", "safe"], ["?", "suspicious"], ["✗", "critical"],
    ["critcal", "critcal"], ["Mystery", "Mystery"], ["", ""], ["  ", ""],
  ]) {
    assert.equal(normalizeIocFlag(stored), expected);
  }
  const loaded = ["critical", "critcal", ""].map(flag => ({ ...rule, flag: normalizeIocFlag(flag) }));
  assert.deepEqual(loaded.map(entry => entry.flag), ["critical", "critcal", ""]);
  assert.deepEqual(loaded.map(entry => isIocFlag(entry.flag)), [true, false, false]);
  assert.equal(iocBulkArgs("case", loaded[0]).flag, "critical");
  for (const invalid of loaded.slice(1)) {
    assert.throws(() => iocBulkArgs("case", invalid), /invalid flag/);
  }
});

test("IOC save/apply validation reports the invalid value without defaulting to Safe", () => {
  assert.throws(() => requireIocFlag("critcal", "IOC rule 2"), /IOC rule 2: invalid flag "critcal"/);
  for (const flag of ["critcal", "suspiciouss", "none", "unknown", " "]) {
    assert.throws(() => iocBulkArgs("case", { ...rule, flag }), /invalid flag/);
  }
  assert.equal(iocBulkArgs("case", { ...rule, flag: "✗" }).flag, "critical");
});
