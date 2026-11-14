import assert from "node:assert/strict";
import { test } from "node:test";
import { iocBulkArgs, iocRuleKey } from "./iocRules.ts";
import type { IocEntry } from "./types";

const rule: IocEntry = { flag: "critical", tag: " C2 ", query: " host:evil | beacon " };

test("IOC preview uses all data and the normal search language, with a non-destructive annotation patch", () => {
  const args = iocBulkArgs("case", rule);
  assert.equal(args.target, "filtered"); assert.equal(args.search, "host:evil | beacon");
  assert.deepEqual(args.searchColumns, []); assert.equal(args.condition, null);
  assert.equal(args.flagFilter, "all"); assert.equal(args.bookmarkedOnly, false);
  assert.equal(args.flag, "critical"); assert.deepEqual(args.add_tags, ["C2"]);
  for (const field of ["memo", "tags", "bookmarked", "remove_tags", "rowIndices", "previewToken"]) {
    assert.equal(Object.hasOwn(args, field), false, `existing ${field} must not be overwritten`);
  }
});

test("IOC apply carries the exact preview token and requires a new preview after any semantic rule edit", () => {
  const key = iocRuleKey("case", rule);
  assert.equal(iocRuleKey("case", { ...rule, query: rule.query.trim(), tag: "C2" }), key);
  for (const changed of [{ ...rule, flag: "safe" as const }, { ...rule, query: "different" }, { ...rule, tag: "different" }]) {
    assert.notEqual(iocRuleKey("case", changed), key);
  }
  assert.notEqual(iocRuleKey("other case", rule), key);
  assert.equal(iocBulkArgs("case", rule, "preview-token").previewToken, "preview-token");
});

test("IOC rules cannot annotate everything with an empty query or silently choose a flag", () => {
  assert.throws(() => iocBulkArgs("case", { ...rule, query: "  " }), /query/);
  assert.throws(() => iocBulkArgs("case", { ...rule, flag: "" as IocEntry["flag"] }), /flag/);
  assert.equal(Object.hasOwn(iocBulkArgs("case", { ...rule, tag: " " }), "add_tags"), false);
});
