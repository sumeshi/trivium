import assert from "node:assert/strict";
import { test } from "node:test";
import { createCellIocEntry, iocBulkArgs, iocEntryError, iocQueryError, isIocFlag, normalizeIocFlag, prepareIocEntries, requireIocFlag, requireIocQuery } from "./iocRules.ts";
import { applyBulkAnnotation } from "./bulkAnnotation.ts";
import type { BulkAnnotateArgs } from "./backend";
import type { BulkCount } from "./types";
import type { IocEntry } from "./types";

const rule: IocEntry = { flag: "critical", tag: " C2 ", query: " host:evil | beacon " };

test("cell IOC rules preserve literal evidence and use a column-specific equality condition", () => {
  for (const value of ['', '   ', ' "powershell -enc" | user:alice ', 'C:\\Temp\\file\\', 'one\r\ntwo', '{"command":"a|b"}', '日本語']) {
    const entry = createCellIocEntry(' source:name " | ', value);
    assert.equal(entry.flag, 'critical');
    assert.equal(entry.query, '');
    const args = iocBulkArgs('case', entry);
    assert.equal(args.search, '');
    assert.deepEqual(args.condition, { column: ' source:name " | ', op: 'eq', values: [value], negated: false });
    assert.equal(args.flagFilter, 'all');
    assert.equal(args.bookmarkedOnly, false);
    assert.equal(iocEntryError(entry, [' source:name " | ']), null);
    assert.match(iocEntryError(entry, ['other'])!, /unavailable/);
  }
});

test("cell rules cannot lose a column, combine match modes, or silently truncate large values", () => {
  for (const column of ['', ' \t']) assert.throws(() => createCellIocEntry(column, 'value'), /column/);
  assert.throws(() => iocBulkArgs('case', { ...createCellIocEntry('c1', 'value'), query: 'another' }), /not both/);
  const boundary = '🙂'.repeat(16384);
  assert.equal(createCellIocEntry('c1', boundary).cell?.value, boundary);
  assert.throws(() => createCellIocEntry('c1', boundary + 'a'), /64 KiB/);
  const entry = { ...createCellIocEntry('c1', ' exact '), id: 'ui-only', tag: ' review ' };
  assert.deepEqual(prepareIocEntries([entry, { flag: 'critical', tag: '', query: '  ' }]), [
    { flag: 'critical', tag: 'review', query: '', cell: { column: 'c1', value: ' exact ' } },
  ]);
});

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

test("IOC queries need an effective term and complete OR operands before Save or Apply", () => {
  for (const query of ['|', '||', '""', '| || ""', 'host:', '-host:', 'host:""', '|powershell', 'powershell|']) {
    assert.ok(iocQueryError(query), query);
    assert.throws(() => requireIocQuery(query, "IOC rule 2"), /IOC rule 2:/, query);
    assert.throws(() => iocBulkArgs("case", { ...rule, query }), /search|query|column/i, query);
  }
});

test("IOC input feedback preserves existing quoted literals, OR shorthand and normal words", () => {
  for (const query of ['powershell', 'host:ws01|ws02', 'one|||two', 'one | -two', 'command:"powershell -enc"', '"|"', '"||"', '" "', 'host:" "', '"-"', '"host:"', '-', 'OR', 'one "" two', 'host:"""value', '"unterminated phrase']) {
    assert.equal(iocQueryError(query), null, query);
    assert.equal(requireIocQuery(` ${query} `), query);
  }
});
