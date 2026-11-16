import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { ensureFormatsLoaded, formats, formatCatalogError, saveCustomFormat, deleteCustomFormat } from "./patternCatalog.ts";
import type { Backend } from "./backend.ts";
import type { FormatCatalogBackend } from "./patternCatalog.ts";
import type { FormatCatalogEntry, FormatDefinition } from "./formats.ts";

const definition: FormatDefinition = { schemaVersion: 1, id: "ip", name: "IPv4", pattern: "[0-9.]+" };
const entry: FormatCatalogEntry = { definition, builtin: false };
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
}
function backend(overrides: Partial<FormatCatalogBackend> = {}) {
  return { listFormats: async () => [entry], validateFormat: async () => {},
    saveFormat: async (format: FormatDefinition) => ({ definition: format, builtin: false }),
    deleteFormat: async () => {}, ...overrides } as unknown as Backend;
}

test("catalog loading is shared and late backend responses cannot replace the current catalog", async () => {
  const first = deferred<FormatCatalogEntry[]>();
  let calls = 0;
  const a = backend({ listFormats: () => { calls++; return first.promise; } });
  const loading = ensureFormatsLoaded(a);
  assert.equal(ensureFormatsLoaded(a), loading);
  assert.equal(calls, 1);
  const b = backend({ listFormats: async () => [{ ...entry, definition: { ...definition, id: "b", name: "New backend" } }] });
  await ensureFormatsLoaded(b);
  first.resolve([entry]); await loading;
  assert.equal(get(formats)[0].definition.name, "New backend");
});

test("catalog mutations serialize without changing frozen definitions already applied to a case", async () => {
  const first = deferred<FormatCatalogEntry>();
  const order: string[] = [];
  const service = backend({ saveFormat: async format => {
    order.push(`save:${format.name}`);
    return format.name === "first" ? first.promise : { definition: format, builtin: false };
  }, deleteFormat: async id => { order.push(`delete:${id}`); } });
  await ensureFormatsLoaded(service);
  const frozen = structuredClone(get(formats)[0].definition);
  const saving = saveCustomFormat(service, { ...definition, name: "first" });
  const deleting = deleteCustomFormat(service, definition.id);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(order, ["save:first"]);
  first.resolve({ ...entry, definition: { ...definition, name: "first" } });
  await Promise.all([saving, deleting]);
  assert.deepEqual(order, ["save:first", "delete:ip"]);
  assert.deepEqual(get(formats), []);
  assert.equal(frozen.name, "IPv4");
});

test("a failed reload reports its reason and retains the last valid definitions", async () => {
  let fail = false;
  const service = backend({ listFormats: async () => { if (fail) throw new Error("Invalid classifier CIDR"); return [entry]; } });
  await ensureFormatsLoaded(service);
  fail = true;
  await assert.rejects(ensureFormatsLoaded(service, true), /Invalid classifier CIDR/);
  assert.equal(get(formats)[0].definition.id, definition.id);
  assert.equal(get(formatCatalogError), "Invalid classifier CIDR");
});

test("a reload started during a save cannot replace its committed catalog entry", async () => {
  const saved = deferred<FormatCatalogEntry>(), reloaded = deferred<FormatCatalogEntry[]>();
  let loads = 0;
  const service = backend({ listFormats: () => ++loads === 1 ? Promise.resolve([entry]) : reloaded.promise,
    saveFormat: () => saved.promise });
  await ensureFormatsLoaded(service);
  const saving = saveCustomFormat(service, { ...definition, name: "committed" });
  await new Promise(resolve => setImmediate(resolve));
  const reload = ensureFormatsLoaded(service, true);
  saved.resolve({ ...entry, definition: { ...definition, name: "committed" } });
  await saving;
  reloaded.resolve([entry]); await reload;
  assert.equal(get(formats)[0].definition.name, "committed");
});
