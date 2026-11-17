import assert from 'node:assert/strict';
import { test } from 'node:test';
import { writeClipboard } from './clipboard.ts';

async function environment(
  write: ((text: string) => Promise<void>) | undefined,
  fallback: boolean | Error,
  run: (state: { copied: string[]; removed: number; restored: number }) => Promise<void>,
) {
  const previous = new Map(['navigator', 'document', 'HTMLElement'].map(key =>
    [key, Object.getOwnPropertyDescriptor(globalThis, key)]));
  const state = { copied: [] as string[], removed: 0, restored: 0 };
  class Element {
    isConnected = true;
    focus() { state.restored++; }
  }
  const textarea = { value: '', style: {}, readOnly: false, select() {}, remove() { state.removed++; } };
  Object.defineProperty(globalThis, 'HTMLElement', { configurable: true, value: Element });
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: { clipboard: write ? { writeText: write } : undefined } });
  Object.defineProperty(globalThis, 'document', { configurable: true, value: {
    activeElement: new Element(), createElement: () => textarea, body: { append() {} },
    execCommand: (command: string) => {
      assert.equal(command, 'copy');
      assert.equal(textarea.readOnly, true);
      if (fallback instanceof Error) throw fallback;
      if (fallback) state.copied.push(textarea.value);
      return fallback;
    },
  } });
  try { await run(state); }
  finally {
    for (const [key, descriptor] of previous) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor);
      else Reflect.deleteProperty(globalThis, key);
    }
  }
}

test('Clipboard API copies exact source text without invoking the fallback', async () => {
  const copied: string[] = [];
  await environment(async text => { copied.push(text); }, false, async state => {
    await writeClipboard('<CommandLine>\r\n日本語\t</CommandLine>');
    assert.deepEqual(copied, ['<CommandLine>\r\n日本語\t</CommandLine>']);
    assert.equal(state.removed, 0);
  });
});

test('a denied or absent Clipboard API falls back and restores focus', async () => {
  for (const write of [undefined, async () => { throw new Error('Permission denied'); }]) {
    await environment(write, true, async state => {
      await writeClipboard('exact\nvalue');
      await writeClipboard('');
      assert.deepEqual(state.copied, ['exact\nvalue', '']);
      assert.equal(state.removed, 2);
      assert.equal(state.restored, 2);
    });
  }
});

test('failed fallback rejects instead of reporting success and always removes the temporary field', async () => {
  for (const fallback of [false, new Error('Copy failed')]) {
    await environment(undefined, fallback, async state => {
      await assert.rejects(writeClipboard('not copied'), /denied|Copy failed/);
      assert.deepEqual(state.copied, []);
      assert.equal(state.removed, 1);
      assert.equal(state.restored, 1);
    });
  }
});
