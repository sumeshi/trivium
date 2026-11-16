import assert from 'node:assert/strict';
import { test } from 'node:test';
import { copyFileSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { generateThemes } from './generate-themes.mjs';

test('adding one JSON registers its ID, label, order and stylesheet without a manual options list', () => {
  const root = mkdtempSync(path.join(tmpdir(), 'trivium-themes-'));
  try {
    const directory = path.join(root, 'themes'); mkdirSync(directory);
    const light = JSON.parse(readFileSync(new URL('../definitions/themes/light.json', import.meta.url), 'utf8'));
    copyFileSync(new URL('../definitions/themes/light.json', import.meta.url), path.join(directory, 'light.json'));
    const future = { ...light, id: 'future', name: 'Future', order: 99, tokens: { ...light.tokens, accent: '#234567' } };
    writeFileSync(path.join(directory, 'future.json'), JSON.stringify(future));
    const options = { directory, typescriptPath: path.join(root, 'catalog.ts'), cssPath: path.join(root, 'catalog.css') };
    assert.deepEqual(generateThemes(options).map(definition => definition.id), ['light', 'future']);
    assert.match(readFileSync(options.typescriptPath, 'utf8'), /"name": "Future"/);
    assert.match(readFileSync(options.cssPath, 'utf8'), /\[data-theme="future"\]/);
    assert.match(readFileSync(options.cssPath, 'utf8'), /--accent: #234567/);
    generateThemes({ ...options, check: true });
    writeFileSync(path.join(directory, 'future.json'), JSON.stringify({ ...future, name: 'Changed' }));
    assert.throws(() => generateThemes({ ...options, check: true }), /stale/);
    writeFileSync(path.join(directory, 'duplicate.json'), JSON.stringify(light));
    assert.throws(() => generateThemes(options), /Duplicate/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
