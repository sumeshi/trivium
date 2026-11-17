import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { AddressInfo } from 'node:net';
import { createServer } from 'vite';

test('the development server never exposes the host editor launcher', async () => {
  const server = await createServer({
    configFile: 'vite.config.ts', logLevel: 'silent',
    server: { host: '127.0.0.1', port: 0, strictPort: true, watch: null },
  });
  try {
    await server.listen();
    const port = (server.httpServer!.address() as AddressInfo).port;
    const base = `http://127.0.0.1:${port}`;
    for (const path of ['/__open-in-editor', '/__open-in-editor/nested']) {
      for (const method of ['GET', 'POST']) {
        const response = await fetch(`${base}${path}?file=not-an-editor-target`, { method });
        assert.equal(response.status, 404);
        assert.equal(await response.text(), '');
      }
    }
    // Disabling the unused launcher must preserve the actual app and HMR.
    const response = await fetch(base);
    assert.equal(response.status, 200);
    assert.match(await response.text(), /\/src\/main\.ts/);
  } finally { await server.close(); }
});
