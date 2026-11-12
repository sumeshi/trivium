import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';

const version = JSON.parse(readFileSync(new URL('./package.json', import.meta.url), 'utf8')).version as string;
let commit = process.env.GITHUB_SHA ?? '';
if (!/^[a-f0-9]{7,40}$/i.test(commit)) {
  try { commit = execFileSync('git', ['rev-parse', '--short=7', 'HEAD'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim(); }
  catch { commit = ''; }
}


export default defineConfig({
  define: {
    __TRIVIUM_VERSION__: JSON.stringify(version),
    __TRIVIUM_COMMIT__: JSON.stringify(commit.slice(0, 7)),
  },
  plugins: [svelte()],
  server: {
    port: 5173,
    strictPort: true
  },
  preview: {
    port: 4173
  },
  build: {
    target: 'esnext'
  }
});
