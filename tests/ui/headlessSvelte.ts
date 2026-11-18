/** Real Svelte actions/lifecycle in an isolated browser-conditioned Node process. */
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs/promises';
import { dirname, extname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = fileURLToPath(new URL('../../', import.meta.url));
// Compiler/runtime internals have no public TS contract; keep that boundary local.
const runtimeModule = (name: string) => import(name);

export async function compileHeadless(name: string) {
  const [{ compile, preprocess }, { default: sveltePreprocess }] = await Promise.all([
    runtimeModule('svelte/compiler'), runtimeModule('svelte-preprocess'),
  ]);
  const filename = resolve(root, 'src/lib/components/project_view', `${name}.svelte`);
  let source = await fs.readFile(filename, 'utf8');
  source = source.replace(/import \{ useWorkspace \} from [^;]+;/, 'const useWorkspace=()=>globalThis.__workspace;')
    .replace(/import \{ annotationVersions \} from [^;]+;/, 'const annotationVersions=globalThis.__annotations;')
    .replace(/import (\w+) from ['"][^'"]+\.svelte['"];?/g, 'const $1=class {};');
  const processed = await preprocess(source, sveltePreprocess({ typescript: true }), { filename });
  let code: string = compile(processed.code, { filename, generate: 'dom', dev: true, css: 'external' }).js.code;
  // Rewrite import specifiers only; source text and regex literals stay intact.
  code = code.replace(/((?:^|\n)import(?:\s+[^;]+?\s+from)?\s*)(['"])([^'"]+)\2/g, (_all, prefix, _quote, specifier: string) => {
    const url = specifier.startsWith('.')
      ? pathToFileURL(resolve(dirname(filename), specifier + (extname(specifier) ? '' : '.ts'))).href
      : import.meta.resolve(specifier);
    return prefix + JSON.stringify(url);
  });
  // Keep the real reactive instance and destruction hooks, without DOM paint.
  assert.match(code, /instance,\s*create_fragment,/);
  code = code.replace(/instance,\s*create_fragment,/g, 'instance, null,');
  return (await import('data:text/javascript;base64,' + Buffer.from(code).toString('base64'))).default;
}

export async function mountHeadless(Component: any, props: Record<string, unknown> = {}) {
  const { set_current_component } = await runtimeModule('svelte/internal');
  set_current_component({ $$: { root: {}, context: new Map() } } as any);
  try { return new Component({ $$inline: true, props }); }
  finally { set_current_component(null); }
}

export function runSvelteTest(scenario: string, timeout = 10_000) {
  const script = `
    import assert from 'node:assert/strict';
    import { writable } from 'svelte/store';
    import { tick } from 'svelte';
    import { compileHeadless, mountHeadless } from ${JSON.stringify(import.meta.url)};
    globalThis.document={dispatchEvent:()=>true};
    ${scenario}
    console.log('SVELTE_TEST_COMPLETE');
  `;
  const result = spawnSync(process.execPath, ['--conditions=browser', '--experimental-strip-types', '--input-type=module'], {
    cwd: root, input: script, encoding: 'utf8', timeout, maxBuffer: 1_000_000,
  });
  const error = result.stderr?.replace(/data:text\/javascript;base64,[A-Za-z0-9+/=]+/g, '<compiled Svelte component>');
  assert.equal(result.status, 0, result.error?.message ?? error);
  assert.equal(result.stdout.trim(), 'SVELTE_TEST_COMPLETE', 'Every lifecycle assertion must finish');
}
