/** Real Svelte actions, reactivity, and destruction without mounting browser DOM. */
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs/promises';
import { dirname, extname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = fileURLToPath(new URL('../../', import.meta.url));
// Compiler/runtime internals have no public TS contract; keep that boundary local.
const runtimeModule = (name: string) => import(name);

export async function compileHeadless(name: string) {
  const [{ compile, parse, preprocess }, { default: sveltePreprocess }] = await Promise.all([
    runtimeModule('svelte/compiler'), runtimeModule('svelte-preprocess'),
  ]);
  const filename = resolve(root, 'src/lib/components/project_view', `${name}.svelte`);
  let source = await fs.readFile(filename, 'utf8');
  source = source.replace(/import \{ useWorkspace \} from [^;]+;/, 'const useWorkspace=()=>globalThis.__workspace;')
    .replace(/import \{ annotationVersions \} from [^;]+;/, 'const annotationVersions=globalThis.__annotations;')
    .replace(/import (\w+) from ['"][^'"]+\.svelte['"];?/g, 'const $1=class {};');
  const processed = await preprocess(source, sveltePreprocess({ typescript: true }), { filename });
  const ast = parse(processed.code);
  const names = new Set<string>();
  const mutable = new Set<string>();
  const bindings = (node: any, target: Set<string>) => {
    if (!node) return;
    if (node.type === 'Identifier') target.add(node.name);
    else if (node.type === 'RestElement') bindings(node.argument, target);
    else if (node.type === 'AssignmentPattern') bindings(node.left, target);
    else if (node.type === 'ArrayPattern') node.elements.forEach((item: any) => bindings(item, target));
    else if (node.type === 'ObjectPattern') node.properties.forEach((item: any) => bindings(item.value ?? item.argument, target));
  };
  for (const statement of ast.instance.content.body) {
    const node = statement.type === 'ExportNamedDeclaration' ? statement.declaration : statement;
    if (node?.type === 'VariableDeclaration') {
      for (const declaration of node.declarations) {
        bindings(declaration.id, names);
        if (node.kind !== 'const') bindings(declaration.id, mutable);
      }
    } else if (node?.type === 'FunctionDeclaration' || node?.type === 'ClassDeclaration') names.add(node.id.name);
    else if (node?.type === 'ImportDeclaration') node.specifiers.forEach((item: any) => names.add(item.local.name));
    else if (node?.type === 'LabeledStatement' && node.label.name === '$'
      && node.body.type === 'ExpressionStatement' && node.body.expression.type === 'AssignmentExpression') {
      bindings(node.body.expression.left, names);
      bindings(node.body.expression.left, mutable);
    }
  }
  const stores = (node: any) => {
    if (!node || typeof node !== 'object') return;
    if (node.type === 'Identifier' && /^\$[^$]/.test(node.name)) names.add(node.name);
    for (const value of Object.values(node)) {
      if (Array.isArray(value)) value.forEach(stores);
      else if (value && typeof value === 'object') stores(value);
    }
  };
  stores(ast.instance.content);
  // Expose the same state and actions the Svelte 4 development compiler exposed.
  // Removing only the template keeps the real Svelte 5 reactive graph and lifecycle.
  let script = processed.code.slice(ast.instance.content.start, ast.instance.content.end);
  for (const statement of [...ast.instance.content.body].reverse()) {
    if (statement.type !== 'ImportDeclaration' || statement.source.value !== 'svelte') continue;
    const onMount = statement.specifiers.find((item: any) => item.imported?.name === 'onMount');
    if (!onMount) continue;
    const retained = statement.specifiers.filter((item: any) => item !== onMount)
      .map((item: any) => item.imported.name === item.local.name ? item.local.name : `${item.imported.name} as ${item.local.name}`);
    // The old no-target constructor never mounted the component. Keep browser
    // setup deferred while real onDestroy hooks and reactive work still execute.
    const replacement = `${retained.length ? `import { ${retained.join(',')} } from 'svelte';` : ''}
      const ${onMount.local.name} = () => {};`;
    const start = statement.start - ast.instance.content.start;
    const end = statement.end - ast.instance.content.start;
    script = script.slice(0, start) + replacement + script.slice(end);
  }
  const instrumented = `${ast.module ? processed.code.slice(ast.module.start, ast.module.end) : ''}<script>
    ${script}
    export function __capture() { return { ${[...names].join(',')} }; }
    export function __inject(values) { ${[...mutable].map(name => `if (Object.hasOwn(values, ${JSON.stringify(name)})) ${name} = values[${JSON.stringify(name)}];`).join('\n')} }
  </script>`;
  let code: string = compile(instrumented, { filename, generate: 'client', dev: true, accessors: true, css: 'external' }).js.code;
  // This entry point initializes browser node operations, unused without a template.
  code = code.replace(/^import ['"]svelte\/internal\/init-operations['"];\s*/m, '');
  // Rewrite import specifiers only; source text and regex literals stay intact.
  code = code.replace(/((?:^|\n)import(?:\s+[^;]+?\s+from)?\s*)(['"])([^'"]+)\2/g, (_all, prefix, _quote, specifier: string) => {
    const url = specifier.startsWith('.')
      ? pathToFileURL(resolve(dirname(filename), specifier + (extname(specifier) ? '' : '.ts'))).href
      : import.meta.resolve(specifier);
    return prefix + JSON.stringify(url);
  });
  return (await import('data:text/javascript;base64,' + Buffer.from(code).toString('base64'))).default;
}

export async function mountHeadless(Component: any, props: Record<string, unknown> = {}) {
  const [{ effect_root }, { flushSync }] = await Promise.all([
    runtimeModule('svelte/internal/client'), runtimeModule('svelte'),
  ]);
  const events: Record<string, Array<(event: unknown) => void>> = {};
  let instance: any;
  const destroy = effect_root(() => { instance = Component(null, { ...props, $$events: events }); });
  flushSync();
  return Object.assign(instance, {
    $capture_state: instance.__capture,
    $inject_state: instance.__inject,
    $set: instance.__inject,
    $destroy: destroy,
    $on(name: string, callback: (event: unknown) => void) {
      (events[name] ??= []).push(callback);
      return () => { events[name] = events[name].filter(listener => listener !== callback); };
    },
  });
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
