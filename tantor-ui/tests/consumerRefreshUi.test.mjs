import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';
import postcss from 'postcss';

const source = readFileSync(new URL('../src/pages/Consumers.tsx', import.meta.url), 'utf8');
const ast = ts.createSourceFile('Consumers.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
let handler;
function visit(node) {
  if (ts.isVariableDeclaration(node) && node.name.getText(ast) === 'handleManualRefresh') handler = node.initializer.getText(ast);
  ts.forEachChild(node, visit);
}
visit(ast);
assert.ok(handler);
const compiled = ts.transpileModule(`globalThis.refresh = (${handler});`, {
  compilerOptions: { target: ts.ScriptTarget.ES2022 },
}).outputText;
const flush = async () => { for (let i = 0; i < 10; i += 1) await Promise.resolve(); };

function harness(t, load = async () => {}) {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const state = { busy: false, calls: 0, updates: 0 };
  const context = vm.createContext({
    loading: false, window: { setTimeout }, manualRefreshInFlight: { current: false }, manualRefreshSequence: { current: 0 },
    setManualRefreshing: value => { state.busy = value; state.updates += 1; },
    fetchGroups: async () => { state.calls += 1; await load(); },
  });
  vm.runInContext(compiled, context);
  return { state, context, refresh: () => context.refresh() };
}

test('fast consumer refresh stays visibly busy for 500ms and ignores repeated clicks', async t => {
  const h = harness(t);
  const pending = h.refresh();
  assert.equal(h.state.busy, true);
  await h.refresh();
  assert.equal(h.state.calls, 1);
  await flush();
  t.mock.timers.tick(499);
  await flush();
  assert.equal(h.state.busy, true);
  t.mock.timers.tick(1);
  await pending;
  assert.equal(h.state.busy, false);
  assert.equal(h.context.manualRefreshInFlight.current, false);
});

test('slow consumer refresh keeps spinning until the data request completes', async t => {
  let finish;
  const h = harness(t, () => new Promise(resolve => { finish = resolve; }));
  const pending = h.refresh();
  t.mock.timers.tick(1000);
  await flush();
  assert.equal(h.state.busy, true);
  finish();
  await pending;
  assert.equal(h.state.busy, false);
});

test('refresh clears its busy state after failure without updating after unmount', async t => {
  const h = harness(t, async () => { throw new Error('Network failed'); });
  const pending = h.refresh();
  await flush();
  const rejected = assert.rejects(pending, /Network failed/);
  t.mock.timers.tick(500);
  await rejected;
  assert.equal(h.state.busy, false);
  h.context.fetchGroups = async () => {};
  const next = h.refresh();
  await flush();
  const updates = h.state.updates;
  h.context.manualRefreshSequence.current += 1;
  t.mock.timers.tick(500);
  await next;
  assert.equal(h.state.updates, updates);
});

test('only the inner focus ring is removed and the button owns its animation', () => {
  const css = postcss.parse(readFileSync(new URL('../src/pages/Consumers.css', import.meta.url), 'utf8'));
  const declarations = selector => {
    const result = {};
    css.walkRules(rule => {
      if (rule.selector.split(',').map(value => value.trim()).includes(selector)) {
        rule.walkDecls(declaration => { result[declaration.prop] = declaration.value; });
      }
    });
    return result;
  };
  for (const selector of ['.consumer-refresh-rate select:focus', '.consumer-refresh-rate select:focus-visible']) {
    assert.equal(declarations(selector)['box-shadow'], 'none');
    assert.equal(declarations(selector).outline, 'none');
  }
  assert.match(declarations('.consumer-refresh-rate:focus-within').outline, /2px solid/);
  let animation;
  css.walkRules('.consumer-refresh-button .consumer-refresh-spin', rule => {
    if (rule.parent.type === 'root') animation = rule.nodes.find(node => node.prop === 'animation')?.value;
  });
  assert.equal(animation, 'consumer-refresh-spin 0.8s linear infinite');
  assert.equal(declarations('.consumer-refresh-button .consumer-refresh-spin').animation, 'none');
  assert.match(source, /onClick=\{handleManualRefresh\}/);
  assert.match(source, /aria-busy=\{loading \|\| manualRefreshing\}/);
  assert.match(source, /disabled=\{loading \|\| manualRefreshing\}/);
  assert.match(source, /\[2, 5, 10, 15\]\.map/);
  assert.match(source, /refreshInterval \* 1000/);
});
