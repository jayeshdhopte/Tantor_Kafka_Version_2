import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';
import React from 'react';

const require = createRequire(import.meta.url);
const compile = path => ts.transpileModule(readFileSync(new URL(path, import.meta.url), 'utf8'), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
}).outputText;

function hookHarness(fetchImpl) {
  let cursor = 0;
  const states = [], effects = [], timers = new Set(), focus = new Set(), requests = [];
  const context = vm.createContext({ exports: {}, AbortController,
    fetch: async (url, options) => { requests.push({ url, options }); return fetchImpl(url, options); },
    window: { setInterval(fn, delay) { assert.equal(delay, 10000); timers.add(fn); return fn; },
      clearInterval(fn) { timers.delete(fn); }, addEventListener(event, fn) { assert.equal(event, 'focus'); focus.add(fn); },
      removeEventListener(event, fn) { focus.delete(fn); } },
    require: () => ({ useCallback: fn => fn, useEffect: effect => effects.push(effect),
      useState(initial) { const index = cursor++; if (!(index in states)) states[index] = initial;
        return [states[index], value => { states[index] = typeof value === 'function' ? value(states[index]) : value; }]; } }),
  });
  vm.runInContext(compile('../src/hooks/useConfigRollbackEligibility.ts'), context);
  const render = (enabled = true) => { cursor = 0; effects.length = 0;
    return context.exports.useConfigRollbackEligibility('old-job', enabled); };
  return { render, start: () => effects[0](), requests, timers, focus };
}
const settle = () => new Promise(resolve => setImmediate(resolve));

test('eligibility starts disabled, enables verified recovery, and notices newer changes on polling', async () => {
  let allowed = true;
  const h = hookHarness(async () => Response.json({ allowed, reason: allowed ? '' : 'A newer configuration change affects recovery' }));
  assert.equal(h.render().allowed, false);
  const cleanup = h.start(); await settle();
  assert.equal(h.render().allowed, true);
  allowed = false; [...h.timers][0](); await settle();
  assert.equal(h.render().allowed, false);
  assert.match(h.render().reason, /newer configuration/);
  assert.equal(h.requests[0].url, '/api/v1/ui/jobs/old-job/rollback-eligibility');
  allowed = true; [...h.focus][0](); await settle(); assert.equal(h.render().allowed, true);
  cleanup(); assert.equal(h.timers.size, 0); assert.equal(h.focus.size, 0);
  assert.equal(h.requests[0].options.signal.aborted, true);
});
test('errors and malformed eligibility fail closed', async () => {
  for (const response of [() => { throw Error('offline'); }, () => Response.json({ allowed: false }),
    () => Response.json({ allowed: 'true', reason: '' }), () => new Response('', { status: 500 })]) {
    const h = hookHarness(async () => response()); h.render(); const cleanup = h.start(); await settle();
    assert.equal(h.render().allowed, false); assert.match(h.render().reason, /Unable to verify/); cleanup();
  }
});
test('unrelated jobs do not request eligibility', () => {
  const h = hookHarness(() => { throw Error('must not fetch'); });
  assert.equal(h.render(false).allowed, true); h.start(); assert.equal(h.requests.length, 0);
});

const pageSource = readFileSync(new URL('../src/pages/JobStatusPage.tsx', import.meta.url), 'utf8');
const ast = ts.createSourceFile('page.tsx', pageSource, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
const stateNames = [];
function visit(node) {
  if (ts.isVariableDeclaration(node) && ts.isArrayBindingPattern(node.name)
      && node.initializer && ts.isCallExpression(node.initializer) && node.initializer.expression.getText(ast) === 'useState')
    stateNames.push(node.name.elements[0].name.getText(ast));
  ts.forEachChild(node, visit);
}
visit(ast);
const text = node => typeof node === 'string' || typeof node === 'number' ? String(node)
  : React.Children.toArray(node?.props?.children).map(text).join('');
function all(node, predicate) {
  if (!node || typeof node !== 'object') return [];
  return [...(predicate(node) ? [node] : []), ...React.Children.toArray(node.props?.children).flatMap(child => all(child, predicate))];
}
test('actual job page disables stale retry, explains why, and preserves valid retries', async () => {
  let allowed = false, cursor = 0;
  const states = [], requests = [];
  const job = { id: 'old-job', type: 'TARGETED_CONFIG_CHANGE', status: 'ROLLBACK_FAILED', payload: '{}',
    progress: '{}', logs: '', rollbackSupported: true, retryCount: 0 };
  const context = vm.createContext({ exports: {}, window: {}, document: {},
    setInterval, clearInterval, fetch: async (...args) => { requests.push(args); return Response.json(job); },
    require: name => {
      if (name === 'react') return { ...React, useEffect() {}, useCallback: fn => fn, useMemo: fn => fn(), useRef: value => ({ current: value }),
        useState(initial) { const index = cursor++; if (!(index in states)) states[index] = typeof initial === 'function' ? initial() : initial;
          return [states[index], value => { states[index] = typeof value === 'function' ? value(states[index]) : value; }]; } };
      if (name === 'react-router-dom') return { useParams: () => ({ id: 'old-job' }), useNavigate: () => () => {} };
      if (name === 'lucide-react') return new Proxy({}, { get: () => 'svg' });
      if (name.endsWith('usePermissions')) return { usePermissions: () => ({ canManage: true }) };
      if (name.endsWith('useConfigRollbackEligibility')) return { useConfigRollbackEligibility: () => ({ allowed, reason: 'Newer configuration blocks rollback', refresh() {} }) };
      if (name.endsWith('confirmUtils')) return { confirmAction: async () => true, notifyAction() {} };
      if (name.endsWith('istLogTime')) return { formatIstDateTime: () => '', formatLogTimestampsIst: value => value };
      if (name.endsWith('.css')) return {};
      return require(name);
    },
  });
  vm.runInContext(compile('../src/pages/JobStatusPage.tsx'), context);
  const render = () => { cursor = 0; return context.exports.JobStatusPage(); };
  render(); states[stateNames.indexOf('job')] = job; states[stateNames.indexOf('loading')] = false;
  let tree = render();
  let retry = all(tree, node => node.type === 'button' && text(node).trim() === 'Retry rollback')[0];
  assert.ok(retry); assert.equal(retry.props.disabled, true);
  assert.equal(all(tree, node => node.props?.id === 'config-rollback-reason').length, 1);
  await retry.props.onClick(); assert.equal(requests.length, 0);
  allowed = true; tree = render();
  retry = all(tree, node => node.type === 'button' && text(node).trim() === 'Retry rollback')[0];
  assert.equal(retry.props.disabled, false);
  assert.equal(all(tree, node => node.props?.id === 'config-rollback-reason').length, 0);
});
