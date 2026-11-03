import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import vm from 'node:vm';
import React from 'react';
import ts from 'typescript';

const require = createRequire(import.meta.url);
const source = readFileSync(new URL('../src/pages/JobStatusPage.tsx', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX,
} }).outputText;

function page(reason, status = 'ROLLBACK_FAILED') {
  const job = { id: 'old-job', type: 'TARGETED_CONFIG_CHANGE', status, rollbackSupported: true,
    configRecoveryBlockedReason: reason, logs: '', progress: '{}', retryCount: 1 };
  const states = [job, [], {}, false, false, false, false];
  const effects = [], requests = [], updates = [];
  let index = 0, tick;
  const react = { ...React, useState: () => {
    const slot = index++;
    return [states[slot], value => updates.push([slot, value])];
  }, useMemo: fn => fn(), useCallback: fn => fn, useRef: () => ({ current: null }),
  useEffect: fn => effects.push(fn) };
  const context = vm.createContext({ exports: {}, console,
    setInterval: fn => { tick = fn; return 1; }, clearInterval() {},
    fetch: async url => { requests.push(url); return { ok: true, json: async () => url.endsWith('/steps') || url.endsWith('/hosts') ? [] : job }; },
    require: name => {
      if (name === 'react') return react;
      if (name === 'react-router-dom') return { useParams: () => ({ id: job.id }), useNavigate: () => () => {} };
      if (name.includes('usePermissions')) return { usePermissions: () => ({ canManage: true }) };
      if (name.includes('confirmUtils')) return { confirmAction: async () => true, notifyAction() {} };
      if (name.includes('istLogTime')) return { formatIstDateTime: () => '', formatLogTimestampsIst: value => value };
      if (name.endsWith('.css')) return {};
      return require(name);
    },
  });
  vm.runInContext(compiled, context);
  return { tree: context.exports.JobStatusPage(), effects, requests, updates, tick: () => tick() };
}
function nodes(tree, predicate) {
  if (!tree || typeof tree !== 'object') return [];
  return [...(predicate(tree) ? [tree] : []), ...React.Children.toArray(tree.props?.children).flatMap(child => nodes(child, predicate))];
}

test('stale rollback is disabled, explains newer change and guards click handler', async () => {
  const reason = 'A newer configuration job new-job has applied changes on this cluster.';
  const view = page(reason);
  const button = nodes(view.tree, node => node.type === 'button' && node.props.onClick?.name === 'handleRetry')[0];
  assert.equal(button.props.disabled, true);
  assert.equal(button.props.title, reason);
  assert.equal(nodes(view.tree, node => node.props?.role === 'alert').length, 1);
  await button.props.onClick();
  assert.equal(view.requests.length, 0);
});

test('current failed rollback remains retryable and stale initial rollback is disabled', () => {
  const current = page(null);
  assert.equal(nodes(current.tree, node => node.type === 'button' && node.props.onClick?.name === 'handleRetry')[0].props.disabled, false);
  const stale = page('Newer configuration exists', 'FAILED');
  const actions = nodes(stale.tree, node => node.type === 'button' && ['handleRetry', 'handleRollback'].includes(node.props.onClick?.name));
  assert.equal(actions.length, 2);
  assert.ok(actions.every(button => button.props.disabled));
});

test('failed config job keeps polling so a newer successful change disables an already open page', async () => {
  const view = page(null);
  view.effects.forEach(effect => effect());
  await new Promise(resolve => setImmediate(resolve));
  view.requests.length = 0;
  view.tick();
  await new Promise(resolve => setImmediate(resolve));
  assert.ok(view.requests.includes('/api/v1/ui/jobs/old-job'));
});
