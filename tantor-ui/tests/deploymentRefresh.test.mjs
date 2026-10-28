import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';

const source = readFileSync(new URL('../src/pages/DeploymentLogs.tsx', import.meta.url), 'utf8');
const ast = ts.createSourceFile('DeploymentLogs.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);

// Execute the actual page callbacks with controlled fetches, state and timers.
function callback(name) {
  let result;
  function visit(node) {
    if (ts.isVariableDeclaration(node) && node.name.getText(ast) === name) {
      result = node.initializer.arguments[0].getText(ast);
    }
    ts.forEachChild(node, visit);
  }
  visit(ast);
  assert.ok(result, `Missing page callback: ${name}`);
  return ts.transpileModule(`globalThis.${name} = (${result});`, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  }).outputText;
}

const originalTask = { id: 'task-1', command: 'INSTALL_KAFKA', status: 'SUCCESS', logOutput: '[2026-10-23T06:00:00Z] Done' };
const flush = async () => { for (let i = 0; i < 20; i += 1) await Promise.resolve(); };

function harness(t, taskResponse, clusterResponse = { ok: true, json: async () => ({ status: 'SUCCESS' }) }) {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const state = { tasks: [originalTask], selectedTaskId: 'task-1', refreshing: false, loading: false, notice: null };
  const calls = [];
  const context = vm.createContext({
    id: 'cluster-1',
    window: { setTimeout, clearTimeout },
    console: { error() {} },
    noticeTimerRef: { current: null },
    refreshInFlightRef: { current: false },
    refreshGenerationRef: { current: 0 },
    fetch: async (url, options) => {
      calls.push({ url, options });
      return url.endsWith('/tasks') ? await taskResponse() : clusterResponse;
    },
    ...Object.fromEntries(['tasks', 'selectedTaskId', 'refreshing', 'loading', 'notice', 'cluster'].map(key => [
      `set${key[0].toUpperCase()}${key.slice(1)}`,
      value => { state[key] = typeof value === 'function' ? value(state[key]) : value; },
    ])),
  });
  for (const name of ['deploymentCommands', 'isDeploymentTask']) {
    const statement = ast.statements.find(node => ts.isVariableStatement(node)
      && node.declarationList.declarations.some(decl => decl.name.getText(ast) === name));
    assert.ok(statement);
    vm.runInContext(ts.transpileModule(statement.getText(ast), {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
    }).outputText, context);
  }
  vm.runInContext(callback('showNotice'), context);
  vm.runInContext(callback('fetchTasks'), context);
  return { state, calls, context, refresh: () => context.fetchTasks(true) };
}

test('refresh issues uncached reads, updates tasks and visibly acknowledges even unchanged data', async t => {
  const h = harness(t, async () => ({ ok: true, json: async () => [originalTask] }));
  const pending = h.refresh();
  assert.equal(h.state.refreshing, true);
  await h.refresh();
  await flush();
  assert.deepEqual(h.calls.map(call => call.url), ['/api/v1/ui/clusters/cluster-1', '/api/v1/ui/clusters/cluster-1/tasks']);
  assert.ok(h.calls.every(call => call.options.cache === 'no-store'));
  t.mock.timers.tick(499);
  await flush();
  assert.equal(h.state.refreshing, true);
  assert.equal(h.state.notice, null);
  t.mock.timers.tick(1);
  await pending;
  assert.equal(h.state.refreshing, false);
  assert.equal(h.state.notice.message, 'Logs refreshed');
  assert.equal(h.state.notice.error, false);
  assert.equal(h.state.tasks[0].logOutput, originalTask.logOutput);
  t.mock.timers.tick(2500);
  assert.equal(h.state.notice, null);

  const nextTask = { ...originalTask, logOutput: 'New backend output' };
  h.context.fetch = async url => ({ ok: true, json: async () => url.endsWith('/tasks') ? [nextTask] : { status: 'SUCCESS' } });
  const next = h.refresh();
  await flush();
  t.mock.timers.tick(500);
  await next;
  assert.equal(h.state.tasks[0].logOutput, 'New backend output');
});

test('a slow refresh remains busy until the response finishes', async t => {
  let resolveResponse;
  const h = harness(t, () => new Promise(resolve => { resolveResponse = resolve; }));
  const pending = h.refresh();
  t.mock.timers.tick(1000);
  await flush();
  assert.equal(h.state.refreshing, true);
  assert.equal(h.state.notice, null);
  resolveResponse({ ok: true, json: async () => [originalTask] });
  await pending;
  assert.equal(h.state.refreshing, false);
  assert.equal(h.state.notice.message, 'Logs refreshed');
});

test('HTTP, network, parsing and malformed-response failures preserve logs and report failure', async t => {
  const cases = [
    [async () => ({ ok: false })],
    [async () => { throw new Error('Offline'); }],
    [async () => ({ ok: true, json: async () => { throw new Error('Invalid JSON'); } })],
    [async () => ({ ok: true, json: async () => ({ unexpected: true }) })],
    [async () => ({ ok: true, json: async () => [originalTask] }), { ok: false }],
  ];
  for (const [taskResponse, clusterResponse] of cases) {
    const h = harness(t, taskResponse, clusterResponse);
    const pending = h.refresh();
    await flush();
    t.mock.timers.tick(500);
    await pending;
    assert.equal(h.state.tasks[0], originalTask);
    assert.equal(h.state.selectedTaskId, 'task-1');
    assert.equal(h.state.refreshing, false);
    assert.equal(h.state.notice.message, "Couldn't refresh logs. Try again.");
    assert.equal(h.state.notice.error, true);
    t.mock.timers.reset();
  }
});

test('background polling stays silent and unmount prevents delayed refresh feedback', async t => {
  const h = harness(t, async () => ({ ok: true, json: async () => [originalTask] }));
  await h.context.fetchTasks();
  assert.equal(h.state.notice, null);
  assert.equal(h.state.refreshing, false);
  const pending = h.refresh();
  await flush();
  h.context.refreshGenerationRef.current += 1;
  t.mock.timers.tick(500);
  await pending;
  assert.equal(h.state.notice, null);
});

test('refresh button and feedback remain accessible; copy uses the same toast', () => {
  assert.match(source, /onClick=\{\(\) => fetchTasks\(true\)\}/);
  assert.match(source, /disabled=\{loading \|\| refreshing\}/);
  assert.match(source, /aria-busy=\{refreshing\}/);
  assert.match(source, /className=\{loading \|\| refreshing \? 'spin' : ''\}/);
  assert.match(source, /showNotice\('Copied to clipboard'\)/);
  assert.match(source, /role=\{notice.error \? 'alert' : 'status'\}/);
});


test('deployment history excludes config reads and maintenance from count, selection and output', async t => {
  const commands = ['READ_KAFKA_PROPERTIES', 'READ_KAFKA_PROPERTY', 'PATCH_KAFKA_CONFIG',
    'BACKUP_KAFKA_CONFIG', 'RESTART_SERVICE', 'DELETE_CLUSTER', 'UNKNOWN_COMMAND'];
  const unrelated = commands.map((command, i) => ({ ...originalTask, id: `other-${i}`, command, logOutput: 'config-read output' }));
  const deployments = ['INSTALL_KAFKA', 'CHECK_KRAFT_CONNECTIVITY', 'VERIFY_KRAFT_QUORUM',
    'VERIFY_ZK_QUORUM', 'PRECHECK_SCHEMA', 'INSTALL_SCHEMA', 'VERIFY_SCHEMA_REGISTRY',
    'PRECHECK_CONNECT', 'INSTALL_CONNECT', 'VERIFY_CONNECT', 'INSTALL_MONITORING', 'UPGRADE_KAFKA']
    .map((command, i) => ({ ...originalTask, id: `deploy-${i}`, command }));
  const h = harness(t, async () => ({ ok: true, json: async () => [...unrelated, ...deployments] }));
  h.state.selectedTaskId = 'other-0';
  const pending = h.refresh();
  await flush(); t.mock.timers.tick(500); await pending;
  assert.deepEqual(Array.from(h.state.tasks, task => task.command), deployments.map(task => task.command));
  assert.equal(h.state.selectedTaskId, 'deploy-0');
  assert.ok(h.state.tasks.every(task => task.logOutput === originalTask.logOutput));
});

test('a cluster with only configuration reads has no deployment log to select', async t => {
  const h = harness(t, async () => ({ ok: true, json: async () => [
    { ...originalTask, command: 'READ_KAFKA_PROPERTIES' },
  ] }));
  const pending = h.refresh();
  await flush(); t.mock.timers.tick(500); await pending;
  assert.equal(h.state.tasks.length, 0);
  assert.equal(h.state.selectedTaskId, '');
});
