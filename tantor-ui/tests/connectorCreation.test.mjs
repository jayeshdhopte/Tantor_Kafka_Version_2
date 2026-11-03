import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import { acceptedConnector, mergeCreatedConnectors, refreshCreatedConnectors, ConnectorSummaryReadError } from '../src/utils/connectorCreation.ts';

const snapshot = (connectors = []) => ({ connection: 'http://worker:8083', version: '4.3.0', plugins: [],
  connectors, connectorCount: connectors.length, taskCount: 0, runningTasks: 0,
  runningConnectors: 0, pausedConnectors: 0, failedConnectors: 0 });
const body = { name: 'new-sink', config: { 'connector.class': 'FileStreamSinkConnector' } };
const pending = acceptedConnector(body, { name: body.name, type: 'sink' });
const running = { ...pending, initializing: false, state: 'RUNNING', tasks: 1, runningTasks: 1,
  status: { connector: { state: 'RUNNING' }, tasks: [{ id: 0, state: 'RUNNING' }] } };

test('accepted connector is visible immediately without fabricated runtime state or duplicated counts', () => {
  const visible = mergeCreatedConnectors(snapshot(), [pending]);
  assert.equal(visible.connectorCount, 1);
  assert.equal(visible.runningConnectors, 0);
  assert.equal(visible.connectors[0].state, 'INITIALIZING');
  assert.equal(mergeCreatedConnectors(visible, [pending]).connectorCount, 1);
  const ready = mergeCreatedConnectors(snapshot([running]), [pending]);
  assert.equal(ready.connectorCount, 1);
  assert.equal(ready.runningTasks, 1);
  assert.equal(ready.connectors[0].state, 'RUNNING');
});

test('read-after-create retries 404, missing inventory and unassigned tasks until real runtime state arrives', async () => {
  let reads = 0;
  const updates = [];
  const ready = await refreshCreatedConnectors({ created: [pending], signal: new AbortController().signal,
    wait: async () => {}, publish: value => updates.push(value),
    read: async () => {
      reads++;
      if (reads === 1) throw new ConnectorSummaryReadError('Not yet published', 404);
      if (reads === 2) return snapshot();
      if (reads === 3) return snapshot([{ ...running, tasks: 0, runningTasks: 0 }]);
      return snapshot([running]);
    } });
  assert.equal(ready, true);
  assert.equal(reads, 4);
  assert.equal(updates[0].connectorCount, 1);
  assert.equal(updates.at(-1).runningTasks, 1);
});

test('reconciliation is bounded and genuine upstream errors are not hidden', async () => {
  let reads = 0;
  assert.equal(await refreshCreatedConnectors({ created: [pending], signal: new AbortController().signal,
    attempts: 3, wait: async () => {}, publish() {}, read: async () => { reads++; return snapshot(); } }), false);
  assert.equal(reads, 3);
  for (const status of [401, 403, 500]) {
    reads = 0;
    await assert.rejects(refreshCreatedConnectors({ created: [pending], signal: new AbortController().signal,
      wait: async () => {}, publish() {}, read: async () => { reads++; throw new ConnectorSummaryReadError('Real failure', status); } }));
    assert.equal(reads, 1);
  }
});

test('switching instance or leaving the page cancels old responses before they update the UI', async () => {
  const controller = new AbortController();
  let finish, published = 0;
  const result = refreshCreatedConnectors({ created: [pending], signal: controller.signal,
    publish: () => published++, read: () => new Promise(resolve => { finish = resolve; }) });
  controller.abort();
  finish(snapshot([running]));
  await assert.rejects(result, { name: 'AbortError' });
  assert.equal(published, 0);
});

function creationHandler() {
  const text = readFileSync(new URL('../src/pages/KafkaConnect.tsx', import.meta.url), 'utf8');
  const ast = ts.createSourceFile('KafkaConnect.tsx', text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  let handler;
  function visit(node) {
    if (ts.isVariableDeclaration(node) && node.name.getText(ast) === 'createConnector') handler = node.initializer;
    ts.forEachChild(node, visit);
  }
  visit(ast);
  return ts.transpileModule('globalThis.create = (' + handler.getText(ast) + ');', {
    compilerOptions: { target: ts.ScriptTarget.ES2022 },
  }).outputText;
}

for (const type of ['source', 'sink']) {
  test(type + ' creation stays successful across transient 404 and refreshes automatically with Live paused', async () => {
    const calls = [], errors = [], messages = [], cache = [];
    let current = snapshot(), activeTab, showCreate = true, reads = 0;
    const context = vm.createContext({ AbortController, Error, canManage: true, saving: false,
      creationRequest: { current: null }, loadRequestId: { current: 0 }, id: 'cluster', selectedConnectionId: 'instance-a',
      summary: current, connectorTemplate: '{}', connectorPayloads: () => [body],
      acceptedConnector, mergeCreatedConnectors, ConnectorSummaryReadError,
      refreshCreatedConnectors: options => refreshCreatedConnectors({ ...options, wait: async () => {} }),
      withConnId: path => path + '?connectionId=instance-a', errorMessage: e => e.message,
      setLoading() {}, setSaving() {}, setError: value => { if (value) errors.push(value); },
      setCreateError: value => { if (value) errors.push(value); }, setHasFetched() {}, setConnectorJson() {},
      setSummary: value => { current = typeof value === 'function' ? value(current) : value; },
      setActiveTab: value => { activeTab = value; }, setShowCreate: value => { showCreate = value; },
      setSuccessMessage: value => messages.push(value), writeDataServiceSession: (...args) => cache.push(args),
      fetch: async (url, init) => {
        calls.push({ url, method: init.method || 'GET' });
        if (init.method === 'POST') return { ok: true, json: async () => ({ name: body.name, type }) };
        reads++;
        return reads === 1 ? { ok: false, status: 404, json: async () => ({ message: 'The native REST API returned HTTP 404.' }) }
          : { ok: true, json: async () => snapshot([running]) };
      },
    });
    vm.runInContext(creationHandler(), context);
    await context.create({ preventDefault() {} });
    assert.equal(calls.filter(call => call.method === 'POST').length, 1);
    assert.equal(calls.filter(call => call.method === 'GET').length, 2);
    assert.ok(calls.every(call => call.url.endsWith('?connectionId=instance-a')));
    assert.equal(showCreate, false);
    assert.equal(activeTab, 'connectors');
    assert.equal(current.runningTasks, 1);
    assert.equal(current.connectors[0].state, 'RUNNING');
    assert.deepEqual(errors, []);
    assert.ok(messages.at(-1).includes('Current runtime status loaded'));
    assert.equal(cache.at(-1)[2].summary.runningTasks, 1);
    // A 404 returned by CREATE itself is a real failure, never a propagation retry.
    calls.length = 0; errors.length = 0; showCreate = true;
    context.fetch = async (url, init) => {
      calls.push({ url, method: init.method || 'GET' });
      return { ok: false, status: 404, json: async () => ({ message: 'Create endpoint rejected the request.' }) };
    };
    await context.create({ preventDefault() {} });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].method, 'POST');
    assert.equal(showCreate, true);
    assert.ok(errors[0].includes('Create endpoint rejected'));
  });
}
