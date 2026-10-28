import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';

const read = path => readFileSync(new URL(`../src/${path}`, import.meta.url), 'utf8');
function initializer(path, name) {
  const ast = ts.createSourceFile(path, read(path), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  let result;
  function visit(node) {
    if (ts.isVariableDeclaration(node) && node.name.getText(ast) === name) result = node.initializer;
    ts.forEachChild(node, visit);
  }
  visit(ast);
  assert.ok(result, `${path}: ${name}`);
  return { ast, node: result };
}

test('all five target tabs render the same Live control and use separate per-cluster settings', () => {
  for (const [path, label, scope] of [
    ['pages/Consumers.tsx', 'Consumers', 'consumers'], ['pages/Partitions.tsx', 'Partitions', 'partitions'],
    ['pages/SchemaRegistry.tsx', 'Schema Registry', 'schema-registry'], ['pages/KafkaConnect.tsx', 'Kafka Connect', 'kafka-connect'],
    ['components/SecurityManager.tsx', 'ACLs', 'acls'],
  ]) {
    const source = read(path);
    assert.ok(source.includes(`<LiveRefreshControl {...liveRefresh} label="${label}"`));
    const hook = initializer(path, 'liveRefresh').node.getText();
    assert.ok(hook.includes(`:${scope}`));
    assert.ok(!hook.includes('fetchWithDiscovery'));
    assert.ok(!hook.includes('createAcl'));
    assert.ok(!hook.includes('deleteAcl'));
  }
});

for (const [path, service] of [['pages/SchemaRegistry.tsx', 'schema-registry'], ['pages/KafkaConnect.tsx', 'kafka-connect']]) {
  test(`${service} automatic loader only reads the selected connection and ignores cancelled responses`, async () => {
    const { node } = initializer(path, 'load');
    const script = ts.transpileModule(`globalThis.load = (${node.getText()});`, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText;
    const calls = [], summaries = [], loading = [];
    const response = { subjects: [], connectors: [] };
    const context = vm.createContext({ id: 'cluster-test', selectedConnectionId: 'instance-b', showDeletedSubjects: true,
      loadRequestId: { current: 0 }, setHasFetched() {}, setLoading: value => loading.push(value), setError() {},
      setSummary: value => summaries.push(value), loadGlobalCompatibility: async () => 'BACKWARD', writeDataServiceSession() {},
      errorMessage: error => error.message,
      withConnId: (url, id) => `${url}?connectionId=${id}`,
      fetch: async (url, init) => { calls.push({ url, init }); return { ok: true, json: async () => response }; },
    });
    vm.runInContext(script, context);
    const controller = new AbortController();
    if (service === 'schema-registry') await context.load('instance-b', undefined, true, true, controller.signal);
    else await context.load('instance-b', undefined, true, controller.signal);
    assert.equal(calls.length, 1);
    assert.ok(calls[0].url.includes(`/${service}/summary?connectionId=instance-b`));
    if (service === 'schema-registry') assert.ok(calls[0].url.includes('includeDeleted=true'));
    assert.equal(calls[0].init.signal, controller.signal);
    assert.equal(calls[0].init.method, undefined);
    assert.deepEqual(loading, []);
    assert.equal(summaries.length, 1);
    controller.abort();
    if (service === 'schema-registry') await context.load('instance-b', undefined, true, true, controller.signal);
    else await context.load('instance-b', undefined, true, controller.signal);
    assert.equal(summaries.length, 1);
  });
}

test('ACL auto refresh uses the existing authenticated GET helper, not any mutation endpoint', () => {
  const api = read('lib/api.ts');
  assert.match(api, /getAcls = \(clusterId: string, params\?: AclQueryParams, signal\?: AbortSignal\)/);
  const get = initializer('lib/api.ts', 'getAcls').node.getText();
  assert.match(get, /fetchWithAuth/);
  assert.match(get, /\{ signal \}/);
  assert.doesNotMatch(get, /POST|PUT|DELETE/);
});

for (const [path, name, endpoint] of [
  ['pages/Consumers.tsx', 'fetchGroups', '/consumer-groups?page=2&size=10&search=orders&sortBy=totalLag'],
  ['pages/Partitions.tsx', 'fetchPartitions', '/partitions?page=2&size=10&search=orders&sortBy=totalLag'],
  ['components/SecurityManager.tsx', 'fetchAcls', '/security/acls'],
]) {
  test(`${name} refreshes quietly, preserves existing data on failure and ignores cancelled results`, async () => {
    const declaration = initializer(path, name).node;
    const script = ts.transpileModule(`globalThis.load = (${declaration.arguments[0].getText()});`, {
      compilerOptions: { target: ts.ScriptTarget.ES2022 },
    }).outputText;
    const requests = [], updates = [], loading = [], errors = [];
    let fail = false;
    const result = { content: [{ id: 'fresh' }], acls: [{ principal: 'User:fresh' }] };
    const context = vm.createContext({ id: 'cluster-test', clusterId: 'cluster-test',
      page: 2, size: 10, searchQuery: 'orders', sortBy: 'totalLag',
      activeRequest: { current: null }, AbortController, Error,
      setLoading: value => loading.push(value), setAclsLoading: value => loading.push(value),
      setError: value => errors.push(value), setAclsError: value => errors.push(value),
      setData: value => updates.push(value), setAcls: value => updates.push(value), console: { error() {} },
      fetch: async (url, init) => {
        requests.push({ url, signal: init.signal });
        if (fail) throw new Error('Network unavailable');
        return { ok: true, json: async () => result };
      },
      getAcls: async (id, params, signal) => {
        requests.push({ url: `/api/v1/clusters/${id}/security/acls`, signal });
        if (fail) throw new Error('Network unavailable');
        return result;
      },
    });
    vm.runInContext(script, context);
    await context.load(true, new AbortController().signal);
    assert.ok(requests[0].url.endsWith(endpoint));
    assert.ok(requests[0].signal instanceof AbortSignal);
    assert.ok(!loading.includes(true));
    assert.equal(updates.length, 1);
    const controller = new AbortController(); controller.abort();
    await context.load(true, controller.signal);
    assert.equal(updates.length, 1);
    fail = true;
    await context.load(true, new AbortController().signal);
    assert.equal(updates.length, 1);
    assert.ok(errors.includes('Network unavailable'));
  });
}
