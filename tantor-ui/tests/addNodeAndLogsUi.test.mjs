import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import vm from 'node:vm';
import React from 'react';
import ts from 'typescript';

const require = createRequire(import.meta.url);
function load(path, mocks = {}, globals = {}) {
  const source = readFileSync(new URL('../src/' + path, import.meta.url), 'utf8');
  const compiled = ts.transpileModule(source, { compilerOptions: {
    module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX,
  } }).outputText;
  const context = vm.createContext({ exports: {}, require: name => mocks[name] ?? require(name), ...globals });
  vm.runInContext(compiled, context);
  return context.exports;
}
const { relativeLogRange } = load('utils/logTimeRange.ts');
const { reportedJavaRuntime } = load('utils/reportedJavaRuntime.ts');
const { uniqueJavaUpgradeHosts } = load('utils/javaUpgradeHosts.ts');

test('separate broker and controller assignments produce one selectable Java host', () => {
  const broker = { hostId: 'host-191', hostname: 'broker3', ipAddress: '192.168.3.191', role: 'broker' };
  const controller = { ...broker, role: 'controller' };
  const hosts = uniqueJavaUpgradeHosts([broker, controller]);
  assert.equal(hosts.length, 1);
  assert.equal(hosts[0], broker);
  const receipt = { hostId: 'host-191' };
  assert.equal(hosts[0].hostId, receipt.hostId);
});

test('Java host deduplication preserves different targets even when their IPs match', () => {
  const hosts = uniqueJavaUpgradeHosts([
    { hostId: 'external:broker', ipAddress: '192.168.3.191' },
    { hostId: 'external:controller', ipAddress: '192.168.3.191' },
    { hostId: 'external:broker', ipAddress: '192.168.3.191' },
  ]);
  assert.equal(hosts.length, 2);
  assert.equal(hosts[0].hostId, 'external:broker');
  assert.equal(hosts[1].hostId, 'external:controller');
  assert.equal(uniqueJavaUpgradeHosts([]).length, 0);
});

for (const mode of ['kraft', 'EXTERNAL']) {
  test(`Java Upgrade ${mode} page renders duplicate host once and submits its original ID once`, async () => {
    const hostId = mode === 'EXTERNAL' ? 'external:node-191' : 'host-191';
    const broker = { hostId, hostname: 'broker3', ipAddress: '192.168.3.191', role: 'broker' };
    const hosts = [broker, { ...broker, role: 'controller' }];
    const values = [];
    let cursor = 0;
    let effect;
    let submitted;
    let destination;
    const { ClusterJavaUpgrade } = load('pages/ClusterJavaUpgrade.tsx', {
      react: { ...React,
        useState: initial => {
          const index = cursor++;
          if (!(index in values)) values[index] = initial;
          return [values[index], next => { values[index] = typeof next === 'function' ? next(values[index]) : next; }];
        },
        useEffect: callback => { effect ??= callback; },
      },
      'react-router-dom': { useParams: () => ({ id: 'cluster-1' }), useNavigate: () => next => { destination = next; } },
      '../hooks/usePermissions': { usePermissions: () => ({ canManage: true }) },
      '../hooks/useActiveClusterJobs': { useActiveClusterJobs: () => [] },
      '../components/confirmUtils': { confirmAction: async () => true },
      '../components/ClusterLoadingState': { ClusterLoadingState: () => null },
      '../utils/reportedJavaRuntime': { reportedJavaRuntime },
      '../utils/javaUpgradeHosts': { uniqueJavaUpgradeHosts },
      './ClusterActions.css': {},
    }, {
      AbortController,
      fetch: async (url, options) => {
        if (options?.method === 'POST') {
          submitted = JSON.parse(options.body);
          return { ok: true, json: async () => ({ jobId: 'job-1' }) };
        }
        const data = url.includes('/external-java/hosts') ? hosts
          : url.endsWith('/parcels') ? [{ artifactId: 'jdk-21', hostId, serviceType: 'JAVA', status: 'DISTRIBUTED', version: '21.0.13' }]
          : url.endsWith('/hosts') ? [{ id: hostId, javaVersion: '17.0.21', javaVendor: 'OpenJDK' }]
          : { id: 'cluster-1', mode, status: 'SUCCESS', kafkaVersion: '4.1.0', hosts };
        return { ok: true, json: async () => data };
      },
    });
    const render = () => { cursor = 0; return expand(ClusterJavaUpgrade()); };
    render();
    effect();
    await new Promise(resolve => setImmediate(resolve));
    let nodes = render();
    const table = nodes.find(node => node.type === 'table');
    assert.equal(expand(table).filter(node => node.type === 'tbody').flatMap(node => expand(node.props.children)).filter(node => node.type === 'tr').length, 1);
    nodes.find(node => node.type === 'select').props.onChange({ target: { value: 'jdk-21' } });
    nodes = render();
    const boxes = nodes.filter(node => node.type === 'input' && node.props.type === 'checkbox');
    assert.equal(boxes.length, 1);
    boxes[0].props.onChange({ target: { checked: true } });
    boxes[0].props.onChange({ target: { checked: true } });
    nodes = render();
    await nodes.find(node => node.type === 'button' && node.props.className === 'cluster-action-primary').props.onClick();
    assert.deepEqual(submitted.hostIds, [hostId]);
    assert.equal(submitted.artifactId, 'jdk-21');
    assert.equal(destination, '/jobs/job-1');
  });
}

test('last five minutes uses the same instant for IST labels and the UTC request', () => {
  const range = relativeLogRange(String(5 / 60), Date.parse('2026-11-24T20:32:00Z'));
  assert.equal(range.from, '2026-11-25T01:57');
  assert.equal(range.to, '2026-11-25T02:02');
  assert.equal(range.fromIso, '2026-11-24T20:27:00.000Z');
});
test('relative range advances across midnight and rejects invalid durations', () => {
  const first = relativeLogRange('1', Date.parse('2026-11-24T18:29:00Z'));
  const next = relativeLogRange('1', Date.parse('2026-11-24T18:31:00Z'));
  assert.equal(first.to, '2026-11-24T23:59');
  assert.equal(next.to, '2026-11-25T00:01');
  for (const value of ['custom', '0', '-1']) assert.throws(() => relativeLogRange(value, Date.now()));
});
test('Java inventory matches host ID before a conflicting IP and preserves reported vendor', () => {
  const result = reportedJavaRuntime({ hostId: 'a', hostname: 'node', ipAddress: '192.0.2.1' }, [
    { id: 'b', ipAddress: '192.0.2.1', javaVersion: '17' },
    { id: 'a', javaVersion: '21.0.13', javaVendor: 'Eclipse Adoptium' },
  ]);
  assert.equal(result.version, '21.0.13');
  assert.equal(result.distribution, 'Eclipse Adoptium');
});
test('external Java host can match a unique reported IP but never invents vendor', () => {
  const host = { hostId: 'external:a', hostname: '192.0.2.1', ipAddress: '192.0.2.1' };
  const record = { id: 'a', ipAddresses: '["192.0.2.1"]', javaVersion: '17.0.21' };
  assert.equal(reportedJavaRuntime(host, [record]).version, '17.0.21');
  assert.equal(reportedJavaRuntime(host, [record]).distribution, 'Not reported');
  assert.equal(reportedJavaRuntime(host, [record, { ...record, id: 'b' }]).version, 'Not reported');
});

function expand(node) {
  if (!node || typeof node !== 'object') return [];
  if (Array.isArray(node)) return node.flatMap(expand);
  if (typeof node.type === 'function') return expand(node.type(node.props));
  return [node, ...expand(node.props?.children)];
}
test('Add Node hides manual runtime/config paths and derives config path when installation changes', () => {
  const { AddNodeDeploymentSettings } = load('components/AddNodeDeploymentSettings.tsx', {
    react: { ...React, useState: initial => [initial, () => {}] },
  });
  const value = { installPath: '/opt/kafka', configDirectory: '/opt/kafka/config', javaHome: '/jdk', runtimeUser: 'kafka',
    brokerDataPaths: ['/data/broker'], brokerMetadataPath: '', brokerLogPath: '/logs/broker', brokerHeapXmx: '1G',
    controllerDataPaths: ['/data/controller'], controllerMetadataPath: '/metadata', controllerLogPath: '/logs/controller', controllerHeapXmx: '1G' };
  let changed;
  const nodes = expand(AddNodeDeploymentSettings({ value, onChange: next => { changed = next; }, disabled: false, role: 'broker' }));
  const inputs = nodes.filter(node => node.type === 'input');
  assert(!inputs.some(node => /Java|runtime|configuration directory|controller/i.test(node.props['aria-label'])));
  inputs.find(node => node.props['aria-label'] === 'Install directory').props.onChange({ target: { value: '/srv/kafka/' } });
  assert.equal(changed.configDirectory, '/srv/kafka/config');
  assert.equal(changed.javaHome, '/jdk');
  assert.equal(changed.runtimeUser, 'kafka');
  assert(nodes.some(node => node.type === 'button' && String(node.props.children).includes('Config')));
});
