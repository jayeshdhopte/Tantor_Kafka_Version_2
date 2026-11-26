import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import vm from 'node:vm';
import React from 'react';
import ts from 'typescript';

const require = createRequire(import.meta.url);
function load(path, mocks = {}) {
  const source = readFileSync(new URL('../src/' + path, import.meta.url), 'utf8');
  const compiled = ts.transpileModule(source, { compilerOptions: {
    module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX,
  } }).outputText;
  const context = vm.createContext({ exports: {}, require: name => mocks[name] ?? require(name) });
  vm.runInContext(compiled, context);
  return context.exports;
}
const { relativeLogRange } = load('utils/logTimeRange.ts');
const { reportedJavaRuntime } = load('utils/reportedJavaRuntime.ts');

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
