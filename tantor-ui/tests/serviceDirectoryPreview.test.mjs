import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import React from 'react';
import ts from 'typescript';

const require = createRequire(import.meta.url);
const read = path => readFileSync(new URL(`../src/${path}`, import.meta.url), 'utf8');
const compile = source => ts.transpileModule(source, { compilerOptions: {
  module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX,
} }).outputText;
const flush = () => new Promise(resolve => setImmediate(resolve));
const text = node => typeof node === 'string' ? node
  : React.Children.toArray(node?.props?.children).map(text).join(' ');
function all(node, predicate) {
  if (!node || typeof node !== 'object') return [];
  return [...(predicate(node) ? [node] : []), ...React.Children.toArray(node.props?.children).flatMap(child => all(child, predicate))];
}
const rows = ['SCHEMA_REGISTRY', 'KAFKA_CONNECT'].flatMap(serviceType => [
  { serviceType, hostId: 'host-191', hostname: 'broker3.translab.io', ipAddress: '192.168.3.191',
    installDir: '/opt/191', configFile: '/etc/191.properties', workingDir: '/var/lib/191', logDir: '/var/log/191' },
  { serviceType, hostId: 'host-229', hostname: 'node229.translab.io', ipAddress: '192.168.3.229',
    installDir: '/opt/229', configFile: '/etc/229.properties', workingDir: '/var/lib/229', logDir: '/var/log/229' },
]);

function harness(props) {
  const states = [], effects = [], requests = [];
  let cursor = 0;
  const context = vm.createContext({ exports: {}, AbortController,
    fetch: async (url, options) => { requests.push({ url, options }); return Response.json({ dataServicePaths: rows }); },
    require: name => {
      if (name === 'react') return { ...React, useId: () => 'directories', useEffect: effect => effects.push(effect),
        useState(initial) { const i = cursor++; if (!(i in states)) states[i] = initial;
          return [states[i], value => { states[i] = typeof value === 'function' ? value(states[i]) : value; }]; } };
      if (name.endsWith('RefreshButton')) return { RefreshButton: 'button' };
      if (name === 'lucide-react') return new Proxy({}, { get: () => 'svg' });
      if (name.endsWith('.css')) return {};
      return require(name);
    },
  });
  vm.runInContext(compile(read('components/ServiceDirectories.tsx')), context);
  return { requests, render() { cursor = 0; effects.length = 0; return context.exports.ServiceDirectories(props); },
    load: () => effects[0]() };
}

for (const serviceType of ['SCHEMA_REGISTRY', 'KAFKA_CONNECT']) {
  test(`${serviceType}: selected hostname or IP shows only that server and refresh retains the filter`, async () => {
    for (const connectionHost of ['192.168.3.229', ' NODE229.TRANSLAB.IO. ']) {
      const h = harness({ clusterId: 'cluster-1', serviceType, preview: true, connectionHost });
      h.render(); const cleanup = h.load(); await flush();
      let tree = h.render();
      const articles = all(tree, node => node.type === 'article');
      assert.equal(articles.length, 1);
      assert.match(text(articles[0]), /node229.translab.io/);
      assert.doesNotMatch(text(tree), /broker3|192\.168\.3\.191|\/opt\/191/);
      const refresh = all(tree, node => node.type === 'button')[0]; refresh.props.onClick();
      tree = h.render(); assert.match(text(tree), /Loading service directories/);
      cleanup(); const refreshCleanup = h.load(); await flush(); tree = h.render();
      assert.equal(h.requests.length, 2);
      assert.equal(all(tree, node => node.type === 'article').length, 1);
      assert.doesNotMatch(text(tree), /broker3|\/opt\/191/); refreshCleanup();
    }
  });
  test(`${serviceType}: missing or unmatched selected host never falls back to every server`, async () => {
    for (const connectionHost of [undefined, '', 'node229', '192.168.3.2', 'unknown.example.com']) {
      const h = harness({ clusterId: 'cluster-1', serviceType, preview: true, connectionHost });
      h.render(); const cleanup = h.load(); await flush(); const tree = h.render();
      assert.equal(all(tree, node => node.type === 'article').length, 0);
      assert.match(text(tree), /No directory information has been reported for this connection/); cleanup();
    }
  });
}

test('whole-service component retains its unfiltered behaviour outside an instance preview', async () => {
  const h = harness({ clusterId: 'cluster-1', serviceType: 'SCHEMA_REGISTRY' });
  h.render(); const cleanup = h.load(); await flush();
  assert.equal(all(h.render(), node => node.type === 'article').length, 2); cleanup();
});

for (const page of ['SchemaRegistry', 'KafkaConnect']) {
  test(`${page}: inspecting an option uses that option's host even when another connection is selected`, () => {
    const source = read(`pages/${page}.tsx`);
    const ast = ts.createSourceFile(`${page}.tsx`, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
    let callback;
    function visit(node) {
      if (ts.isJsxAttribute(node) && node.name.getText(ast) === 'onInspect'
          && node.initializer && ts.isJsxExpression(node.initializer)) callback = node.initializer.expression.getText(ast);
      ts.forEachChild(node, visit);
    }
    visit(ast); assert.ok(callback);
    let preview;
    const savedConnections = [
      { id: 'first', connectionName: 'First', host: '192.168.3.191', isDefault: true },
      { id: 'second', connectionName: 'Second', host: '192.168.3.229', isDefault: false },
    ];
    const context = vm.createContext({ savedConnections, selectedConnectionId: 'first',
      setDirectoryPreview: value => { preview = value; } });
    vm.runInContext(compile(`const inspect = ${callback}; inspect('second', 'focus-target');`), context);
    assert.equal(preview.connectionHost, '192.168.3.229');
    assert.equal(preview.connectionName, 'Second');
    assert.equal(preview.returnFocus, 'focus-target');
  });
}

test('modal forwards the inspected host into its directory component', () => {
  const context = vm.createContext({ exports: {}, document: { body: {} }, require: name => {
    if (name === 'react') return { ...React, useEffect() {}, useId: () => 'title', useRef: () => ({ current: null }) };
    if (name === 'react-dom') return { createPortal: node => node };
    if (name === 'lucide-react') return { X: 'svg' };
    if (name.endsWith('ServiceDirectories')) return { ServiceDirectories: 'directories' };
    return require(name);
  } });
  vm.runInContext(compile(read('components/ServiceDirectoriesModal.tsx')), context);
  const tree = context.exports.ServiceDirectoriesModal({ clusterId: 'cluster-1', serviceType: 'SCHEMA_REGISTRY',
    connectionName: 'Second', connectionHost: '192.168.3.229', returnFocus: { focus() {} }, onClose() {} });
  const directory = all(tree, node => node.type === 'directories')[0];
  assert.equal(directory.props.connectionHost, '192.168.3.229'); assert.equal(directory.props.preview, true);
});
