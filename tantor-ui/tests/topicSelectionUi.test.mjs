import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import React from 'react';
import ts from 'typescript';

const require = createRequire(import.meta.url);
const source = readFileSync(new URL('../src/pages/Topics.tsx', import.meta.url), 'utf8');
const ast = ts.createSourceFile('Topics.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
const stateNames = [];
function visit(node) {
  if (ts.isVariableDeclaration(node) && node.initializer && ts.isCallExpression(node.initializer)
    && node.initializer.expression.getText(ast) === 'useState') stateNames.push(node.name.elements[0].name.getText(ast));
  ts.forEachChild(node, visit);
}
visit(ast);
const compiled = ts.transpileModule(source, { compilerOptions: {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX,
} }).outputText;
const page = { content: [{ name: 'orders', partitionCount: 3, replicationFactor: 3, underReplicated: 0, messageCount: 90, internal: false },
  { name: 'payments', partitionCount: 3, replicationFactor: 3, underReplicated: 0, messageCount: 120, internal: false }],
  totalElements: 2, page: 0, size: 10, totalPages: 1, hasNext: false };

function all(node, predicate) {
  if (!node || typeof node !== 'object') return [];
  return [...(predicate(node) ? [node] : []), ...React.Children.toArray(node.props?.children).flatMap(child => all(child, predicate))];
}
const text = node => typeof node === 'string' || typeof node === 'number' ? String(node)
  : React.Children.toArray(node?.props?.children).map(text).join('');

// Exercise the real page and its event handlers without a browser or backend.
function harness(canManage = true) {
  const states = [];
  let cursor = 0;
  let clusterId = 'cluster-1';
  const requests = [], navigation = [];
  const Modal = () => null;
  const hooks = { ...React, useEffect() {}, useCallback: callback => callback, useMemo: calculate => calculate(),
    useState(initial) {
      const index = cursor++;
      if (!(index in states)) states[index] = typeof initial === 'function' ? initial() : initial;
      return [states[index], update => { states[index] = typeof update === 'function' ? update(states[index]) : update; }];
    } };
  const context = vm.createContext({ exports: {}, URL, Blob, navigator: { clipboard: { writeText: async () => {} } },
    window: { localStorage: { getItem: () => null, setItem() {} } },
    fetch: async (url, options) => { requests.push({ url, options }); return Response.json(options ? {} : page); },
    require: name => {
      if (name === 'react') return hooks;
      if (name === 'react-dom') return { createPortal: child => child };
      if (name === 'react-router-dom') return { useParams: () => ({ id: clusterId }), useNavigate: () => path => navigation.push(path) };
      if (name === 'lucide-react') return new Proxy({}, { get: () => () => null });
      if (name.endsWith('RefreshButton')) return { RefreshButton: 'button' };
      if (name.endsWith('usePermissions')) return { usePermissions: () => ({ canManage }) };
      if (name.endsWith('TopicActionConfirmationModal')) return { TopicActionConfirmationModal: Modal };
      if (name.endsWith('topicActionTypes')) return { topicActionCopy: { remove: { button: 'Delete' }, clear: { button: 'Clear messages' } } };
      if (name.endsWith('AnchoredMenu') || name.endsWith('.css')) return {};
      return require(name);
    } });
  vm.runInContext(compiled, context);
  const render = () => { cursor = 0; return context.exports.Topics(); };
  render();
  states[stateNames.indexOf('data')] = structuredClone(page);
  states[stateNames.indexOf('loading')] = false;
  const button = (tree, label) => all(tree, node => node.type === 'button' && text(node).trim() === label)[0];
  const checkbox = (tree, label) => all(tree, node => node.type === 'input' && node.props['aria-label'] === label)[0];
  return { render, button, checkbox, requests, navigation, Modal,
    setCluster: id => { clusterId = id; },
    get: name => states[stateNames.indexOf(name)],
    set: (name, value) => { states[stateNames.indexOf(name)] = value; } };
}

const selectionBoxes = tree => all(tree, node => node.type === 'input' && String(node.props['aria-label'] || '').startsWith('Select '));

test('topic checkboxes are hidden until Select topics, and Cancel clears and hides them', () => {
  const h = harness();
  let tree = h.render();
  assert.equal(selectionBoxes(tree).length, 0);
  const toolbar = all(tree, node => node.props.className === 'topics-toolbar-right')[0];
  assert.match(text(toolbar), /Select topicsShow internal topics/);
  h.button(tree, 'Select topics').props.onClick();
  tree = h.render();
  assert.equal(selectionBoxes(tree).length, 3);
  assert.equal(h.button(tree, 'Delete selected'), undefined);
  h.checkbox(tree, 'Select orders').props.onChange({ stopPropagation() {} });
  tree = h.render();
  assert.ok(h.button(tree, 'Delete selected'));
  assert.ok(h.button(tree, 'Copy names'));
  assert.ok(h.button(tree, 'Clear messages'));
  h.button(tree, 'Cancel selection').props.onClick();
  tree = h.render();
  assert.equal(selectionBoxes(tree).length, 0);
  assert.equal(h.button(tree, 'Delete selected'), undefined);
  assert.equal(h.get('topicSelection').names.size, 0);
  assert.deepEqual(h.requests, []);
});

test('select-all, partial selection and checkbox clicks preserve row navigation', () => {
  const h = harness();
  h.button(h.render(), 'Select topics').props.onClick();
  h.checkbox(h.render(), 'Select all topics on this page').props.onChange();
  let tree = h.render();
  assert.equal(h.checkbox(tree, 'Select orders').props.checked, true);
  assert.equal(h.checkbox(tree, 'Select payments').props.checked, true);
  let stopped = false;
  h.checkbox(tree, 'Select orders').props.onClick({ stopPropagation: () => { stopped = true; } });
  assert.equal(stopped, true);
  assert.deepEqual(h.navigation, []);
  h.checkbox(tree, 'Select orders').props.onChange({ stopPropagation() {} });
  tree = h.render();
  const input = {};
  h.checkbox(tree, 'Select all topics on this page').props.ref(input);
  assert.equal(input.indeterminate, true);
  const row = all(tree, node => node.type === 'tr' && node.props.onClick)[0];
  row.props.onClick();
  assert.deepEqual(h.navigation, ['/clusters/cluster-1/topics/orders']);
});

test('filters clear selection; cluster changes cannot expose a previous bulk confirmation', () => {
  const h = harness();
  const select = () => {
    h.button(h.render(), 'Select topics').props.onClick();
    h.checkbox(h.render(), 'Select orders').props.onChange({ stopPropagation() {} });
  };
  select();
  let tree = h.render();
  const search = all(tree, node => node.type === 'input' && node.props.placeholder === 'Search by topic name')[0];
  search.props.onChange({ target: { value: 'orders' } });
  assert.equal(selectionBoxes(h.render()).length, 0);
  assert.equal(h.get('topicSelection').names.size, 0);
  select();
  tree = h.render();
  h.button(tree, 'Delete selected').props.onClick();
  assert.equal(all(h.render(), node => node.type === h.Modal).length, 1);
  h.setCluster('cluster-2');
  assert.equal(all(h.render(), node => node.type === h.Modal).length, 0);
  assert.equal(selectionBoxes(h.render()).length, 0);
  assert.deepEqual(h.requests, []);
});

test('read-only users have no selection controls', () => {
  const h = harness(false);
  const tree = h.render();
  assert.equal(h.button(tree, 'Select topics'), undefined);
  assert.equal(selectionBoxes(tree).length, 0);
});

test('deletion still waits for confirmation and uses the existing selected-topic DELETE requests', async () => {
  const h = harness();
  h.button(h.render(), 'Select topics').props.onClick();
  h.checkbox(h.render(), 'Select all topics on this page').props.onChange();
  h.button(h.render(), 'Delete selected').props.onClick();
  assert.deepEqual(h.requests, []);
  const modal = all(h.render(), node => node.type === h.Modal)[0];
  assert.equal(modal.props.action, 'remove');
  await modal.props.onConfirm();
  assert.deepEqual(h.requests.filter(call => call.options?.method === 'DELETE').map(call => call.url), [
    '/api/v1/clusters/cluster-1/topics/orders', '/api/v1/clusters/cluster-1/topics/payments',
  ]);
  assert.equal(selectionBoxes(h.render()).length, 0);
  assert.equal(h.get('pendingAction'), null);
});
