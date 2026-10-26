import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';
import postcss from 'postcss';

const require = createRequire(import.meta.url);
const read = path => readFileSync(new URL(`../src/${path}`, import.meta.url), 'utf8');
const compiled = ts.transpileModule(read('components/RefreshButton.tsx'), { compilerOptions: {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX,
} }).outputText;
const flush = async () => { for (let i = 0; i < 10; i++) await Promise.resolve(); };

function harness(t, props = {}) {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const slots = [], cleanups = [];
  let cursor = 0, updates = 0;
  const context = vm.createContext({ exports: {}, setTimeout, clearTimeout, require: name => {
    if (name.endsWith('.css')) return {};
    if (name !== 'react') return require(name);
    return {
      useState(initial) {
        const i = cursor++;
        if (!(i in slots)) slots[i] = initial;
        return [slots[i], value => { slots[i] = value; updates++; }];
      },
      useRef(initial) {
        const i = cursor++;
        return slots[i] ??= { current: initial };
      },
      useEffect(effect) {
        const i = cursor++;
        if (!(i in slots)) { slots[i] = true; cleanups.push(effect()); }
      },
    };
  } });
  vm.runInContext(compiled, context);
  const render = () => { cursor = 0; return context.exports.RefreshButton(props); };
  return { render, unmount: () => cleanups.forEach(cleanup => cleanup?.()), updates: () => updates };
}

test('refresh invokes the original handler immediately once and shows a full rotation for fast reads', async t => {
  const event = {}, children = { icon: true };
  let calls = 0;
  const h = harness(t, { className: 'existing-toolbar', title: 'Refresh', children,
    onClick: received => { assert.equal(received, event); calls++; } });
  const button = h.render();
  assert.equal(button.type, 'button');
  assert.equal(button.props.type, 'button');
  assert.equal(button.props.children, children);
  assert.equal(button.props.className, 'existing-toolbar refresh-feedback-button');
  const pending = button.props.onClick(event);
  assert.equal(calls, 1);
  await button.props.onClick(event);
  assert.equal(calls, 1);
  assert.equal(h.render().props['data-refreshing'], true);
  assert.equal(h.render().props.disabled, true);
  t.mock.timers.tick(599);
  await flush();
  assert.equal(h.render().props['aria-busy'], true);
  t.mock.timers.tick(1);
  await pending;
  assert.equal(h.render().props['data-refreshing'], false);
  assert.equal(h.render().props.disabled, false);
});

test('a slow request keeps spinning until it completes, not just until the animation timer ends', async t => {
  let finish;
  const h = harness(t, { onClick: () => new Promise(resolve => { finish = resolve; }) });
  const pending = h.render().props.onClick({});
  t.mock.timers.tick(2000);
  await flush();
  assert.equal(h.render().props['data-refreshing'], true);
  finish();
  await pending;
  assert.equal(h.render().props['data-refreshing'], false);
});

test('failed callbacks stop spinning and retain the original error without retrying', async t => {
  let calls = 0;
  const h = harness(t, { onClick: () => { calls++; throw new Error('Offline'); } });
  const pending = assert.rejects(h.render().props.onClick({}), /Offline/);
  t.mock.timers.tick(600);
  await pending;
  assert.equal(calls, 1);
  assert.equal(h.render().props['data-refreshing'], false);
});

test('permission and loading attributes are preserved, and unmount cancels delayed state updates', async t => {
  let calls = 0;
  const disabled = harness(t, { disabled: true, 'aria-label': 'Refresh agents', 'aria-busy': true, onClick: () => calls++ });
  const button = disabled.render();
  await button.props.onClick({});
  assert.equal(calls, 0);
  assert.equal(button.props['aria-label'], 'Refresh agents');
  assert.equal(button.props['aria-busy'], true);
  t.mock.timers.reset();
  const h = harness(t, { onClick: () => {} });
  const pending = h.render().props.onClick({});
  h.unmount();
  const updates = h.updates();
  await pending;
  t.mock.timers.tick(1000);
  assert.equal(h.updates(), updates);
});

test('animation is scoped to refresh icons, supports ongoing loading and respects reduced motion', () => {
  const css = postcss.parse(read('components/RefreshButton.css'));
  const rotation = [], reduced = [];
  css.walkDecls('animation', declaration => {
    (declaration.parent.parent.type === 'atrule' ? reduced : rotation).push(declaration.value);
  });
  assert.deepEqual(rotation, ['refresh-feedback-rotation 0.6s linear infinite']);
  assert.deepEqual(reduced, ['none']);
  assert.match(css.toString(), /prefers-reduced-motion: reduce/);
  assert.match(css.toString(), /\.refresh-feedback-button > svg\.spin/);
  assert.match(css.toString(), /rotate\(360deg\)/);
  assert.doesNotMatch(read('components/RefreshButton.tsx'), /\bfetch\s*\(/);
});

test('all 29 additional refresh controls use the shared button while existing log and consumer feedback stays intact', () => {
  const coverage = {
    'pages/Alerts.tsx': 1, 'pages/Artifacts.tsx': 1, 'pages/AuditLogs.tsx': 2,
    'pages/ClusterDeployment.tsx': 1, 'pages/ClusterDetails.tsx': 1, 'pages/Clusters.tsx': 1,
    'pages/ConfigChange.tsx': 1, 'pages/Dashboard.tsx': 1, 'pages/ExternalClusters.tsx': 1,
    'pages/Hosts.tsx': 1, 'pages/JobsList.tsx': 1, 'pages/KafkaConnect.tsx': 2,
    'pages/Monitoring.tsx': 1, 'pages/Partitions.tsx': 1, 'pages/SchemaRegistry.tsx': 2,
    'pages/TopicDetails.tsx': 5, 'pages/Topics.tsx': 1,
    'components/ConfigChangeHistory.tsx': 1, 'components/KafkaConnectorDetails.tsx': 1,
    'components/KafkaGroupPanel.tsx': 1, 'components/SecurityManager.tsx': 1,
    'components/ServiceDirectories.tsx': 1,
  };
  let total = 0;
  for (const [file, count] of Object.entries(coverage)) {
    const source = read(file);
    const ast = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
    const buttons = [];
    function visit(node) {
      if (ts.isJsxElement(node) && node.openingElement.tagName.getText(ast) === 'RefreshButton') buttons.push(node);
      ts.forEachChild(node, visit);
    }
    visit(ast);
    assert.equal(buttons.length, count, file);
    for (const button of buttons) {
      assert.ok(button.openingElement.attributes.properties.some(prop => prop.name?.getText(ast) === 'onClick'), file);
      assert.match(button.getText(ast), /<(?:RefreshCw|CustomRefreshIcon)\b/, file);
    }
    if (file === 'pages/ClusterDetails.tsx') {
      assert.doesNotMatch(buttons[0].getText(ast), /setCluster\(null\)/);
      assert.match(buttons[0].getText(ast), /return fetch\(`\/api\/v1\/ui\/clusters\/\$\{id\}`\)/);
    }
    total += count;
  }
  assert.equal(total, 29);
  assert.match(read('pages/Consumers.tsx'), /onClick=\{handleManualRefresh\}/);
  assert.match(read('pages/DeploymentLogs.tsx'), /onClick=\{\(\) => fetchTasks\(true\)\}/);
});

test('cluster header still fetches cluster data and reloads the active tab without removing its refresh button', async () => {
  const source = read('pages/ClusterDetails.tsx');
  const ast = ts.createSourceFile('ClusterDetails.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  let handler;
  function visit(node) {
    if (ts.isJsxElement(node) && node.openingElement.tagName.getText(ast) === 'RefreshButton') {
      handler = node.openingElement.attributes.properties.find(prop => prop.name?.getText(ast) === 'onClick').initializer.expression.getText(ast);
    }
    ts.forEachChild(node, visit);
  }
  visit(ast);
  const calls = [], updates = [], cluster = { id: 'test-cluster', status: 'SUCCESS' };
  let revision = 0;
  const context = vm.createContext({ id: cluster.id, console,
    fetch: async url => { calls.push(url); return { json: async () => cluster }; },
    setCluster: value => updates.push(value), setRefreshRevision: update => { revision = update(revision); },
  });
  await vm.runInContext(`(${handler})()`, context);
  assert.deepEqual(calls, ['/api/v1/ui/clusters/test-cluster']);
  assert.deepEqual(updates, [cluster]);
  assert.equal(revision, 1);
  assert.equal((source.match(/<Outlet key=\{refreshRevision\} \/>/g) || []).length, 2);
});
