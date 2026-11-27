import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import vm from 'node:vm';
import ts from 'typescript';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
const require = createRequire(import.meta.url);
const source = readFileSync(new URL('../src/pages/ClusterDetails.tsx', import.meta.url), 'utf8');

test('Producers is alongside Consumers for internal and external clusters; ACLs stays visible', () => {
  const declaration = source.slice(source.indexOf('  const tabs = ['), source.indexOf('  const isDropdownActive'));
  for (const mode of ['INTERNAL', 'EXTERNAL']) {
    for (const config of [false, true]) {
      const id = 'test-cluster';
      const context = vm.createContext({ id, cluster: { mode, status: 'SUCCESS' }, canManage: true,
        location: { pathname: `/clusters/${id}/${config ? 'config-change' : 'producers'}` },
        ...Object.fromEntries(['LineChart','Network','Server','LayoutList','Database','Users','FileJson','Plug','Shield','Settings','Activity','RefreshCw'].map(name => [name, name])) });
      const result = vm.runInContext(declaration + ';({ visibleTabs, dropdownTabs })', context);
      const labels = Array.from(result.visibleTabs, tab => tab.label);
      assert.equal(labels[labels.indexOf('Consumers') + 1], 'Producers');
      assert.ok(labels.includes('ACLs'));
      assert.equal(result.visibleTabs.find(tab => tab.label === 'Producers').to, '/clusters/test-cluster/producers');
      assert.ok(result.dropdownTabs.some(tab => tab.label === 'Java Upgrade'));
    }
  }
});

test('Producers page renders an honest unavailable state without a fake discovery request', () => {
  const page = readFileSync(new URL('../src/pages/Producers.tsx', import.meta.url), 'utf8');
  const compiled = ts.transpileModule(page, { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } }).outputText;
  const context = vm.createContext({ exports: {}, require: name => name.endsWith('.css') ? {} : require(name) });
  vm.runInContext(compiled, context);
  const html = renderToStaticMarkup(React.createElement(context.exports.Producers));
  assert.match(html, /Producer information is unavailable/);
  assert.match(html, /role="status"/);
  assert.doesNotMatch(html, /No producers found/);
  assert.ok(!page.includes('fetch('));
  const app = readFileSync(new URL('../src/App.tsx', import.meta.url), 'utf8');
  assert.ok(app.includes('path="producers" element={<Producers />}'));
});
