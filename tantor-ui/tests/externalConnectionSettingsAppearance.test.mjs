import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import ts from 'typescript';
import postcss from 'postcss';

const require = createRequire(import.meta.url);
const source = readFileSync(new URL('../src/pages/ExternalConnectionSettings.tsx', import.meta.url), 'utf8');
const css = postcss.parse(readFileSync(new URL('../src/pages/ExternalConnectionSettings.css', import.meta.url), 'utf8'));
const compiled = ts.transpileModule(source, { compilerOptions: {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX,
} }).outputText;

function render({ manage = true, loading = false, saving = false, error = '', notice = '' } = {}) {
  const values = [{}, 'demo.test:9092', loading, 'demo-id', saving, error, notice];
  let stateIndex = 0;
  const context = vm.createContext({ exports: {}, require: name => {
    if (name.endsWith('.css')) return {};
    if (name === 'react') return { ...React, useEffect() {}, useState: () => [values[stateIndex++], () => {}] };
    if (name === 'react-router-dom') return { useParams: () => ({ id: 'demo-id' }), useNavigate: () => () => {} };
    if (name.endsWith('/usePermissions')) return { usePermissions: () => ({ canManage: manage }) };
    if (name.endsWith('/KeycloakService')) return {};
    if (name.endsWith('/externalConnectionOptions')) return {};
    if (name.endsWith('/ExternalConnectionOptionsForm')) return {
      BootstrapServersInput: props => React.createElement('input', { 'aria-label': 'Bootstrap fixture', disabled: props.disabled }),
      ExternalConnectionOptionsForm: props => React.createElement('fieldset', { disabled: props.disabled }),
    };
    return require(name);
  } });
  vm.runInContext(compiled, context);
  return renderToStaticMarkup(React.createElement(context.exports.ExternalConnectionSettings));
}

test('connection settings presents the existing form with navigation, header and save action', () => {
  const html = render();
  assert.match(html, /class="external-settings-page"/);
  assert.match(html, /Back to clusters/);
  assert.match(html, /External cluster connection settings/);
  assert.match(html, /class="external-settings-footer"/);
  assert.match(html, /Save connection settings/);
  assert.match(source, /navigate\('\/clusters'\)/);
  assert.match(source, /method: 'PUT'/);
  assert.match(source, /JSON.stringify\(\{ \.\.\.options, bootstrapServers: servers \}\)/);
  assert.match(source, /validateBootstrapServers\(bootstrap\); validateConnectionOptions\(options\)/);
});

test('read-only, loading, saving and result states retain their original behavior', () => {
  const readOnly = render({ manage: false });
  assert.doesNotMatch(readOnly, /Save connection settings/);
  assert.match(readOnly, /fieldset disabled/);
  assert.match(render({ loading: true }), /role="status"/);
  assert.doesNotMatch(render({ loading: true }), /Bootstrap fixture/);
  assert.match(render({ saving: true }), /disabled="" aria-busy="true"/);
  assert.match(render({ error: 'Failed to connect' }), /role="alert"/);
  assert.match(render({ notice: 'Settings saved' }), /role="status"/);
});

test('all new styling stays scoped to this page with responsive form tracks and keyboard focus', () => {
  css.walkRules(rule => {
    if (rule.parent.type === 'atrule' && rule.parent.name === 'keyframes') return;
    for (const selector of rule.selectors) assert.ok(selector.startsWith('.external-settings-'), selector);
  });
  const rules = [];
  css.walkRules(rule => rules.push(rule));
  const text = css.toString();
  assert.match(text, /minmax\(0, 1fr\)/);
  assert.ok(rules.some(rule => rule.selector.includes(':focus-visible')));
  assert.ok(rules.some(rule => rule.selector.endsWith('details[open] > summary')));
  assert.ok(rules.some(rule => rule.parent.params === '(max-width: 640px)' && rule.selector.endsWith('.connection-service-row')));
  assert.ok(rules.some(rule => rule.parent.params === '(prefers-reduced-motion: reduce)'));
});

test('page heading is unframed with a plain black icon rather than a colored tile', () => {
  const declarations = selector => {
    const result = {};
    css.walkRules(selector, rule => {
      if (rule.parent.type !== 'root') return;
      rule.walkDecls(({ prop, value }) => { result[prop] = value; });
    });
    return result;
  };
  const header = declarations('.external-settings-header');
  assert.equal(header.background, 'transparent');
  assert.equal(header.border, '0');
  assert.equal(header['border-radius'], '0');
  const icon = declarations('.external-settings-header > svg');
  assert.equal(icon.color, '#000000');
  assert.equal(icon.width, '22px');
  assert.equal(icon.height, '22px');
  assert.equal(icon.border, '0');
  assert.equal(icon.background, 'transparent');
  assert.equal(icon.padding, '0');
  assert.equal(icon['border-radius'], undefined);
  assert.equal(declarations('.external-settings-header h2')['overflow-wrap'], 'anywhere');
});

test('material-style accents tint removal controls and define form rows without changing their dimensions', () => {
  const declarations = selector => {
    const result = {};
    css.walkRules(selector, rule => {
      if (rule.parent.type !== 'root') return;
      rule.walkDecls(({ prop, value }) => { result[prop] = value; });
    });
    return result;
  };
  const remove = declarations(".external-settings-page .connection-options .ds-button[aria-label^='Remove']");
  assert.equal(remove.background, '#F2EAF9');
  assert.equal(remove.color, '#602C82');
  assert.equal(remove.width, undefined);
  assert.equal(remove.height, undefined);
  assert.equal(declarations('.external-settings-page .connection-row > .ds-button').width, '40px');
  assert.equal(declarations('.external-settings-page .connection-service-row')['border-radius'], '6px');
  assert.equal(declarations('.external-settings-page .bootstrap-options > .connection-row').background, '#F8FAFC');
  assert.equal(declarations('.external-settings-page .connection-options summary span').color, '#602C82');
});
