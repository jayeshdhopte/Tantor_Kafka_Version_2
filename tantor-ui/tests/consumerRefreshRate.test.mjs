import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import React from 'react';
import ts from 'typescript';
import postcss from 'postcss';

const require = createRequire(import.meta.url);
const source = readFileSync(new URL('../src/components/ConsumerRefreshRate.tsx', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX,
} }).outputText;

function harness(pickerAvailable = true) {
  const calls = { focus: 0, picker: 0, click: 0, prevented: 0, values: [] };
  const field = { contains: target => target === field || target === 'native-option',
    focus: () => calls.focus++, click: () => calls.click++,
    ...(pickerAvailable ? { showPicker: () => calls.picker++ } : {}),
  };
  const context = vm.createContext({ exports: {}, require: name => name === 'react'
    ? { useRef: () => ({ current: field }) } : require(name) });
  vm.runInContext(compiled, context);
  const tree = context.exports.ConsumerRefreshRate({ value: 0, onChange: value => calls.values.push(value) });
  const select = React.Children.toArray(tree.props.children).find(child => child.type === 'select');
  const click = target => tree.props.onClick({ target, preventDefault: () => calls.prevented++ });
  return { tree, select, click, field, calls };
}

test('clicking the label, empty space or any edge opens the original native picker', () => {
  const h = harness();
  assert.equal(h.tree.type, 'label');
  assert.equal(h.select.type, 'select');
  for (const target of ['label', 'padding', 'top edge', 'bottom edge', 'left edge', 'right edge']) h.click(target);
  assert.equal(h.calls.picker, 6);
  assert.equal(h.calls.focus, 6);
  assert.equal(h.calls.prevented, 6);
  assert.equal(h.calls.click, 0);
  assert.deepEqual(h.calls.values, []);
});

test('native select and option clicks are not intercepted or reopened', () => {
  const h = harness();
  h.click(h.field);
  h.click('native-option');
  assert.equal(h.calls.picker, 0);
  assert.equal(h.calls.prevented, 0);
  assert.equal(h.select.props.onKeyDown, undefined);
  assert.equal(h.select.props['aria-label'], 'Consumer groups refresh rate');
});

test('the original five native options and interval updates are unchanged', () => {
  const h = harness();
  const options = React.Children.toArray(h.select.props.children);
  assert.deepEqual(options.map(option => option.props.value), [0, 2, 5, 10, 15]);
  assert.deepEqual(options.map(option => option.props.children), ['Off', '2 sec', '5 sec', '10 sec', '15 sec']);
  for (const value of [0, 2, 5, 10, 15]) h.select.props.onChange({ target: { value: String(value) } });
  assert.deepEqual(h.calls.values, [0, 2, 5, 10, 15]);
  const fallback = harness(false);
  fallback.click('label');
  assert.equal(fallback.calls.focus, 1);
  assert.equal(fallback.calls.click, 1);
});

test('original styling is restored with no custom menu, arrow or invisible overlay', () => {
  const css = postcss.parse(readFileSync(new URL('../src/pages/Consumers.css', import.meta.url), 'utf8'));
  const declarations = selector => {
    const result = {};
    css.walkRules(selector, rule => rule.walkDecls(declaration => { result[declaration.prop] = declaration.value; }));
    return result;
  };
  assert.equal(declarations('.consumer-refresh-rate').height, '40px');
  assert.equal(declarations('.consumer-refresh-rate').width, undefined);
  assert.equal(declarations('.consumer-refresh-rate select').padding, '6px 4px');
  assert.equal(declarations('.consumer-refresh-rate select').opacity, undefined);
  assert.match(declarations('.consumer-refresh-rate:focus-within').outline, /2px solid/);
  assert.doesNotMatch(css.toString(), /consumer-refresh-rate-menu/);
  assert.doesNotMatch(source, /ChevronDown|AnchoredMenu|menuitemradio/);
});
