import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import ts from 'typescript';
import * as live from '../src/utils/liveRefresh.ts';

const require = createRequire(import.meta.url);
const source = readFileSync(new URL('../src/components/LiveRefreshControl.tsx', import.meta.url), 'utf8');
const code = ts.transpileModule(source, { compilerOptions: {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX,
} }).outputText;
function harness(enabled = false, seconds = 15) {
  const calls = [], focus = [];
  const states = [{ focus: () => focus.push(true) }, false];
  let index = 0;
  const context = vm.createContext({ exports: {}, document: { activeElement: null }, require: name => {
    if (name === 'react') return { ...React, useId: () => 'refresh-menu', useState: () => {
      const current = index++;
      return [states[current], next => { states[current] = typeof next === 'function' ? next(states[current]) : next; }];
    } };
    if (name.endsWith('liveRefresh')) return live;
    if (name.endsWith('.css')) return {};
    if (name === './AnchoredMenu') return { AnchoredMenu: props => React.createElement(React.Fragment, null, props.children) };
    return require(name);
  } });
  vm.runInContext(code, context);
  const render = () => { index = 0; return context.exports.LiveRefreshControl({ enabled, seconds, label: 'Partitions', onChange: (...args) => calls.push(args) }); };
  return { render, states, calls, focus, context };
}
function children(tree) { return React.Children.toArray(tree.props.children); }
function menu(tree) { return children(tree).find(child => child.type !== 'button').props.children; }

test('the whole native button toggles one menu with the same five Topics intervals', () => {
  const h = harness();
  let tree = h.render();
  const trigger = children(tree)[0];
  assert.equal(trigger.type, 'button');
  assert.equal(trigger.props['aria-label'], 'Partitions auto refresh: Off');
  trigger.props.onClick(); tree = h.render();
  assert.equal(children(tree)[0].props['aria-expanded'], true);
  const options = children(menu(tree)).filter(child => child.type === 'button');
  assert.equal(options.length, 6);
  const html = renderToStaticMarkup(tree);
  for (const interval of [5, 10, 15, 30, 60]) assert.match(html, new RegExp(`Live \\| ${interval} Sec`));
  assert.match(html, /Resume Live Feed/);
  options[1].props.onClick();
  assert.deepEqual(h.calls, [[true, 10]]);
  assert.equal(h.states[1], false);
  assert.equal(h.focus.length, 1);
});

test('Pause retains the interval and the enabled selection is accessible', () => {
  const h = harness(true, 30);
  children(h.render())[0].props.onClick();
  const tree = h.render();
  const options = children(menu(tree)).filter(child => child.type === 'button');
  assert.equal(options[3].props['aria-checked'], true);
  assert.equal(options[0].props['aria-checked'], false);
  assert.match(renderToStaticMarkup(tree), /Pause Live Feed/);
  options.at(-1).props.onClick();
  assert.deepEqual(h.calls, [[false, 30]]);
});

test('keyboard menu navigation and Escape preserve full-control accessibility', () => {
  const h = harness();
  children(h.render())[0].props.onClick();
  const content = menu(h.render());
  const focused = [];
  const buttons = [0, 1, 2, 3, 4, 5].map(index => ({ focus: () => focused.push(index) }));
  const currentTarget = { querySelectorAll: () => buttons };
  content.props.onKeyDown({ key: 'ArrowDown', currentTarget, preventDefault() {} });
  content.props.onKeyDown({ key: 'End', currentTarget, preventDefault() {} });
  content.props.onKeyDown({ key: 'Escape', currentTarget, preventDefault() {} });
  assert.deepEqual(focused, [0, 5]);
  assert.equal(h.states[1], false);
});
