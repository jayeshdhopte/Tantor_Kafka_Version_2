import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import postcss from 'postcss';

const source = readFileSync(new URL('../src/pages/ExternalClusters.tsx', import.meta.url), 'utf8');
const css = postcss.parse(readFileSync(new URL('../src/pages/ExternalClusters.css', import.meta.url), 'utf8'));
function declarations(selector, media) {
  const result = {};
  css.walkRules(rule => {
    if (!rule.selectors.includes(selector)) return;
    if ((rule.parent.type === 'atrule' ? rule.parent.params : undefined) !== media) return;
    rule.walkDecls(({ prop, value }) => { result[prop] = value; });
  });
  return result;
}
const prefix = '.external-page .external-browsing-settings';

test('setup browsing presentation is isolated around the existing form and state bindings', () => {
  assert.match(source, /<div className="external-browsing-settings">\s*<ExternalConnectionOptionsForm value=\{connectionOptions\} onChange=\{setConnectionOptions\} disabled=\{registering\} \/>\s*<\/div>/);
  assert.equal(declarations(`${prefix} .connection-options`).border, '0');
  assert.equal(declarations(`${prefix} legend`).color, '#4C1D6F');
  assert.equal(declarations(`${prefix} details:not([open]) > summary`)['margin-bottom'], '0');
  assert.equal(declarations(`${prefix} details[open] > summary`).background, '#F6F2FA');
});

test('setup browsing rows retain fixed icon-button sizes, wrapped labels and responsive grid tracks', () => {
  const remove = declarations(`${prefix} .ds-button[aria-label^='Remove']`);
  assert.equal(remove.color, '#4C1D6F');
  assert.equal(remove.width, '40px');
  assert.equal(remove.height, '40px');
  assert.equal(declarations(`${prefix} label`)['overflow-wrap'], 'anywhere');
  assert.match(declarations(`${prefix} .connection-service-row`)['grid-template-columns'], /minmax\(0, 1.4fr\)/);
  assert.equal(declarations(`${prefix} .connection-service-row`, '(max-width: 640px)')['grid-template-columns'], 'minmax(0, 1fr)');
  assert.equal(declarations(`${prefix} .connection-row > label`, '(max-width: 640px)')['flex-basis'], '100%');
  assert.match(declarations(`${prefix} :is(input, select, button, summary):focus-visible`).outline, /2px solid/);
});
