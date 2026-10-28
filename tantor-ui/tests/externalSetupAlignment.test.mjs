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

test('bootstrap servers retains its separate right-hand panel and labelled hostname/port fields', () => {
  const input = declarations('.external-page .bootstrap-url-field > .bootstrap-options');
  assert.equal(input.border, '1px solid #D6D1DD');
  assert.equal(input.padding, '20px');
  assert.equal(input.margin, '0');
  assert.equal(input.background, '#F8FAFC');
  const label = declarations('.external-page .bootstrap-url-field .bootstrap-options > label');
  assert.equal(label['line-height'], declarations('.external-page .external-bootstrap-container .form-field-group > label')['line-height']);
});

test('the main purple Bootstrap server heading is outside and above the bordered fields container', () => {
  assert.match(source, /<section className="bootstrap-setup-section" aria-labelledby="bootstrap-setup-title">\s*<h3 id="bootstrap-setup-title">Bootstrap server<\/h3>\s*<div className="external-card bootstrap-server-card">/);
  assert.doesNotMatch(source, /<div className="external-card bootstrap-server-card">\s*<div className="card-header-title">/);
  assert.equal(declarations('.external-page .bootstrap-setup-section').gap, '12px');
  assert.equal(declarations('.external-page .bootstrap-setup-section > h3').color, '#4C1D6F');
  assert.equal(declarations('.external-page .bootstrap-setup-section > h3').margin, '0');
});

test('first endpoint and adjacent inputs align with matching heights and zero-minimum tracks', () => {
  assert.match(declarations('.external-page .bootstrap-server-card .form-row-three')['grid-template-columns'], /minmax\(0, 2fr\)/);
  assert.equal(declarations('.external-page .bootstrap-server-card .form-row-three')['align-items'], 'start');
  assert.equal(declarations('.external-page .bootstrap-server-card .form-field-group > :is(input, select)').height, '40px');
  assert.equal(declarations('.external-page .bootstrap-url-field .bootstrap-endpoint-row input').height, '40px');
  assert.equal(declarations('.external-page .bootstrap-url-field .connection-row > button').width, '40px');
  assert.equal(declarations('.external-page .bootstrap-url-field .connection-row')['grid-template-columns'], 'minmax(0, 1fr) 100px 40px');
  assert.equal(declarations('.external-page .bootstrap-server-card .form-row-three')['grid-template-areas'], "'name endpoints' 'environment endpoints'");
  assert.equal(declarations('.external-page .bootstrap-server-card .form-row-three > .form-field-group:nth-child(2)')['grid-area'], 'environment');
  assert.equal(declarations('.external-page .bootstrap-url-field')['grid-area'], 'endpoints');
});

test('setup fields stack at narrow widths without changing the shared form or its handlers', () => {
  assert.equal(declarations('.external-page .bootstrap-server-card .form-row-three', '(max-width: 1050px)')['grid-template-areas'], "'name' 'environment' 'endpoints'");
  assert.equal(declarations('.external-page .bootstrap-server-card .form-row-three', '(max-width: 640px)')['grid-template-columns'], 'minmax(0, 1fr)');
  assert.equal(declarations('.external-page .external-bootstrap-container .ssl-hostname-field')['grid-column'], '1 / -1');
  assert.equal(declarations('.external-page .external-tab-switcher', '(max-width: 640px)').width, '100%');
  assert.match(source, /<BootstrapServersInput separateFields value=\{form.bootstrapServers\}/);
  assert.match(source, /disabled=\{testing \|\| registering\}/);
  assert.match(source, /setForm\(prev => \(\{ \.\.\.prev, bootstrapServers: value \}\)\); setBootstrapResult\(null\)/);
});

test('bootstrap controls and selected setup tab have scoped purple treatments and visible focus', () => {
  assert.equal(declarations('.external-page .bootstrap-url-field .connection-row > button').color, '#4C1D6F');
  assert.equal(declarations('.external-page .bootstrap-url-field .bootstrap-options > button').background, '#4C1D6F');
  assert.equal(declarations('.external-page .external-tab-switcher .tab-btn.active').color, '#FFFFFF');
  assert.equal(declarations('.external-page .external-tab-switcher .tab-btn.active').background, '#4C1D6F');
  assert.match(declarations('.external-page .external-tab-switcher .tab-btn:focus-visible').outline, /2px solid/);
});
