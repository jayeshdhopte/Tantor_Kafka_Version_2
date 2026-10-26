import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { keycloakRedirectUri } from '../src/utils/keycloakRedirectUri.ts';

test('login destinations exclude a previous successful Keycloak callback', () => {
  const callback = '#state=previous-state&session_state=old-session&iss=http%3A%2F%2Flocalhost%3A8180%2Frealms%2FGatekeeper&code=used-code';
  assert.equal(keycloakRedirectUri(`http://localhost:5173/${callback}`), 'http://localhost:5173/');
  assert.equal(keycloakRedirectUri(`http://localhost:5173/clusters/test/logs?tab=logs${callback}`), 'http://localhost:5173/clusters/test/logs?tab=logs');
  assert.equal(keycloakRedirectUri(`https://tantor.example.com/jobs/123${callback}`), 'https://tantor.example.com/jobs/123');
});

test('login error callback parameters are not sent back as a new redirect', () => {
  assert.equal(keycloakRedirectUri('http://localhost:5173/#error=login_required&error_description=Expired&error_uri=https%3A%2F%2Fexample.com&state=old'), 'http://localhost:5173/');
});

test('query response mode and unrelated page parameters are handled independently', () => {
  assert.equal(keycloakRedirectUri('http://localhost:5173/jobs?tab=logs&state=old&code=used&session_state=old&iss=issuer#details'), 'http://localhost:5173/jobs?tab=logs#details');
  assert.equal(keycloakRedirectUri('http://localhost:5173/jobs?tab=logs#state=old&code=used&section=output'), 'http://localhost:5173/jobs?tab=logs#section=output');
});

test('ordinary routes, filters and fragment anchors are preserved', () => {
  for (const url of [
    'http://localhost:5173/',
    'http://localhost:5173/clusters?search=orders%20value#versions',
    'http://localhost:5173/jobs?search=a+b&search=c#section-2',
  ]) {
    assert.equal(keycloakRedirectUri(url), url);
  }
  assert.equal(keycloakRedirectUri('http://localhost:5173/jobs?state=FAILED#versions'), 'http://localhost:5173/jobs?state=FAILED#versions');
});

test('initialization and login use the clean destination while preserving PKCE and callback processing', () => {
  const service = readFileSync(new URL('../src/services/KeycloakService.ts', import.meta.url), 'utf8');
  assert.match(service, /const currentRedirectUri = \(\) => keycloakRedirectUri\(window.location.href\)/);
  assert.equal((service.match(/redirectUri: currentRedirectUri\(\)/g) || []).length, 2);
  assert.match(service, /onLoad: 'login-required'/);
  assert.match(service, /pkceMethod: 'S256'/);
  const utility = readFileSync(new URL('../src/utils/keycloakRedirectUri.ts', import.meta.url), 'utf8');
  assert.doesNotMatch(utility, /history\.|location\.|window\./);
});
