import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const parseDotEnv = (contents) => {
  const parsed = {};
  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const separator = line.indexOf('=');
    if (separator < 1) continue;
    const key = line.slice(0, separator).trim();
    let value = line.slice(separator + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"'))
        || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    parsed[key] = value;
  }
  return parsed;
};

const envPath = resolve('.env');
const fileEnvironment = existsSync(envPath) ? parseDotEnv(readFileSync(envPath, 'utf8')) : {};
const environment = { ...fileEnvironment, ...process.env };
const runtimeKeys = [
  'VITE_AUTH_ENABLED',
  'VITE_KEYCLOAK_URL',
  'VITE_KEYCLOAK_REALM',
  'VITE_KEYCLOAK_CLIENT_ID',
  'VITE_PUBLIC_ORIGIN',
  'TANTOR_PUBLIC_ORIGIN',
];

if (!runtimeKeys.some(key => Object.hasOwn(environment, key))) {
  console.log('No deployment UI environment found; keeping the bundled development runtime config.');
  process.exit(0);
}

const authEnabled = String(environment.VITE_AUTH_ENABLED || '').toLowerCase() === 'true';
const keycloakUrl = environment.VITE_KEYCLOAK_URL || '';
const keycloakRealm = environment.VITE_KEYCLOAK_REALM || '';
const keycloakClientId = environment.VITE_KEYCLOAK_CLIENT_ID || '';

if (authEnabled && (!keycloakUrl || !keycloakRealm || !keycloakClientId)) {
  throw new Error('Authentication is enabled but Keycloak URL, realm, or client ID is missing.');
}

const publicOrigin = environment.VITE_PUBLIC_ORIGIN || environment.TANTOR_PUBLIC_ORIGIN;
const runtimeConfig = {
  environment: (environment.VITE_APP_ENVIRONMENT || environment.TANTOR_ENVIRONMENT || 'development').toLowerCase(),
  authEnabled,
  keycloakUrl,
  keycloakRealm,
  keycloakClientId,
  apiBasePath: environment.VITE_API_BASE_PATH || '/api',
  artifactApiBasePath: environment.VITE_ARTIFACT_API_BASE_PATH || '/api/v1/artifacts',
};
const serialized = JSON.stringify(runtimeConfig, null, 2);
const originProperty = publicOrigin
  ? `  publicOrigin: ${JSON.stringify(publicOrigin)},\n`
  : '  publicOrigin: window.location.origin,\n';
const body = serialized.replace(/^\{\n/, `{\n${originProperty}`);
const output = `// Generated from deployment environment by npm run build.\nwindow.__TANTOR_CONFIG__ = Object.freeze(${body});\n`;

writeFileSync(resolve('dist/runtime-config.js'), output, 'utf8');
console.log(`Generated dist/runtime-config.js (authentication ${authEnabled ? 'enabled' : 'disabled'}).`);
