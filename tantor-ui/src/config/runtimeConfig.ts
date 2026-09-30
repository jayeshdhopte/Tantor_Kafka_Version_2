type RawRuntimeConfig = {
  environment?: string;
  publicOrigin?: string;
  authEnabled?: boolean;
  keycloakUrl?: string;
  keycloakRealm?: string;
  keycloakClientId?: string;
  apiBasePath?: string;
  artifactApiBasePath?: string;
};

declare global {
  interface Window {
    __TANTOR_CONFIG__?: RawRuntimeConfig;
  }
}

export type RuntimeConfig = {
  environment: string;
  publicOrigin: string;
  authEnabled: boolean;
  keycloakUrl: string;
  keycloakRealm: string;
  keycloakClientId: string;
  apiBasePath: string;
  artifactApiBasePath: string;
};

const deployed = window.__TANTOR_CONFIG__ ?? {};
const productionBuild = import.meta.env.PROD;
const value = (runtimeValue: string | undefined, developmentValue: string | undefined) =>
  (runtimeValue || (productionBuild ? '' : developmentValue) || '').trim();

const config: RuntimeConfig = {
  environment: value(deployed.environment, 'development').toLowerCase(),
  publicOrigin: value(deployed.publicOrigin, window.location.origin),
  authEnabled: deployed.authEnabled ?? (productionBuild || import.meta.env.VITE_AUTH_ENABLED === 'true'),
  keycloakUrl: value(deployed.keycloakUrl, import.meta.env.VITE_KEYCLOAK_URL || 'https://keycloak.example.invalid'),
  keycloakRealm: value(deployed.keycloakRealm, import.meta.env.VITE_KEYCLOAK_REALM || 'development'),
  keycloakClientId: value(deployed.keycloakClientId, import.meta.env.VITE_KEYCLOAK_CLIENT_ID || 'tantor-ui-development'),
  apiBasePath: value(deployed.apiBasePath, '/api'),
  artifactApiBasePath: value(deployed.artifactApiBasePath, '/api/v1/artifacts'),
};

// `import.meta.env.PROD` describes how Vite produced the static bundle, not
// where the bundle is deployed. A release bundle is also used on development
// VMs, whose runtime-config explicitly disables SSO. Security requirements
// must therefore be based on the deployment environment supplied at runtime.
const securedDeployment = ['sit', 'uat', 'production'].includes(config.environment);

const requireRelativePath = (name: string, path: string) => {
  if (!path.startsWith('/') || path.startsWith('//') || path.includes('://')) {
    throw new Error(`${name} must be a same-origin absolute path`);
  }
};

const validateRuntimeConfig = () => {
  requireRelativePath('apiBasePath', config.apiBasePath);
  requireRelativePath('artifactApiBasePath', config.artifactApiBasePath);
  let publicOrigin: URL;
  try {
    publicOrigin = new URL(config.publicOrigin);
  } catch {
    throw new Error('publicOrigin must be an absolute URL');
  }
  if (publicOrigin.origin !== config.publicOrigin || publicOrigin.username || publicOrigin.password) {
    throw new Error('publicOrigin must be an origin without credentials, path, query, or fragment');
  }
  if (securedDeployment && (publicOrigin.protocol !== 'https:' || publicOrigin.origin !== window.location.origin)) {
    throw new Error('Production publicOrigin must be HTTPS and exactly match the browser origin');
  }
  if (!config.authEnabled) {
    if (securedDeployment) throw new Error('SIT, UAT, and production runtime configuration must enable authentication');
    return;
  }
  let keycloak: URL;
  try {
    keycloak = new URL(config.keycloakUrl);
  } catch {
    throw new Error('keycloakUrl must be an absolute HTTPS URL');
  }
  if (keycloak.protocol !== 'https:' || !keycloak.hostname || keycloak.username || keycloak.password
      || keycloak.origin !== config.keycloakUrl) {
    throw new Error('keycloakUrl must be an absolute HTTPS origin without credentials or a path');
  }
  if (securedDeployment && (keycloak.hostname === 'localhost' || keycloak.hostname.startsWith('127.')
      || /\.(example|invalid|test)$/.test(keycloak.hostname))) {
    throw new Error('Production keycloakUrl cannot use a local or placeholder host');
  }
  if (!config.keycloakRealm || !config.keycloakClientId) {
    throw new Error('keycloakRealm and keycloakClientId are required when authentication is enabled');
  }
};

validateRuntimeConfig();
export const runtimeConfig = Object.freeze(config);

export const resolveRuntimeApiUrl = (input: string | URL): string => {
  const url = new URL(input.toString(), window.location.origin);
  if (url.origin !== window.location.origin) return url.toString();

  const rewrite = (sourcePrefix: string, configuredPrefix: string) => {
    if (url.pathname === sourcePrefix || url.pathname.startsWith(`${sourcePrefix}/`)) {
      url.pathname = `${configuredPrefix}${url.pathname.slice(sourcePrefix.length)}`;
      return true;
    }
    return false;
  };
  if (!rewrite('/api/v1/artifacts', config.artifactApiBasePath)) {
    rewrite('/api', config.apiBasePath);
  }
  return url.toString();
};

const deploymentFetch = window.fetch.bind(window);
window.fetch = (input: RequestInfo | URL, init?: RequestInit) => {
  if (input instanceof Request) {
    const resolved = resolveRuntimeApiUrl(input.url);
    return deploymentFetch(resolved === input.url ? input : new Request(resolved, input), init);
  }
  return deploymentFetch(resolveRuntimeApiUrl(input), init);
};
