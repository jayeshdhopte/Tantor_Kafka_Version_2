import Keycloak from 'keycloak-js';
import { runtimeConfig } from '../config/runtimeConfig';

const keycloak = new Keycloak({
  url: runtimeConfig.keycloakUrl,
  realm: runtimeConfig.keycloakRealm,
  clientId: runtimeConfig.keycloakClientId,
});

export const isAuthEnabled = () => runtimeConfig.authEnabled;

let initializationPromise: Promise<boolean> | undefined;
let authenticatedFetchInstalled = false;
let nativeFetch: typeof window.fetch | undefined;

const currentRedirectUri = () => window.location.href;
const pathMatches = (path: string, prefix: string) => path === prefix || path.startsWith(`${prefix}/`);
const isRuntimeApiPath = (path: string) => pathMatches(path, '/api')
  || pathMatches(path, runtimeConfig.apiBasePath)
  || pathMatches(path, runtimeConfig.artifactApiBasePath);

export const initKeycloak = (): Promise<boolean> => {
  if (!isAuthEnabled()) {
    return Promise.resolve(true);
  }

  if (!initializationPromise) {
    initializationPromise = keycloak.init({
      onLoad: 'login-required',
      pkceMethod: 'S256',
      checkLoginIframe: false,
      redirectUri: currentRedirectUri(),
    });
  }

  return initializationPromise;
};

export const login = () =>
  isAuthEnabled()
    ? keycloak.login({
        redirectUri: currentRedirectUri(),
      })
    : Promise.resolve();

export const logout = () => {
  if (!isAuthEnabled()) {
    return Promise.resolve();
  }

  // Reset the cached init promise so a fresh init happens when the app
  // re-mounts after the Keycloak redirect.  Without this the stale promise
  // resolves immediately with `authenticated = true` from the previous session.
  initializationPromise = undefined;

  return keycloak.logout({
    redirectUri: `${window.location.origin}/`,
  });
};

export const getToken = () => isAuthEnabled() ? keycloak.token : undefined;

export const getKeycloak = () => keycloak;

export const getValidToken = async () => {
  if (!isAuthEnabled()) {
    return undefined;
  }

  if (!keycloak.authenticated) {
    await login();
    return undefined;
  }

  try {
    await keycloak.updateToken(30);
    return keycloak.token;
  } catch {
    await login();
    return undefined;
  }
};

export const installAuthenticatedFetch = () => {
  if (authenticatedFetchInstalled) return;

  nativeFetch = window.fetch.bind(window);
  authenticatedFetchInstalled = true;

  window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : undefined;
    const url = new URL(request?.url || input.toString(), window.location.origin);
    const headers = new Headers(init?.headers || request?.headers);

    if (url.origin === window.location.origin && isRuntimeApiPath(url.pathname)) {
      if (isAuthEnabled()) {
        const token = await getValidToken();
        if (token) {
          headers.set('Authorization', `Bearer ${token}`);
        }
      }
    }

    return nativeFetch!(input, {
      ...init,
      headers,
    });
  };
};
