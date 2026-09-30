import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';

import {
  getKeycloak,
  getToken,
  initKeycloak,
  installAuthenticatedFetch,
  isAuthEnabled,
  login,
  logout as keycloakLogout,
} from '../services/KeycloakService';

import { type DecodedToken, type AuthContextValue, AuthContext } from './authContextDef';

const sessionIdFromToken = (token?: DecodedToken) => {
  const sessionState = token?.session_state;
  if (typeof sessionState === 'string') return sessionState;
  return token?.sid;
};

export function AuthProvider({ children }: { children: ReactNode }) {
  const authEnabled = isAuthEnabled();
  const [isInitializing, setIsInitializing] = useState(authEnabled);
  const [isAuthenticated, setIsAuthenticated] = useState(!authEnabled);
  const [accessToken, setAccessToken] = useState<string | undefined>();
  const [decodedToken, setDecodedToken] = useState<DecodedToken | undefined>();
  const [currentSessionId, setCurrentSessionId] = useState<string | undefined>();

  const syncAuthState = useCallback(() => {
    const keycloak = getKeycloak();
    const token = keycloak.tokenParsed as DecodedToken | undefined;
    setIsAuthenticated(Boolean(keycloak.authenticated));
    setAccessToken(getToken());
    setDecodedToken(token);
    setCurrentSessionId(sessionIdFromToken(token));
  }, []);

  const clearAuthState = useCallback(() => {
    setIsAuthenticated(false);
    setAccessToken(undefined);
    setDecodedToken(undefined);
    setCurrentSessionId(undefined);
  }, []);

  const refreshToken = useCallback(async () => {
    if (!authEnabled) {
      return true;
    }

    const keycloak = getKeycloak();
    try {
      const refreshed = await keycloak.updateToken(30);
      if (refreshed) {
        syncAuthState();
      }
      return true;
    } catch {
      clearAuthState();
      await login();
      return false;
    }
  }, [authEnabled, clearAuthState, syncAuthState]);

  const logout = useCallback(async () => {
    if (!authEnabled) {
      return;
    }

    clearAuthState();
    try {
      await keycloakLogout();
    } catch (e) {
      console.error('Logout failed', e);
      // Return to SSO instead of restoring an unauthenticated application shell.
      await login();
    }
  }, [authEnabled, clearAuthState]);
  useEffect(() => {
    if (!authEnabled) {
      installAuthenticatedFetch();
      Promise.resolve().then(() => {
        // Authentication-disabled development mode has no Keycloak identity.
        // Never fabricate a named administrator because it misrepresents both
        // the signed-in user and the permissions available to that user.
        setDecodedToken(undefined);
        setIsInitializing(false);
        setIsAuthenticated(true);
      });
      return;
    }

    let cancelled = false;
    let refreshTimer: number | undefined;
    const keycloak = getKeycloak();

    keycloak.onTokenExpired = () => {
      void refreshToken();
    };

    initKeycloak()
      .then(async authenticated => {
        if (cancelled) return;

        if (!authenticated) {
          clearAuthState();
          await login();
          return;
        }

        installAuthenticatedFetch();
        syncAuthState();
        refreshTimer = window.setInterval(() => {
          void refreshToken();
        }, 30000);
      })
      .catch(async () => {
        clearAuthState();
        await login();
      })
      .finally(() => {
        if (!cancelled) {
          setIsInitializing(false);
        }
      });

    return () => {
      cancelled = true;
      keycloak.onTokenExpired = undefined;
      if (refreshTimer) {
        window.clearInterval(refreshTimer);
      }
    };
  }, [authEnabled, clearAuthState, refreshToken, syncAuthState]);

  const value = useMemo<AuthContextValue>(
    () => ({
      isInitializing,
      isAuthenticated,
      accessToken,
      decodedToken,
      currentSessionId,
      logout,
      refreshToken,
    }),
    [accessToken, currentSessionId, decodedToken, isAuthenticated, isInitializing, logout, refreshToken],
  );

  if (isInitializing || !isAuthenticated) {
    return (
      <div className="auth-shell">
        <div className="auth-status">Connecting to Tantor SSO...</div>
      </div>
    );
  }

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

