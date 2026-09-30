import { createContext } from 'react';
import type { KeycloakTokenParsed } from 'keycloak-js';

export type DecodedToken = KeycloakTokenParsed & {
  name?: string;
  given_name?: string;
  family_name?: string;
  preferred_username?: string;
  email?: string;
  sid?: string;
  auth_time?: number;
  role?: string;
  roles?: string[];
  realm_access?: { roles?: string[] };
  resource_access?: Record<string, { roles?: string[] }>;
  groups?: string[];
};

export type AuthContextValue = {
  isInitializing: boolean;
  isAuthenticated: boolean;
  accessToken?: string;
  decodedToken?: DecodedToken;
  currentSessionId?: string;
  logout: () => Promise<void>;
  refreshToken: () => Promise<boolean>;
};

export const AuthContext = createContext<AuthContextValue | undefined>(undefined);
