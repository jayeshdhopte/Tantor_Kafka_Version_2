import { useMemo } from 'react';
import { useAuth } from '../contexts/useAuth';
import { runtimeConfig } from '../config/runtimeConfig';

type TokenLike = {
  role?: string;
  roles?: string[];
  realm_access?: { roles?: string[] };
  resource_access?: Record<string, { roles?: string[] }>;
  groups?: string[];
};

const normalizeRole = (value: unknown) => {
  if (typeof value !== 'string') return undefined;
  const normalized = value.trim().toLowerCase();
  const role = normalized.split('/').filter(Boolean).pop() || normalized;
  if (role === 'admin' || role === 'administrator') return 'admin';
  if (role === 'monitor' || role === 'viewer' || role === 'readonly' || role === 'read_only') return 'monitor';
  return undefined;
};

const addRole = (roles: Set<string>, value: unknown) => {
  const role = normalizeRole(value);
  if (role) roles.add(role);
};

export function usePermissions() {
  const { decodedToken } = useAuth();

  return useMemo(() => {
    const token = decodedToken as TokenLike | undefined;
    const roles = new Set<string>();

    addRole(roles, token?.role);
    token?.roles?.forEach(role => addRole(roles, role));
    token?.realm_access?.roles?.forEach(role => addRole(roles, role));
    token?.resource_access?.[runtimeConfig.keycloakClientId]?.roles?.forEach(role => addRole(roles, role));
    token?.groups?.forEach(group => addRole(roles, group));

    const isAdmin = roles.has('admin');
    const isMonitor = !isAdmin && roles.has('monitor');
    const effectiveRole = isAdmin ? 'admin' : isMonitor ? 'monitor' : undefined;

    return {
      roles: Array.from(roles),
      effectiveRole,
      isAdmin,
      isMonitor,
      canManage: isAdmin,
    };
  }, [decodedToken]);
}
