import type { UserResponse } from '../types/index.ts';
import { getValidToken } from '../services/KeycloakService.ts';

interface UserPayload {
  username: string;
  password: string;
  role: string;
}

interface UserRolePayload {
  role?: string;
  active?: boolean;
  password?: string;
}

interface AclQueryParams {
  principal?: string;
  resource_type?: string;
  resource_name?: string;
}

interface AclPayload {
  principal?: string;
  resourceType?: string;
  resourceName?: string;
  resourcePatternType?: string;
  pattern_type?: string;
  operation?: string;
  permissionType?: string;
  permission_type?: string;
  host?: string;
}

async function fetchWithAuth(url: string, options: RequestInit = {}) {
  const token = await getValidToken();
  const headers = new Headers(options.headers || {});
  if (token) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  headers.set('Content-Type', 'application/json');

  const res = await fetch(url, { ...options, headers });
  if (!res.ok) {
    const errorData = await res.json().catch(() => ({}));
    throw { response: { data: errorData } };
  }
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

export async function getUsers(): Promise<UserResponse[]> {
  return fetchWithAuth('/api/v1/auth/users');
}

export async function createAuthUser(data: UserPayload): Promise<UserResponse> {
  return fetchWithAuth('/api/v1/auth/users', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function updateAuthUser(id: string, data: UserRolePayload): Promise<UserResponse> {
  return fetchWithAuth(`/api/v1/auth/users/${id}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  });
}

export async function deleteAuthUser(id: string): Promise<unknown> {
  return fetchWithAuth(`/api/v1/auth/users/${id}`, {
    method: 'DELETE',
  });
}

export async function retryTask(clusterId: string, taskId: string): Promise<unknown> {
  return fetchWithAuth(`/api/v1/clusters/${clusterId}/actions/tasks/${taskId}/retry`, {
    method: 'POST',
  });
}

export async function resumeTask(clusterId: string, taskId: string): Promise<unknown> {
  return fetchWithAuth(`/api/v1/clusters/${clusterId}/actions/tasks/${taskId}/resume`, {
    method: 'POST',
  });
}

export async function rollbackTask(clusterId: string, taskId: string): Promise<unknown> {
  return fetchWithAuth(`/api/v1/clusters/${clusterId}/actions/tasks/${taskId}/rollback`, {
    method: 'POST',
  });
}

export async function cleanupTask(clusterId: string, taskId: string): Promise<unknown> {
  return fetchWithAuth(`/api/v1/clusters/${clusterId}/actions/tasks/${taskId}/cleanup`, {
    method: 'POST',
  });
}

// ── Security - ACLs ──────────────────────────────────
export const getAcls = (clusterId: string, params?: AclQueryParams) => {
  const query = new URLSearchParams(params as Record<string, string>).toString();
  return fetchWithAuth(`/api/v1/clusters/${clusterId}/security/acls${query ? '?' + query : ''}`);
};

export const createAcl = (clusterId: string, data: AclPayload) =>
  fetchWithAuth(`/api/v1/clusters/${clusterId}/security/acls`, {
    method: 'POST',
    body: JSON.stringify(data),
  });

export const deleteAcl = (clusterId: string, data: AclPayload) =>
  fetchWithAuth(`/api/v1/clusters/${clusterId}/security/acls`, {
    method: 'DELETE',
    body: JSON.stringify(data),
  });

