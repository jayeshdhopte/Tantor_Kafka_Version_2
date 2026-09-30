export type ClusterStatusTone = 'state-positive' | 'state-negative';

const POSITIVE_STATES = [
  'ONLINE',
  'ACTIVE',
  'SUCCESS',
  'SUCCEEDED',
  'HEALTHY',
  'CONNECTED',
  'WORKING',
  'DEPLOYED',
  'RUNNING',
  'AVAILABLE',
  'READY',
  'UP',
];

const NEGATIVE_STATES = [
  'NOT CONNECTED',
  'NOT DEPLOYED',
  'NOT INSTALLED',
  'DISCONNECTED',
  'UNAVAILABLE',
  'INACTIVE',
  'OFFLINE',
  'FAILED',
  'FAILURE',
  'DEGRADED',
  'UNKNOWN',
  'ERROR',
  'STOPPED',
  'DELETED',
  'DELETING',
  'PENDING',
  'CHECKING',
  'VALIDATING',
  'DOWN',
];

const normalizeState = (value: unknown) => String(value || '')
  .trim()
  .toUpperCase()
  .replace(/[^A-Z0-9]+/g, ' ');

const includesState = (value: string, state: string) =>
  (` ${value} `).includes(` ${state} `);

/** Uses the first recognizable state, allowing callers to pass runtime-first fallbacks. */
export function isPositiveClusterState(...values: unknown[]): boolean {
  for (const rawValue of values) {
    const value = normalizeState(rawValue);
    if (!value) continue;
    if (NEGATIVE_STATES.some(state => includesState(value, state))) return false;
    if (POSITIVE_STATES.some(state => includesState(value, state))) return true;
  }
  return false;
}

export function clusterStatusTone(...values: unknown[]): ClusterStatusTone {
  return isPositiveClusterState(...values) ? 'state-positive' : 'state-negative';
}
