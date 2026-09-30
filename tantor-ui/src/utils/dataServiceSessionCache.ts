export interface DataServiceSessionState<TSummary> {
  selectedConnectionId: string | null;
  summary: TSummary | null;
  hasFetched: boolean;
  metadata?: Record<string, string>;
}

const CACHE_PREFIX = 'tantor:data-service:';
const memoryCache = new Map<string, DataServiceSessionState<unknown>>();

export function readDataServiceSession<TSummary>(service: string, clusterId?: string): DataServiceSessionState<TSummary> | null {
  if (!clusterId) return null;

  const cacheKey = `${CACHE_PREFIX}${service}:${clusterId}`;
  const memoryState = memoryCache.get(cacheKey);
  if (memoryState) return memoryState as DataServiceSessionState<TSummary>;

  try {
    const value = window.sessionStorage.getItem(cacheKey);
    return value ? JSON.parse(value) as DataServiceSessionState<TSummary> : null;
  } catch {
    return null;
  }
}

export function writeDataServiceSession<TSummary>(
  service: string,
  clusterId: string | undefined,
  state: DataServiceSessionState<TSummary>
) {
  if (!clusterId) return;

  const cacheKey = `${CACHE_PREFIX}${service}:${clusterId}`;
  memoryCache.set(cacheKey, state as DataServiceSessionState<unknown>);

  try {
    window.sessionStorage.setItem(cacheKey, JSON.stringify(state));
  } catch {
    // The in-memory cache still preserves data during tab navigation.
  }
}
