interface ServiceEndpoint {
  restHost?: string | null;
  restPort?: number | null;
  hostname?: string | null;
  ipAddress?: string | null;
  hostId?: string | null;
}

const normalizeHost = (host: string | null | undefined) =>
  (host || '').trim().toLowerCase().replace(/^\[|\]$/g, '').replace(/\.$/, '');

/** Match the inspected connection, including distinct instances on the same host. */
export function matchesServiceConnection(path: ServiceEndpoint, connection: { host: string; port: number }): boolean {
  const host = normalizeHost(connection.host);
  return Boolean(host) && path.restPort === connection.port &&
    [path.restHost, path.hostname, path.ipAddress, path.hostId].some(value => normalizeHost(value) === host);
}
