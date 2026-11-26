// Cluster hosts may contain one row per Kafka service. A Java upgrade selects
// a host once; keep its original target ID for parcel receipts and job requests.
export function uniqueJavaUpgradeHosts<T extends { hostId: string }>(hosts: T[]): T[] {
  const seen = new Set<string>();
  return hosts.filter(host => {
    if (seen.has(host.hostId)) return false;
    seen.add(host.hostId);
    return true;
  });
}
