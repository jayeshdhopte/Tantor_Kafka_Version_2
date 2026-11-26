export type ReportedJavaHost = {
  id: string; hostname?: string; ipAddress?: string; ipAddresses?: string;
  javaVersion?: string; javaVendor?: string; javaDistribution?: string;
};

function addresses(host: ReportedJavaHost) {
  try {
    const value: unknown = JSON.parse(host.ipAddresses || '[]');
    return [host.ipAddress, ...(Array.isArray(value) ? value.filter(v => typeof v === 'string') : [])].filter(Boolean);
  } catch { return [host.ipAddress, host.ipAddresses].filter(Boolean); }
}

export function reportedJavaRuntime(host: { hostId: string; hostname: string; ipAddress: string }, inventory: ReportedJavaHost[]) {
  const exact = inventory.find(item => item.id === host.hostId);
  const matches = inventory.filter(item =>
    Boolean(item.hostname && item.hostname === host.hostname)
    || addresses(item).some(ip => ip === host.ipAddress || ip === host.hostname));
  const item = exact ?? (matches.length === 1 ? matches[0] : undefined);
  const version = item?.javaVersion?.trim() || 'Not reported';
  // Never infer Oracle/OpenJDK from a version number alone.
  const distribution = item?.javaDistribution?.trim() || item?.javaVendor?.trim()
    || version.match(/Eclipse Temurin|Temurin|Oracle(?: JDK)?|OpenJDK|Corretto|Zulu|GraalVM|Semeru/i)?.[0]
    || 'Not reported';
  return { version, distribution };
}
