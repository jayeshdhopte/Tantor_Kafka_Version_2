export type SchemaRegistryConfig = {
  artifactId: string; port: number; heapMin: string; heapSize: string; installDir: string; configDir: string;
  logDir: string; workingDir: string; compatibility: string;
};

export type KafkaConnectConfig = {
  artifactId: string; port: number; heapMin: string; heapSize: string; groupId: string; installDir: string;
  configDir: string; logDir: string; workingDir: string; pluginDir: string;
};

export const defaultSchemaRegistryConfig = (unified = true): SchemaRegistryConfig => ({
  artifactId: '', port: 8081, heapMin: '1G', heapSize: '1G', installDir: unified ? '' : '/opt/tantor/schema-registry',
  configDir: unified ? '' : '/opt/tantor/schema-registry/etc/schema-registry', logDir: '/var/log/tantor/schema-registry',
  workingDir: '/var/lib/tantor/schema-registry', compatibility: 'BACKWARD',
});

export const defaultKafkaConnectConfig = (): KafkaConnectConfig => ({
  artifactId: '', port: 8083, heapMin: '1G', heapSize: '1G', groupId: 'tantor-connect', installDir: '',
  configDir: '', logDir: '', workingDir: '', pluginDir: '',
});

export function heapSizeInMiB(value: string): number | null {
  const match = /^([1-9]\d*)([mMgG])$/.exec(value.trim());
  if (!match) return null;
  const amount = Number(match[1]) * (match[2].toUpperCase() === 'G' ? 1024 : 1);
  return Number.isSafeInteger(amount) ? amount : null;
}

export function validateServiceHeap(min: string, max: string, service: string): string {
  const minMiB = heapSizeInMiB(min);
  const maxMiB = heapSizeInMiB(max);
  if (minMiB === null || maxMiB === null) return `${service} Min Heap and Max Heap must be positive sizes such as 512M or 1G.`;
  return minMiB > maxMiB ? `${service} Min Heap cannot exceed Max Heap.` : '';
}

export function validateServicePath(value: string, label: string): string {
  if (!value.trim()) return `${label} is required.`;
  if (!value.trim().startsWith('/')) return `${label} must be an absolute Linux path.`;
  if (value.split('/').includes('..')) return `${label} cannot contain "..".`;
  if (!/^\/[A-Za-z0-9/_\-.]{1,510}$/.test(value.trim())) return `${label} contains unsupported characters.`;
  return '';
}
