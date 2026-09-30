export type SchemaRegistryConfig = {
  artifactId: string; port: number; heapSize: string; installDir: string; configDir: string;
  logDir: string; workingDir: string; compatibility: string;
};

export type KafkaConnectConfig = {
  artifactId: string; port: number; heapSize: string; groupId: string; installDir: string;
  configDir: string; logDir: string; workingDir: string; pluginDir: string;
};

export const defaultSchemaRegistryConfig = (): SchemaRegistryConfig => ({
  artifactId: '', port: 8081, heapSize: '1G', installDir: '/opt/tantor/schema-registry',
  configDir: '/opt/tantor/schema-registry/etc/schema-registry', logDir: '/var/log/tantor/schema-registry',
  workingDir: '/var/lib/tantor/schema-registry', compatibility: 'BACKWARD',
});

export const defaultKafkaConnectConfig = (): KafkaConnectConfig => ({
  artifactId: '', port: 8083, heapSize: '1G', groupId: 'tantor-connect', installDir: '/opt/tantor/kafka-connect',
  configDir: '/opt/tantor/kafka-connect/config', logDir: '/var/log/tantor/kafka-connect',
  workingDir: '/var/lib/tantor/kafka-connect', pluginDir: '/opt/tantor/kafka-connect/plugins',
});

export function validateServicePath(value: string, label: string): string {
  if (!value.trim()) return `${label} is required.`;
  if (!value.trim().startsWith('/')) return `${label} must be an absolute Linux path.`;
  if (value.split('/').includes('..')) return `${label} cannot contain "..".`;
  if (!/^\/[A-Za-z0-9/_\-.]{1,510}$/.test(value.trim())) return `${label} contains unsupported characters.`;
  return '';
}
