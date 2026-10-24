export type ServiceConnection = { id?: string; connectionName: string; protocol: string; host: string; port: number; isDefault?: boolean };
export type MaskRule = { type: 'MASK' | 'REMOVE' | 'REPLACE'; fields: string[]; fieldsPattern: string; topicKeysPattern: string; topicValuesPattern: string; replacement: string };
export type ConnectionOptions = {
  settings: { metrics: { enabled: boolean; brokerPort: number; controllerPort: number } | null; masking: MaskRule[] };
  schemaRegistry: ServiceConnection[];
  kafkaConnect: ServiceConnection[];
};
export const emptyConnectionOptions = (): ConnectionOptions => ({ settings: { metrics: null, masking: [] }, schemaRegistry: [], kafkaConnect: [] });
export function validateBootstrapServers(value: string): string {
  const endpoints = value.split(',').map(row => row.trim());
  if (endpoints.length > 30 || endpoints.some(row => {
    const match = /^(\[[0-9a-fA-F:]+\]|[a-zA-Z0-9.-]+):(\d+)$/.exec(row);
    return !match || Number(match[2]) < 1 || Number(match[2]) > 65535;
  })) throw new Error('Enter each bootstrap server as host:port. IPv6 uses [address]:port.');
  return [...new Set(endpoints)].join(',');
}
export function validateConnectionOptions(options: ConnectionOptions): void {
  for (const services of [options.schemaRegistry, options.kafkaConnect]) {
    const names = new Set<string>();
    for (const service of services) {
      if (!service.connectionName.trim() || names.has(service.connectionName.trim())) throw new Error('Give each service connection a unique name.');
      names.add(service.connectionName.trim());
      if (!service.host.trim() || service.port < 1 || service.port > 65535 || !Number.isInteger(service.port)) throw new Error('Each service connection needs a hostname and a valid port.');
    }
  }
  for (const rule of options.settings.masking) {
    if (!rule.fields.some(field => field.trim()) && !rule.fieldsPattern.trim()) throw new Error('Each masking rule needs a field name or field-name pattern.');
    if (!rule.topicKeysPattern.trim() && !rule.topicValuesPattern.trim()) throw new Error('Choose topic keys or values for each masking rule.');
  }
}
