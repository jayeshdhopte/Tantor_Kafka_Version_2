export function parseConnectorConfig(text: string, name: string): Record<string, string> {
  let value: unknown;
  try { value = JSON.parse(text); } catch { throw new Error('Enter valid JSON before saving.'); }
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Configuration must be a JSON object.');
  const config = value as Record<string, unknown>;
  if (Object.entries(config).some(([key, item]) => !key.trim() || typeof item !== 'string')) throw new Error('Use non-empty property names and string values for connector settings.');
  if (!String(config['connector.class'] || '').trim()) throw new Error('connector.class is required.');
  if ('name' in config && config.name !== name) throw new Error('The connector name cannot be changed here.');
  return config as Record<string, string>;
}

export function sameConnectorConfig(a: Record<string, string>, b: Record<string, string>): boolean {
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every(key => a[key] === b[key]);
}
