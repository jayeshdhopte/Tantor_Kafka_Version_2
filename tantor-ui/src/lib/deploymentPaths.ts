/** New deployments keep the client's chosen mount and add the Tantor namespace. */
export function brandedKafkaInstallBase(value: string): string {
  let base = (value.trim() || '/opt').replace(/\/+/g, '/').replace(/\/+$/, '') || '/';
  const leaf = base.slice(base.lastIndexOf('/') + 1);
  if (leaf === 'kafka' || leaf.startsWith('kafka_')) base = base.slice(0, base.lastIndexOf('/')) || '/';
  if (base.split('/').at(-1) !== 'tantor') base = `${base === '/' ? '' : base}/tantor`;
  return base;
}
