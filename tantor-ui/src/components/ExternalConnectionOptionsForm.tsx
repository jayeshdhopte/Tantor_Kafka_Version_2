import { Plus, Trash2 } from 'lucide-react';
import type { ConnectionOptions, ServiceConnection, MaskRule } from '../utils/externalConnectionOptions';
import './ExternalConnectionOptionsForm.css';

export function BootstrapServersInput({ value, onChange, disabled = false }: { value: string; onChange: (value: string) => void; disabled?: boolean }) {
  const rows = value.split(',');
  const update = (index: number, host: string, port: string) => {
    const address = host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
    onChange(rows.map((row, position) => position === index ? `${address}:${port}` : row).join(','));
  };
  return <div className="connection-options bootstrap-options"><label>Bootstrap servers</label>
    {rows.map((row, index) => {
      const match = /^(\[[^\]]*\]|[^:]*):(.*)$/.exec(row);
      const host = match ? match[1].replace(/^\[|\]$/g, '') : row;
      const port = match?.[2] ?? '';
      return <div className="connection-row bootstrap-server-row" key={index}>
        <label>IP / Hostname<input aria-label={`Bootstrap server ${index + 1} IP or hostname`} placeholder="broker.example.com" value={host} disabled={disabled}
          onChange={event => update(index, event.target.value, port)} /></label>
        <label className="bootstrap-port">Port<input aria-label={`Bootstrap server ${index + 1} port`} type="number" min="1" max="65535" step="1" placeholder="9092" value={port} disabled={disabled}
          onChange={event => update(index, host, event.target.value)} /></label>
        <button type="button" className="ds-button outlined" aria-label={`Remove bootstrap server ${index + 1}`} disabled={disabled || rows.length === 1}
          onClick={() => onChange(rows.filter((_, position) => position !== index).join(','))}><Trash2 size={16} /></button>
      </div>;
    })}
    <button type="button" className="ds-button outlined" disabled={disabled || rows.length >= 30} onClick={() => onChange(`${value},`)}><Plus size={16} /> Add bootstrap server</button>
  </div>;
}

export function ExternalConnectionOptionsForm({ value, onChange, disabled = false }: { value: ConnectionOptions; onChange: (value: ConnectionOptions) => void; disabled?: boolean }) {
  const updateServices = (kind: 'schemaRegistry' | 'kafkaConnect', index: number, patch: Partial<ServiceConnection>) =>
    onChange({ ...value, [kind]: value[kind].map((service, position) => position === index ? { ...service, ...patch } : service) });
  const updateMask = (index: number, patch: Partial<MaskRule>) => onChange({ ...value, settings: { ...value.settings,
    masking: value.settings.masking.map((rule, position) => position === index ? { ...rule, ...patch } : rule) } });
  const metrics = value.settings.metrics;
  return <fieldset className="connection-options" disabled={disabled}>
    <legend>Cluster connections and browsing</legend>
    <p>Connect to existing services. These settings do not install or restart Kafka, Schema Registry or Connect.</p>
    {(['schemaRegistry', 'kafkaConnect'] as const).map(kind => <details key={kind} open={value[kind].length > 0}>
      <summary>{kind === 'schemaRegistry' ? 'Schema Registry' : 'Kafka Connect'} <span>{value[kind].length} connection{value[kind].length === 1 ? '' : 's'}</span></summary>
      {value[kind].map((service, index) => <div className="connection-service-row" key={service.id || index}>
        <label>Name<input aria-label={`${kind} connection ${index + 1} name`} value={service.connectionName} onChange={event => updateServices(kind, index, { connectionName: event.target.value })} /></label>
        <label>Protocol<select value={service.protocol.toLowerCase()} onChange={event => updateServices(kind, index, { protocol: event.target.value })}><option value="http">HTTP</option><option value="https">HTTPS</option></select></label>
        <label>Hostname<input value={service.host} placeholder="service.example.com" onChange={event => updateServices(kind, index, { host: event.target.value })} /></label>
        <label>Port<input type="number" min="1" max="65535" value={service.port} onChange={event => updateServices(kind, index, { port: Number(event.target.value) })} /></label>
        {!service.id && <button type="button" className="ds-button outlined" aria-label={`Remove ${kind} connection ${index + 1}`} onClick={() => onChange({ ...value, [kind]: value[kind].filter((_, position) => position !== index) })}><Trash2 size={16} /></button>}
      </div>)}
      <button type="button" className="ds-button outlined" onClick={() => onChange({ ...value, [kind]: [...value[kind], { connectionName: '', protocol: 'http', host: '', port: kind === 'schemaRegistry' ? 8081 : 8083 }] })}><Plus size={16} /> Add {kind === 'schemaRegistry' ? 'Schema Registry' : 'Kafka Connect'}</button>
      <p>HTTPS reuses the cluster truststore when available. Manage certificates and remove saved connections from the service tab.</p>
    </details>)}
    <details open={metrics !== null}><summary>Metrics</summary>
      <label>Exporter configuration<select value={metrics === null ? 'discovery' : metrics.enabled ? 'custom' : 'disabled'} onChange={event => onChange({ ...value, settings: { ...value.settings, metrics: event.target.value === 'discovery' ? null : { enabled: event.target.value === 'custom', brokerPort: 7071, controllerPort: 7072 } } })}>
        <option value="discovery">Use discovered exporter ports</option><option value="custom">Custom Prometheus / JMX Exporter ports</option><option value="disabled">Disable exporter scraping</option>
      </select></label>
      {metrics?.enabled && <div className="connection-row">
        <label>Broker exporter port<input type="number" min="1" max="65535" value={metrics.brokerPort} onChange={event => onChange({ ...value, settings: { ...value.settings, metrics: { ...metrics, brokerPort: Number(event.target.value) } } })} /></label>
        <label>Controller exporter port<input type="number" min="1" max="65535" value={metrics.controllerPort} onChange={event => onChange({ ...value, settings: { ...value.settings, metrics: { ...metrics, controllerPort: Number(event.target.value) } } })} /></label>
      </div>}
      <p>Uses existing HTTP Prometheus/JMX Exporter endpoints on discovered nodes. Native JMX/RMI is not used.</p>
    </details>
    <details open={value.settings.masking.length > 0}><summary>Masking <span>{value.settings.masking.length} rule{value.settings.masking.length === 1 ? '' : 's'}</span></summary>
      <p>Redacts matching JSON fields in topic-message responses and searches. Stored Kafka records stay unchanged. Topic patterns use Java regular expressions; blank means no topics.</p>
      {value.settings.masking.map((rule, index) => <div className="connection-mask-rule" key={index}>
        <div className="connection-row"><label>Masking type<select value={rule.type} onChange={event => updateMask(index, { type: event.target.value as MaskRule['type'] })}><option>MASK</option><option>REMOVE</option><option>REPLACE</option></select></label>
          <button type="button" className="ds-button outlined" aria-label={`Remove masking rule ${index + 1}`} onClick={() => onChange({ ...value, settings: { ...value.settings, masking: value.settings.masking.filter((_, position) => position !== index) } })}><Trash2 size={16} /></button></div>
        <label>Field names (comma separated)<input value={rule.fields.join(',')} placeholder="password,email" onChange={event => updateMask(index, { fields: event.target.value.split(',') })} /></label>
        <label>Field-name pattern<input value={rule.fieldsPattern} placeholder=".*[Ss]ecret.*" onChange={event => updateMask(index, { fieldsPattern: event.target.value })} /></label>
        <div className="connection-row"><label>Topic keys pattern<input value={rule.topicKeysPattern} placeholder="Leave blank to skip keys" onChange={event => updateMask(index, { topicKeysPattern: event.target.value })} /></label>
          <label>Topic values pattern<input value={rule.topicValuesPattern} placeholder=".*" onChange={event => updateMask(index, { topicValuesPattern: event.target.value })} /></label></div>
        {rule.type === 'REPLACE' && <label>Replacement<input value={rule.replacement} onChange={event => updateMask(index, { replacement: event.target.value })} /></label>}
      </div>)}
      <button type="button" className="ds-button outlined" onClick={() => onChange({ ...value, settings: { ...value.settings, masking: [...value.settings.masking, { type: 'MASK', fields: [], fieldsPattern: '', topicKeysPattern: '', topicValuesPattern: '.*', replacement: '***' }] } })}><Plus size={16} /> Add masking rule</button>
    </details>
  </fieldset>;
}
