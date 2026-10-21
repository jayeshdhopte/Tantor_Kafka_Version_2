import { lazy, Suspense, useEffect, useState } from 'react';
import { Copy, Download } from 'lucide-react';
import { confirmAction } from './confirmUtils';
import { downloadConnectFile } from '../utils/kafkaConnect';
import { parseConnectorConfig, sameConnectorConfig } from '../utils/connectorConfig';

const JsonEditor = lazy(() => import('./ConnectorJsonEditor'));
export function ConnectorConfigPanel({ config, name, url, canManage, busy, onDirty, onSaved }: {
  config: Record<string, string>; name: string; url: string; canManage: boolean; busy: boolean;
  onDirty: (dirty: boolean) => void; onSaved: () => void;
}) {
  const original = JSON.stringify(config, null, 2);
  const [draft, setDraft] = useState(original);
  const [view, setView] = useState('json');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  let parsed: Record<string, string> | undefined;
  let validation = '';
  try { parsed = parseConnectorConfig(draft, name); } catch (reason) { validation = (reason as Error).message; }
  const dirty = parsed ? !sameConnectorConfig(parsed, config) : draft !== original;
  useEffect(() => { onDirty(dirty || saving); return () => onDirty(false); }, [dirty, saving, onDirty]);
  useEffect(() => {
    if (!dirty) return;
    const protect = (event: BeforeUnloadEvent) => { event.preventDefault(); };
    window.addEventListener('beforeunload', protect);
    return () => window.removeEventListener('beforeunload', protect);
  }, [dirty]);
  const save = async () => {
    if (!parsed || !dirty || !canManage || busy || saving) return;
    setSaving(true); setError(''); setNotice('');
    try {
      if (!(await confirmAction(`Save configuration for ${name}? Kafka Connect may restart its tasks and briefly interrupt processing.`, { title: 'Update connector configuration', confirmLabel: 'Save configuration' }))) return;
      const current = await fetch(url);
      const currentConfig = await current.json().catch(() => ({}));
      if (!current.ok) throw new Error(currentConfig.message || 'Unable to verify the current configuration.');
      if (!sameConnectorConfig(config, currentConfig)) throw new Error('Configuration changed since this page was loaded. Copy your edits, discard them, and refresh before saving.');
      const response = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(parsed) });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.message || 'Kafka Connect rejected the configuration. Review the settings and retry.');
      onSaved();
    } catch (reason) { setError(reason instanceof Error ? reason.message : 'Unable to save configuration.'); }
    finally { setSaving(false); }
  };
  return <div className="ds-kc-config-layout">
    <div className="ds-kc-config-toolbar"><div className="ds-inline-actions">
      <button className="ds-button outlined" aria-pressed={view === 'json'} onClick={() => setView('json')}>JSON</button>
      <button className="ds-button outlined" aria-pressed={view === 'properties'} onClick={() => setView('properties')}>Properties</button>
    </div><div className="ds-inline-actions">
      <button className="ds-button outlined" onClick={() => navigator.clipboard.writeText(draft).then(() => setNotice('Configuration copied.')).catch(() => setError('Unable to copy configuration.'))}><Copy size={15} /> Copy JSON</button>
      <button className="ds-button outlined" onClick={() => downloadConnectFile('connector-config.json', draft, 'application/json')}><Download size={15} /> Download JSON</button>
    </div></div>
    {error && <div className="ds-alert" role="alert">{error}</div>}
    {notice && <p role="status">{notice}</p>}
    <div className="ds-kc-config-content">
    <div hidden={view !== 'json'}><Suspense fallback={<p>Loading JSON editor...</p>}><JsonEditor value={draft} readOnly={!canManage || saving || busy} onChange={setDraft} /></Suspense></div>
    {view === 'properties' && <table className="ds-table ds-kc-table"><thead><tr><th>Property</th><th>Value</th></tr></thead><tbody>{Object.entries(parsed || config).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => <tr key={key}><td>{key}</td><td className="ds-kc-config-value">{value}</td></tr>)}</tbody></table>}
    <div className="ds-kc-editor-footer">
      <span>{!canManage ? 'Read-only configuration.' : dirty ? validation || 'Unsaved changes. Kafka Connect validates settings when saved.' : 'Edit JSON to update connector settings.'}</span>
      {canManage && <div className="ds-inline-actions"><button className="ds-button outlined" disabled={!dirty || saving || busy} onClick={async () => {
        if (await confirmAction('Discard unsaved configuration edits?', { title: 'Discard changes', confirmLabel: 'Discard' })) { setDraft(original); setError(''); }
      }}>Discard changes</button><button className="ds-button" disabled={!dirty || !!validation || saving || busy} onClick={() => void save()}>{saving ? 'Saving...' : 'Submit'}</button></div>}
    </div>
    </div>
  </div>;
}
