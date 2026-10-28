import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, CheckCircle2, CircleAlert, Loader2, Save, Settings } from 'lucide-react';
import { BootstrapServersInput, ExternalConnectionOptionsForm } from '../components/ExternalConnectionOptionsForm';
import { emptyConnectionOptions, validateBootstrapServers, validateConnectionOptions, type ConnectionOptions } from '../utils/externalConnectionOptions';
import { usePermissions } from '../hooks/usePermissions';
import { getValidToken } from '../services/KeycloakService';
import './ExternalConnectionSettings.css';

export function ExternalConnectionSettings() {
  const { id } = useParams(); const navigate = useNavigate(); const { canManage } = usePermissions();
  const [options, setOptions] = useState<ConnectionOptions>(emptyConnectionOptions);
  const [bootstrap, setBootstrap] = useState(''); const [loading, setLoading] = useState(true);
  const [loadedId, setLoadedId] = useState('');
  const [saving, setSaving] = useState(false); const [error, setError] = useState(''); const [notice, setNotice] = useState('');
  useEffect(() => {
    let active = true;
    void (async () => {
      try {
        const response = await fetch(`/api/v1/ui/external-clusters/${id}/connection-settings`);
        const data = await response.json();
        if (!response.ok) throw new Error(data.message || data.error || 'Could not load connection settings.');
        if (active) { setBootstrap(data.bootstrapServers); setOptions(data); setLoadedId(id || ''); }
      } catch (failure) { if (active) setError(failure instanceof Error ? failure.message : 'Could not load connection settings.'); }
      finally { if (active) setLoading(false); }
    })();
    return () => { active = false; };
  }, [id]);
  const save = async () => {
    setError(''); setNotice(''); setSaving(true);
    try {
      const servers = validateBootstrapServers(bootstrap); validateConnectionOptions(options);
      const token = await getValidToken(); if (!token) throw new Error('Your session has expired. Sign in again.');
      const response = await fetch(`/api/v1/ui/external-clusters/${id}/connection-settings`, { method: 'PUT', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }, body: JSON.stringify({ ...options, bootstrapServers: servers }) });
      const data = await response.json(); if (!response.ok) throw new Error(data.message || data.error || 'Could not save connection settings.');
      setBootstrap(data.bootstrapServers); setOptions(data);
      const unreachable = [...data.schemaRegistry, ...data.kafkaConnect].some((connection: {status?: string}) => connection.status !== 'CONNECTED' && connection.status !== 'ONLINE' && connection.status !== 'HEALTHY');
      setNotice(unreachable ? 'Settings saved. Check service connection status in the Schema Registry and Kafka Connect tabs.' : 'Connection settings saved.');
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'Could not save connection settings.'); }
    finally { setSaving(false); }
  };
  return <main className="external-settings-page">
    <button type="button" className="external-settings-back" onClick={() => navigate('/clusters')}><ArrowLeft size={16} aria-hidden="true" /> Back to clusters</button>
    <header className="external-settings-header"><Settings size={22} aria-hidden="true" /><div>
      <h2>External cluster connection settings</h2>
      <p>These are connections and browsing settings. Use Configuration change for Kafka broker properties.</p>
    </div></header>
    {error && <p className="external-settings-notice error" role="alert"><CircleAlert size={18} aria-hidden="true" /><span>{error}</span></p>}
    {notice && <p className="external-settings-notice success" role="status"><CheckCircle2 size={18} aria-hidden="true" /><span>{notice}</span></p>}
    {loading ? <p className="external-settings-loading" role="status"><Loader2 size={18} aria-hidden="true" className="external-settings-spin" /> Loading connection settings...</p> : loadedId === id && <>
      <BootstrapServersInput value={bootstrap} onChange={setBootstrap} disabled={!canManage || saving} />
      <ExternalConnectionOptionsForm value={options} onChange={setOptions} disabled={!canManage || saving} />
      {canManage && <footer className="external-settings-footer"><button type="button" className="external-settings-save" disabled={saving || !bootstrap} aria-busy={saving} onClick={() => void save()}>{saving ? <Loader2 size={16} aria-hidden="true" className="external-settings-spin" /> : <Save size={16} aria-hidden="true" />}{saving ? 'Saving and verifying...' : 'Save connection settings'}</button></footer>}
    </>}
  </main>;
}
