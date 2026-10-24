import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { BootstrapServersInput, ExternalConnectionOptionsForm } from '../components/ExternalConnectionOptionsForm';
import { emptyConnectionOptions, validateBootstrapServers, validateConnectionOptions, type ConnectionOptions } from '../utils/externalConnectionOptions';
import { usePermissions } from '../hooks/usePermissions';
import { getValidToken } from '../services/KeycloakService';

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
  return <main className="connection-options">
    <button type="button" onClick={() => navigate('/clusters')}>Back to clusters</button>
    <h2>External cluster connection settings</h2>
    <p>These are connections and browsing settings. Use Configuration change for Kafka broker properties.</p>
    {error && <p role="alert">{error}</p>}{notice && <p role="status">{notice}</p>}
    {loading ? <p>Loading connection settings...</p> : loadedId === id && <>
      <BootstrapServersInput value={bootstrap} onChange={setBootstrap} disabled={!canManage || saving} />
      <ExternalConnectionOptionsForm value={options} onChange={setOptions} disabled={!canManage || saving} />
      {canManage && <button type="button" className="ds-button outlined" disabled={saving || !bootstrap} onClick={() => void save()}>{saving ? 'Saving and verifying...' : 'Save connection settings'}</button>}
    </>}
  </main>;
}
