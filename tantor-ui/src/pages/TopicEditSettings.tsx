import { useEffect, useMemo, useState, type FormEvent } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Plus, Trash2, Undo2 } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import './TopicEditSettings.css';

interface TopicConfig {
  name: string;
  value: string | null;
  defaultValue: string | null;
  source: string;
  readOnly: boolean;
  sensitive: boolean;
}

const commonSettings = [
  { name: 'cleanup.policy', label: 'Cleanup policy' },
  { name: 'min.insync.replicas', label: 'Minimum in-sync replicas' },
  { name: 'retention.ms', label: 'Time to retain data (milliseconds)' },
  { name: 'retention.bytes', label: 'Max partition size (bytes)' },
  { name: 'max.message.bytes', label: 'Maximum message size (bytes)' }
];
const commonNames = new Set(commonSettings.map(setting => setting.name));

async function responseError(response: Response) {
  const body = await response.json().catch(() => null);
  return body?.message || body?.error || `Request failed (HTTP ${response.status})`;
}

export function TopicEditSettings() {
  const { id, topicName: encodedTopicName } = useParams<{ id: string; topicName: string }>();
  const topicName = encodedTopicName ? decodeURIComponent(encodedTopicName) : '';
  const navigate = useNavigate();
  const { canManage } = usePermissions();
  const baseUrl = `/api/v1/clusters/${id}/topics/${encodeURIComponent(topicName)}`;
  const topicUrl = `/clusters/${id}/topics/${encodeURIComponent(topicName)}?tab=settings`;

  const [configs, setConfigs] = useState<TopicConfig[]>([]);
  const [values, setValues] = useState<Record<string, string>>({});
  const [customNames, setCustomNames] = useState<string[]>([]);
  const [resetNames, setResetNames] = useState<string[]>([]);
  const [selectedName, setSelectedName] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!canManage) return;
    const controller = new AbortController();
    fetch(`${baseUrl}/configs`, { signal: controller.signal })
      .then(async response => {
        if (!response.ok) throw new Error(await responseError(response));
        return response.json() as Promise<TopicConfig[]>;
      })
      .then(rows => {
        setConfigs(rows);
        setValues(Object.fromEntries(rows.map(row => [row.name, row.value ?? ''])));
        setCustomNames(rows.filter(row => row.source === 'DYNAMIC_TOPIC_CONFIG' && !commonNames.has(row.name) && !row.readOnly && !row.sensitive).map(row => row.name));
        setLoading(false);
      })
      .catch(requestError => {
        if (controller.signal.aborted) return;
        setError(requestError instanceof Error ? requestError.message : 'Failed to load topic settings');
        setLoading(false);
      });
    return () => controller.abort();
  }, [baseUrl, canManage]);

  const byName = useMemo(() => new Map(configs.map(config => [config.name, config])), [configs]);
  const availableCustom = configs.filter(config => !commonNames.has(config.name) && !config.readOnly && !config.sensitive && !customNames.includes(config.name));
  const changedValues = Object.fromEntries(
    [...commonSettings.map(setting => setting.name), ...customNames]
      .filter(name => !resetNames.includes(name) && byName.has(name) && values[name] !== (byName.get(name)?.value ?? ''))
      .map(name => [name, values[name]])
  );
  const hasChanges = Object.keys(changedValues).length > 0 || resetNames.length > 0;

  const removeCustom = (name: string) => {
    setCustomNames(current => current.filter(item => item !== name));
    if (byName.get(name)?.source === 'DYNAMIC_TOPIC_CONFIG') {
      setResetNames(current => [...current, name]);
    }
  };

  const addCustom = () => {
    if (!selectedName) return;
    setCustomNames(current => [...current, selectedName]);
    setResetNames(current => current.filter(name => name !== selectedName));
    setSelectedName('');
  };

  const updateTopic = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canManage || !hasChanges || saving) return;
    setSaving(true);
    setError(null);
    try {
      const response = await fetch(`${baseUrl}/configs`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ values: changedValues, resets: resetNames })
      });
      if (!response.ok) throw new Error(await responseError(response));
      navigate(topicUrl);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Failed to update topic settings');
    } finally {
      setSaving(false);
    }
  };

  return (
    <section className="topic-edit-page animate-fade-in">
      <button type="button" className="topic-edit-back" onClick={() => navigate(topicUrl)}><ArrowLeft size={16} /> {topicName}</button>
      <h2>Edit settings</h2>
      {!canManage ? <p>You do not have permission to edit topic settings.</p> : loading ? <p>Loading settings...</p> : configs.length === 0 ? <p role="alert">{error || 'No topic settings are available.'}</p> : (
        <form onSubmit={updateTopic}>
          <div className="topic-edit-section">
            <h3>Topic settings</h3>
            <div className="topic-edit-grid">
              {commonSettings.map(setting => {
                const config = byName.get(setting.name);
                if (!config) return null;
                const unavailable = config.readOnly || config.sensitive;
                const resetting = resetNames.includes(setting.name);
                return (
                  <div className="topic-edit-field" key={setting.name}>
                    <label htmlFor={`setting-${setting.name}`}>{setting.label}</label>
                    <div className="topic-edit-input-row">
                      {setting.name === 'cleanup.policy' ? (
                        <select id={`setting-${setting.name}`} value={values[setting.name] ?? ''} disabled={unavailable || resetting || saving} onChange={event => setValues(current => ({ ...current, [setting.name]: event.target.value }))}>
                          {!['delete', 'compact', 'compact,delete'].includes(values[setting.name]) && <option value={values[setting.name]}>{values[setting.name]}</option>}
                          <option value="delete">Delete</option>
                          <option value="compact">Compact</option>
                          <option value="compact,delete">Compact, delete</option>
                        </select>
                      ) : (
                        <input id={`setting-${setting.name}`} value={values[setting.name] ?? ''} disabled={unavailable || resetting || saving} inputMode="numeric" onChange={event => setValues(current => ({ ...current, [setting.name]: event.target.value }))} />
                      )}
                      {!unavailable && (resetting ? (
                        <button type="button" className="topic-edit-icon" title="Undo reset" aria-label={`Undo reset for ${setting.name}`} onClick={() => setResetNames(current => current.filter(name => name !== setting.name))}><Undo2 size={17} /></button>
                      ) : config.source === 'DYNAMIC_TOPIC_CONFIG' ? (
                        <button type="button" className="topic-edit-icon" title="Remove topic override" aria-label={`Remove override for ${setting.name}`} onClick={() => setResetNames(current => [...current, setting.name])}><Trash2 size={17} /></button>
                      ) : null)}
                    </div>
                    {resetting && <span className="topic-edit-note">Topic override will be removed.</span>}
                  </div>
                );
              })}
            </div>
          </div>

          <div className="topic-edit-section">
            <h3>Custom parameters</h3>
            {customNames.map(name => (
              <div className="topic-edit-custom-row" key={name}>
                <label htmlFor={`setting-${name}`}>{name}</label>
                <input id={`setting-${name}`} value={values[name] ?? ''} disabled={saving} onChange={event => setValues(current => ({ ...current, [name]: event.target.value }))} />
                <button type="button" className="topic-edit-icon" title="Remove topic override" aria-label={`Remove ${name}`} onClick={() => removeCustom(name)}><Trash2 size={17} /></button>
              </div>
            ))}
            {availableCustom.length > 0 && <div className="topic-edit-add-row">
              <select aria-label="Custom parameter" value={selectedName} onChange={event => setSelectedName(event.target.value)}>
                <option value="">Select a parameter</option>
                {availableCustom.map(config => <option value={config.name} key={config.name}>{config.name}</option>)}
              </select>
              <button type="button" className="topic-edit-add" disabled={!selectedName || saving} onClick={addCustom}><Plus size={16} /> Add custom parameter</button>
            </div>}
          </div>

          {error && <p role="alert" className="topic-edit-error">{error}</p>}
          <div className="topic-edit-actions">
            <button type="button" className="topic-edit-cancel" onClick={() => navigate(topicUrl)}>Cancel</button>
            <button type="submit" className="topic-edit-save" disabled={!hasChanges || saving}>{saving ? 'Updating...' : 'Update topic'}</button>
          </div>
        </form>
      )}
    </section>
  );
}
