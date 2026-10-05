import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Check, Loader2, Plus, RefreshCw, Settings, ShieldCheck, Trash2 } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import './ConfigChange.css';

interface Service { hostId: string; hostAddress: string; role: string; nodeId: number; serviceName?: string }
interface ConfigPayload { serviceTopology?: Service[]; dynamicConfigs?: Record<string, Record<string, { value: string | null; isReadOnly: boolean; source?: string }>>; staticConfigs?: { configFiles?: Array<{ nodeId?: number; properties?: Record<string, unknown> }> } }
interface PreviewTarget { nodeId: number; oldValue: string | null; newValue: string }
interface Preview { classification: string; key: string; targets: PreviewTarget[]; healthy: boolean; healthFailures: Record<string, string>; clusterLocked: boolean }
const protectedKeys = new Set(['node.id', 'broker.id', 'process.roles', 'log.dirs', 'metadata.log.dir', 'listeners', 'advertised.listeners', 'controller.quorum.voters', 'controller.quorum.bootstrap.servers', 'inter.broker.listener.name', 'cluster.id', 'zookeeper.connect']);

export function ConfigChange() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { canManage } = usePermissions();
  const [payload, setPayload] = useState<ConfigPayload | null>(null);
  const [clusterName, setClusterName] = useState('Cluster');
  const [clusterMode, setClusterMode] = useState('INTERNAL');
  const [role, setRole] = useState<'BROKER' | 'CONTROLLER'>('BROKER');
  const [scope, setScope] = useState<'ALL' | 'SPECIFIC'>('ALL');
  const [selected, setSelected] = useState<number[]>([]);
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [draftValues, setDraftValues] = useState<Record<string, string>>({});
  const [removedKeys, setRemovedKeys] = useState<string[]>([]);
  const [addedKeys, setAddedKeys] = useState<string[]>([]);
  const [propertyToAdd, setPropertyToAdd] = useState('');
  const [staticKey, setStaticKey] = useState('');
  const [staticValue, setStaticValue] = useState('');
  const [fileValues, setFileValues] = useState<Record<string, Record<string, string>> | null>(null);
  const [staticLoading, setStaticLoading] = useState(false);
  const [staticReadError, setStaticReadError] = useState('');
  const [staticDraftValues, setStaticDraftValues] = useState<Record<string, string>>({});
  const [staticRemovedKeys, setStaticRemovedKeys] = useState<string[]>([]);
  const [staticAddedKeys, setStaticAddedKeys] = useState<string[]>([]);
  const [staticAddKey, setStaticAddKey] = useState('');
  const [staticPreview, setStaticPreview] = useState<Preview | null>(null);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');

  const refresh = useCallback(async () => {
    if (!id) return;
    setBusy('load'); setError(''); setPreview(null);
    try {
      const [configResponse, clusterResponse] = await Promise.all([
        fetch(`/api/v1/clusters/${id}/config`), fetch(`/api/v1/ui/clusters/${id}`),
      ]);
      if (!configResponse.ok || !clusterResponse.ok) throw new Error('Unable to load current cluster configuration.');
      const data = await configResponse.json() as ConfigPayload;
      const cluster = await clusterResponse.json() as { name: string; mode: string };
      setPayload(data); setClusterName(cluster.name); setClusterMode(cluster.mode);
    } catch (reason) { setError(reason instanceof Error ? reason.message : 'Unable to load configuration.'); }
    finally { setBusy(''); }
  }, [id]);
  useEffect(() => { void Promise.resolve().then(refresh); }, [refresh]);

  const services = useMemo(() => (payload?.serviceTopology || []).filter(service => service.role.toLowerCase().includes(role.toLowerCase())), [payload, role]);
  const ids = useMemo(() => Array.from(new Set(services.map(service => service.nodeId))).sort((a, b) => a - b), [services]);
  const targetIds = scope === 'ALL' ? ids : selected;
  const eligibleStaticTargets = useMemo(() => services.filter(service => targetIds.includes(service.nodeId)
    && !service.role.toLowerCase().includes('broker + controller')
    && !service.role.toLowerCase().includes('broker_controller')), [services, targetIds]);
  const openStaticTargets = () => navigate(`/clusters/${id}/config?targets=${eligibleStaticTargets.map(service => service.nodeId).join(',')}`);
  const staticTargetIds = useMemo(() => eligibleStaticTargets.map(service => service.nodeId), [eligibleStaticTargets]);
  const suggestedStaticKeys = Array.from(new Set((payload?.staticConfigs?.configFiles || [])
    .filter(file => file.nodeId !== undefined && staticTargetIds.includes(file.nodeId))
    .flatMap(file => Object.keys(file.properties || {}))
    .filter(name => !protectedKeys.has(name) && !/(password|secret|token|jaas|credential|keystore|truststore)/i.test(name)))).sort();
  const allKeys = useMemo(() => {
    const brokerIds = scope === 'ALL' ? ids : selected;
    const configs = payload?.dynamicConfigs || {};
    if (role !== 'BROKER' || brokerIds.length === 0) return [];
    const first = configs[String(brokerIds[0])] || {};
    return Object.keys(first).filter(name => brokerIds.every(brokerId => {
      const entry = configs[String(brokerId)]?.[name];
      return entry && !entry.isReadOnly && !protectedKeys.has(name) && !/(password|secret|token|jaas|credential|keystore|truststore)/i.test(name);
    })).sort();
  }, [payload, role, scope, ids, selected]);
  const visibleKeys = allKeys.filter(name => !removedKeys.includes(name) && (addedKeys.includes(name)
    || targetIds.some(nodeId => {
      const source = payload?.dynamicConfigs?.[String(nodeId)]?.[name]?.source;
      return !source || source !== 'DEFAULT_CONFIG';
    })));
  const addableKeys = allKeys.filter(name => !visibleKeys.includes(name));
  const brokerValues = (name: string) => targetIds.map(nodeId => ({
    nodeId,
    value: payload?.dynamicConfigs?.[String(nodeId)]?.[name]?.value ?? null,
  }));
  useEffect(() => {
    if (!id || !payload || clusterMode === 'EXTERNAL' || staticTargetIds.length === 0) return;
    const controller = new AbortController();
    Promise.resolve().then(() => { setStaticLoading(true); setStaticReadError(''); setFileValues(null); setStaticPreview(null); });
    void (async () => {
      try {
        const response = await fetch(`/api/v1/clusters/${id}/config/change/file-values`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ role, targetNodeIds: staticTargetIds }), signal: controller.signal,
        });
        const result = await response.json() as { values?: Record<string, Record<string, string>>; message?: string };
        if (!response.ok || !result.values) throw new Error(result.message || 'Unable to read active Kafka files.');
        setFileValues(result.values);
      } catch (reason) {
        if (!controller.signal.aborted) setStaticReadError(reason instanceof Error ? reason.message : 'Unable to read active Kafka files.');
      } finally { if (!controller.signal.aborted) setStaticLoading(false); }
    })();
    return () => controller.abort();
  }, [id, payload, clusterMode, role, staticTargetIds]);
  const fileKeys = Array.from(new Set(Object.values(fileValues || {}).flatMap(properties => Object.keys(properties))))
    .filter(name => !protectedKeys.has(name) && !/(password|secret|token|jaas|credential|keystore|truststore)/i.test(name))
    .filter(name => role !== 'BROKER' || staticTargetIds.every(nodeId =>
      payload?.dynamicConfigs?.[String(nodeId)]?.[name]?.isReadOnly === true)).sort();
  const staticVisibleKeys = Array.from(new Set([...fileKeys, ...staticAddedKeys]))
    .filter(name => !staticRemovedKeys.includes(name)).sort();

  const changeRequest = { role, scope, targetNodeIds: scope === 'ALL' ? ids : selected, key, value };
  const requestPreview = async (candidateKey = key, candidateValue = value) => {
    if (!id) return;
    setBusy('preview'); setError(''); setPreview(null);
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/change/preview`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...changeRequest, key: candidateKey, value: candidateValue }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Preview failed.');
      setKey(candidateKey); setValue(candidateValue); setPreview(result as Preview);
    } catch (reason) { setError(reason instanceof Error ? reason.message : 'Preview failed.'); }
    finally { setBusy(''); }
  };
  const apply = async () => {
    if (!id || !preview || !canManage) return;
    setBusy('apply'); setError('');
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/change/apply`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(changeRequest),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Unable to queue configuration change.');
      navigate(`/jobs/${result.jobId}`);
    } catch (reason) { setError(reason instanceof Error ? reason.message : 'Unable to queue configuration change.'); }
    finally { setBusy(''); }
  };
  const staticRequest = { role, scope: 'SPECIFIC', targetNodeIds: staticTargetIds, key: staticKey.trim(), value: staticValue };
  const reviewStatic = async (candidateKey = staticKey, candidateValue = staticValue) => {
    if (!id) return;
    setBusy('static-preview'); setError(''); setStaticPreview(null);
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/change/preview`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...staticRequest, key: candidateKey, value: candidateValue }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Static change preview failed.');
      if (result.classification !== 'ROLLING_RESTART_REQUIRED') throw new Error('Kafka classifies this property as dynamic. Use the broker dynamic change form.');
      setStaticKey(candidateKey); setStaticValue(candidateValue); setStaticPreview(result as Preview);
    } catch (reason) { setError(reason instanceof Error ? reason.message : 'Static change preview failed.'); }
    finally { setBusy(''); }
  };
  const applyStatic = async () => {
    if (!id || !staticPreview || !canManage) return;
    setBusy('static-apply'); setError('');
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/change/apply`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(staticRequest),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Unable to queue rolling configuration change.');
      navigate(`/jobs/${result.jobId}`);
    } catch (reason) { setError(reason instanceof Error ? reason.message : 'Unable to queue rolling configuration change.'); }
    finally { setBusy(''); }
  };
  const toggle = (nodeId: number) => { setSelected(current => current.includes(nodeId) ? current.filter(id => id !== nodeId) : [...current, nodeId]); setPreview(null); setStaticPreview(null); };
  const updateRole = (next: 'BROKER' | 'CONTROLLER') => { setRole(next); setSelected([]); setKey(''); setPreview(null); setStaticPreview(null); setError(''); };
  const staticControls = () => clusterMode === 'EXTERNAL'
    ? <button className="cc-primary" disabled={eligibleStaticTargets.length === 0} onClick={openStaticTargets}>Configure selected {role.toLowerCase()}s</button>
    : <>
      {targetIds.length !== staticTargetIds.length && <p className="cc-error">Combined broker/controller processes are excluded from role-only rolling changes.</p>}
      {staticLoading && <p className="cc-loading"><Loader2 className="spin" size={16} /> Reading active {role.toLowerCase()} properties from selected nodes…</p>}
      {staticReadError && <p className="cc-error">{staticReadError} <button type="button" onClick={() => void refresh()}>Retry</button></p>}
      {fileValues && <>
        <div className="cc-table-wrap"><table className="cc-property-table">
          <thead><tr><th>Property</th><th>Current value in file</th><th>New value</th><th>Actions</th></tr></thead>
          <tbody>{staticVisibleKeys.map(name => <tr key={name} className={staticPreview?.key === name ? 'cc-reviewed-row' : ''}>
            <td><code>{name}</code></td>
            <td><div className="cc-value-list">{staticTargetIds.map(nodeId => {
              const current = fileValues[String(nodeId)]?.[name];
              return <div key={nodeId}><span>Node {nodeId}</span><code>{current === undefined ? 'Unset' : current === '' ? '(Empty)' : current}</code></div>;
            })}</div></td>
            <td><input aria-label={`New value for ${name}`} value={staticDraftValues[name] ?? ''} disabled={!canManage || !!busy} onChange={event => { setStaticDraftValues(current => ({ ...current, [name]: event.target.value })); if (staticKey === name) setStaticPreview(null); }} placeholder="Enter new value" /></td>
            <td><div className="cc-row-actions"><button type="button" title={`Review ${name} and run precheck`} aria-label={`Review ${name} and run precheck`} disabled={!canManage || !!busy} onClick={() => void reviewStatic(name, staticDraftValues[name] ?? '')}><Check size={17} /></button><button type="button" title={`Remove ${name} from this view`} aria-label={`Remove ${name} from this view`} disabled={!canManage || !!busy} onClick={() => { setStaticRemovedKeys(current => [...current, name]); setStaticDraftValues(current => { const next = { ...current }; delete next[name]; return next; }); if (staticKey === name) setStaticPreview(null); }}><Trash2 size={16} /></button></div></td>
          </tr>)}</tbody>
        </table></div>
        {staticVisibleKeys.length === 0 && <p className="cc-empty-properties">No restart-required properties were found in the selected active files. Add a property below.</p>}
        {canManage && <div className="cc-add-property"><input list="cc-static-keys" aria-label="Property to add" value={staticAddKey} disabled={!!busy} onChange={event => setStaticAddKey(event.target.value)} placeholder="Kafka property name" /><datalist id="cc-static-keys">{suggestedStaticKeys.filter(name => !staticVisibleKeys.includes(name)).map(name => <option key={name} value={name} />)}</datalist><button type="button" disabled={!/^[A-Za-z][A-Za-z0-9._-]{0,127}$/.test(staticAddKey.trim()) || staticVisibleKeys.includes(staticAddKey.trim()) || protectedKeys.has(staticAddKey.trim()) || /(password|secret|token|jaas|credential|keystore|truststore)/i.test(staticAddKey) || !!busy} onClick={() => { const name = staticAddKey.trim(); setStaticAddedKeys(current => [...current, name]); setStaticRemovedKeys(current => current.filter(key => key !== name)); setStaticAddKey(''); }}><Plus size={16} /> Add property</button></div>}
      </>}
      {staticPreview && <div className="cc-preview"><h4><ShieldCheck size={17} /> Change preview · {staticPreview.key} · {staticPreview.classification}</h4><p>Current values were read from each node's active file. Agents verify them again before writing.</p><div className="cc-preview-table"><div><strong>Node</strong><strong>Current value</strong><strong>New value</strong></div>{staticPreview.targets.map(target => <div key={target.nodeId}><span>{target.nodeId}</span><code>{target.oldValue ?? 'Unset'}</code><code>{target.newValue}</code></div>)}</div><p>Cluster health: <strong>{staticPreview.healthy ? 'Passed' : 'Failed'}</strong>{staticPreview.clusterLocked ? ' · Another cluster job is active' : ''}</p>{!staticPreview.healthy && <p className="cc-error">{Object.values(staticPreview.healthFailures).join('; ')}</p>}<button className="cc-primary" disabled={!canManage || !staticPreview.healthy || staticPreview.clusterLocked || !!busy} onClick={() => void applyStatic()}>{busy === 'static-apply' ? 'Queueing…' : `Start rolling change on ${staticPreview.targets.length} node${staticPreview.targets.length === 1 ? '' : 's'}`}</button></div>}
    </>;

  return <div className="cc-page">
    <header className="cc-page-header"><div><span className="cc-overline"><Settings size={15} /> Configuration change</span><h2>{clusterName}</h2><p>Select a Kafka role and targets, then review the changes before execution.</p></div><button className="cc-refresh" onClick={() => void refresh()} disabled={!!busy}><RefreshCw size={16} /> Refresh topology</button></header>
    {error && <div className="cc-error" role="alert">{error}</div>}
    {busy === 'load' && <div className="cc-loading"><Loader2 className="spin" /> Loading live configuration…</div>}
    {payload && <>
      <section className="cc-card"><div className="cc-section-heading"><span>1</span><div><h3>Select service role</h3><p>Broker and controller changes use separate target lists.</p></div></div><div className="cc-role-grid"><button className={role === 'BROKER' ? 'active' : ''} disabled={!!busy} onClick={() => updateRole('BROKER')}>Brokers <small>{(payload.serviceTopology || []).filter(item => item.role.toLowerCase().includes('broker')).length} services</small></button><button className={role === 'CONTROLLER' ? 'active' : ''} disabled={!!busy} onClick={() => updateRole('CONTROLLER')}>Controllers <small>{(payload.serviceTopology || []).filter(item => item.role.toLowerCase().includes('controller')).length} services</small></button></div></section>
      <section className="cc-card"><div className="cc-section-heading"><span>2</span><div><h3>Select targets</h3><p>Choose all {role.toLowerCase()}s or only specific nodes.</p></div></div><div className="cc-scope"><label><input type="radio" disabled={!!busy} checked={scope === 'ALL'} onChange={() => { setScope('ALL'); setPreview(null); setStaticPreview(null); }} /> All {role.toLowerCase()}s</label><label><input type="radio" disabled={!!busy} checked={scope === 'SPECIFIC'} onChange={() => { setScope('SPECIFIC'); setPreview(null); setStaticPreview(null); }} /> Specific {role.toLowerCase()}s</label></div><div className="cc-target-list">{services.length === 0 ? <p>No {role.toLowerCase()} services were found in this cluster.</p> : services.map(service => <label key={`${service.nodeId}-${service.hostId}`}><input type="checkbox" checked={scope === 'ALL' || selected.includes(service.nodeId)} disabled={scope === 'ALL' || !!busy} onChange={() => toggle(service.nodeId)} /><span><strong>Node {service.nodeId}</strong><small>{service.hostAddress || service.hostId} · {service.role}</small></span></label>)}</div></section>
      {role === 'BROKER' ? <section className="cc-card">
        <div className="cc-section-heading"><span>3</span><div><h3>Set dynamic broker properties</h3><p>Configured properties are shown below. Add another editable property, enter a new value and click the tick to preview and precheck it.</p></div></div>
        <div className="cc-table-wrap"><table className="cc-property-table">
          <thead><tr><th>Property</th><th>Current value</th><th>New value</th><th>Actions</th></tr></thead>
          <tbody>{visibleKeys.map(name => <tr key={name} className={preview?.key === name ? 'cc-reviewed-row' : ''}>
            <td><code>{name}</code></td>
            <td><div className="cc-value-list">{brokerValues(name).map(target => <div key={target.nodeId}><span>Broker {target.nodeId}</span><code>{target.value === null ? 'Unset' : target.value === '' ? '(Empty)' : target.value}</code></div>)}</div></td>
            <td><input aria-label={`New value for ${name}`} value={draftValues[name] ?? ''} disabled={!canManage || !!busy} onChange={event => { setDraftValues(current => ({ ...current, [name]: event.target.value })); if (key === name) setPreview(null); }} placeholder="Enter new value" /></td>
            <td><div className="cc-row-actions"><button type="button" title={`Review ${name} and run precheck`} aria-label={`Review ${name} and run precheck`} disabled={!canManage || targetIds.length === 0 || !!busy} onClick={() => void requestPreview(name, draftValues[name] ?? '')}><Check size={17} /></button><button type="button" title={`Remove ${name} from this view`} aria-label={`Remove ${name} from this view`} disabled={!canManage || !!busy} onClick={() => { setRemovedKeys(current => [...current, name]); setDraftValues(current => { const next = { ...current }; delete next[name]; return next; }); if (key === name) setPreview(null); }}><Trash2 size={16} /></button></div></td>
          </tr>)}</tbody>
        </table></div>
        {visibleKeys.length === 0 && <p className="cc-empty-properties">No configured properties to show. Use Add property to choose an editable Kafka property.</p>}
        {canManage && <div className="cc-add-property"><select aria-label="Property to add" disabled={!!busy} value={propertyToAdd} onChange={event => setPropertyToAdd(event.target.value)}><option value="">Select a property to add</option>{addableKeys.map(name => <option key={name} value={name}>{name}</option>)}</select><button type="button" disabled={!addableKeys.includes(propertyToAdd) || !!busy} onClick={() => { setAddedKeys(current => [...current, propertyToAdd]); setRemovedKeys(current => current.filter(name => name !== propertyToAdd)); setPropertyToAdd(''); }}><Plus size={16} /> Add property</button></div>}
        {preview && <div className="cc-preview"><h4><ShieldCheck size={17} /> Change preview · {preview.key} · {preview.classification}</h4><div className="cc-preview-table"><div><strong>Broker</strong><strong>Current value</strong><strong>New value</strong></div>{preview.targets.map(target => <div key={target.nodeId}><span>{target.nodeId}</span><code>{target.oldValue ?? 'Unset'}</code><code>{target.newValue}</code></div>)}</div><p>Cluster health: <strong>{preview.healthy ? 'Passed' : 'Failed'}</strong>{preview.clusterLocked ? ' · Another cluster job is active' : ''}</p>{!preview.healthy && <p className="cc-error">{Object.values(preview.healthFailures).join('; ')}</p>}<button className="cc-primary" disabled={!canManage || !preview.healthy || preview.clusterLocked || !!busy} onClick={() => void apply()}>{busy === 'apply' ? 'Queueing…' : 'Apply to selected brokers'}</button></div>}
      </section> : <section className="cc-card"><div className="cc-section-heading"><span>3</span><div><h3>Controller configuration</h3><p>Choose a property, review every selected node and start a controlled rolling change.</p></div></div>{staticControls()}</section>}
      {role === 'BROKER' && <section className="cc-card"><div className="cc-section-heading"><span>4</span><div><h3>Broker properties requiring restart</h3><p>File changes are backed up and applied one node at a time, with Kafka health verification after each restart.</p></div></div>{staticControls()}</section>}
    </>}
  </div>;
}
