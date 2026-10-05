import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { GitCompare, Loader2, RefreshCw, Settings, ShieldCheck } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import './ConfigChange.css';

interface Service { hostId: string; hostAddress: string; role: string; nodeId: number; serviceName?: string }
interface ConfigPayload { serviceTopology?: Service[]; dynamicConfigs?: Record<string, Record<string, { value: string | null; isReadOnly: boolean }>>; staticConfigs?: { configFiles?: Array<{ nodeId?: number; properties?: Record<string, unknown> }> } }
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
  const [staticKey, setStaticKey] = useState('');
  const [staticValue, setStaticValue] = useState('');
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
  const eligibleStaticTargets = services.filter(service => targetIds.includes(service.nodeId)
    && !service.role.toLowerCase().includes('broker + controller')
    && !service.role.toLowerCase().includes('broker_controller'));
  const openStaticTargets = () => navigate(`/clusters/${id}/config?targets=${eligibleStaticTargets.map(service => service.nodeId).join(',')}`);
  const staticTargetIds = eligibleStaticTargets.map(service => service.nodeId);
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

  const changeRequest = { role, scope, targetNodeIds: scope === 'ALL' ? ids : selected, key, value };
  const requestPreview = async () => {
    if (!id) return;
    setBusy('preview'); setError(''); setPreview(null);
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/change/preview`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(changeRequest),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Preview failed.');
      setPreview(result as Preview);
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
  const reviewStatic = async () => {
    if (!id) return;
    setBusy('static-preview'); setError(''); setStaticPreview(null);
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/change/preview`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(staticRequest),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Static change preview failed.');
      if (result.classification !== 'ROLLING_RESTART_REQUIRED') throw new Error('Kafka classifies this property as dynamic. Use the broker dynamic change form.');
      setStaticPreview(result as Preview);
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
      <div className="cc-property-grid"><label>Kafka property<input list="cc-static-keys" value={staticKey} onChange={event => { setStaticKey(event.target.value); setStaticPreview(null); }} placeholder="For example, num.network.threads" /><datalist id="cc-static-keys">{suggestedStaticKeys.map(name => <option key={name} value={name} />)}</datalist></label><label>New value<input value={staticValue} onChange={event => { setStaticValue(event.target.value); setStaticPreview(null); }} placeholder="New property value" /></label></div>
      <button className="cc-primary" disabled={!canManage || !staticKey.trim() || staticTargetIds.length === 0 || !!busy} onClick={() => void reviewStatic()}><GitCompare size={16} /> Preview rolling change and precheck</button>
      {staticPreview && <div className="cc-preview"><h4><ShieldCheck size={17} /> Change preview · {staticPreview.classification}</h4><p>Current values were read from each node's active file. Agents verify them again before writing.</p><div className="cc-preview-table"><div><strong>Node</strong><strong>Current value</strong><strong>New value</strong></div>{staticPreview.targets.map(target => <div key={target.nodeId}><span>{target.nodeId}</span><code>{target.oldValue ?? 'Unset'}</code><code>{target.newValue}</code></div>)}</div><p>Cluster health: <strong>{staticPreview.healthy ? 'Passed' : 'Failed'}</strong>{staticPreview.clusterLocked ? ' · Another cluster job is active' : ''}</p>{!staticPreview.healthy && <p className="cc-error">{Object.values(staticPreview.healthFailures).join('; ')}</p>}<button className="cc-primary" disabled={!canManage || !staticPreview.healthy || staticPreview.clusterLocked || !!busy} onClick={() => void applyStatic()}>{busy === 'static-apply' ? 'Queueing…' : `Start rolling change on ${staticPreview.targets.length} node${staticPreview.targets.length === 1 ? '' : 's'}`}</button></div>}
    </>;

  return <div className="cc-page">
    <header className="cc-page-header"><div><span className="cc-overline"><Settings size={15} /> Configuration change</span><h2>{clusterName}</h2><p>Select a Kafka role and targets, then review the changes before execution.</p></div><button className="cc-refresh" onClick={() => void refresh()} disabled={!!busy}><RefreshCw size={16} /> Refresh topology</button></header>
    {error && <div className="cc-error" role="alert">{error}</div>}
    {busy === 'load' && <div className="cc-loading"><Loader2 className="spin" /> Loading live configuration…</div>}
    {payload && <>
      <section className="cc-card"><div className="cc-section-heading"><span>1</span><div><h3>Select service role</h3><p>Broker and controller changes use separate target lists.</p></div></div><div className="cc-role-grid"><button className={role === 'BROKER' ? 'active' : ''} onClick={() => updateRole('BROKER')}>Brokers <small>{(payload.serviceTopology || []).filter(item => item.role.toLowerCase().includes('broker')).length} services</small></button><button className={role === 'CONTROLLER' ? 'active' : ''} onClick={() => updateRole('CONTROLLER')}>Controllers <small>{(payload.serviceTopology || []).filter(item => item.role.toLowerCase().includes('controller')).length} services</small></button></div></section>
      <section className="cc-card"><div className="cc-section-heading"><span>2</span><div><h3>Select targets</h3><p>Choose all {role.toLowerCase()}s or only specific nodes.</p></div></div><div className="cc-scope"><label><input type="radio" checked={scope === 'ALL'} onChange={() => { setScope('ALL'); setPreview(null); setStaticPreview(null); }} /> All {role.toLowerCase()}s</label><label><input type="radio" checked={scope === 'SPECIFIC'} onChange={() => { setScope('SPECIFIC'); setPreview(null); setStaticPreview(null); }} /> Specific {role.toLowerCase()}s</label></div><div className="cc-target-list">{services.length === 0 ? <p>No {role.toLowerCase()} services were found in this cluster.</p> : services.map(service => <label key={`${service.nodeId}-${service.hostId}`}><input type="checkbox" checked={scope === 'ALL' || selected.includes(service.nodeId)} disabled={scope === 'ALL'} onChange={() => toggle(service.nodeId)} /><span><strong>Node {service.nodeId}</strong><small>{service.hostAddress || service.hostId} · {service.role}</small></span></label>)}</div></section>
      {role === 'BROKER' ? <section className="cc-card"><div className="cc-section-heading"><span>3</span><div><h3>Set a dynamic broker property</h3><p>Only properties Kafka reports as dynamically editable on every target are offered. Changes do not restart Kafka.</p></div></div><div className="cc-property-grid"><label>Property<select value={key} onChange={event => { setKey(event.target.value); setPreview(null); }}><option value="">Select a property</option>{allKeys.map(name => <option key={name} value={name}>{name}</option>)}</select></label><label>New value<input value={value} onChange={event => { setValue(event.target.value); setPreview(null); }} placeholder="New property value" /></label></div><button className="cc-primary" disabled={!canManage || !key || (scope === 'SPECIFIC' && selected.length === 0) || !!busy} onClick={() => void requestPreview()}><GitCompare size={16} /> Preview and precheck</button>{preview && <div className="cc-preview"><h4><ShieldCheck size={17} /> Change preview · {preview.classification}</h4><div className="cc-preview-table"><div><strong>Broker</strong><strong>Current value</strong><strong>New value</strong></div>{preview.targets.map(target => <div key={target.nodeId}><span>{target.nodeId}</span><code>{target.oldValue ?? 'Unset'}</code><code>{target.newValue}</code></div>)}</div><p>Cluster health: <strong>{preview.healthy ? 'Passed' : 'Failed'}</strong>{preview.clusterLocked ? ' · Another cluster job is active' : ''}</p>{!preview.healthy && <p className="cc-error">{Object.values(preview.healthFailures).join('; ')}</p>}<button className="cc-primary" disabled={!canManage || !preview.healthy || preview.clusterLocked || !!busy} onClick={() => void apply()}>{busy === 'apply' ? 'Queueing…' : 'Apply to selected brokers'}</button></div>}</section> : <section className="cc-card"><div className="cc-section-heading"><span>3</span><div><h3>Controller configuration</h3><p>Choose a property, review every selected node and start a controlled rolling change.</p></div></div>{staticControls()}</section>}
      {role === 'BROKER' && <section className="cc-card"><div className="cc-section-heading"><span>4</span><div><h3>Broker properties requiring restart</h3><p>File changes are backed up and applied one node at a time, with Kafka health verification after each restart.</p></div></div>{staticControls()}</section>}
    </>}
  </div>;
}
