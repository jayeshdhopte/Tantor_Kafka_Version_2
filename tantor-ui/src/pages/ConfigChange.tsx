import { RefreshButton } from '../components/RefreshButton';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Check, Loader2, Plus, RefreshCw, Settings, ShieldCheck, Trash2 } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { ConfigPropertySelect } from '../components/ConfigPropertySelect';
import { ConfigChangeHistory } from '../components/ConfigChangeHistory';
import './ConfigEditor.css';
import './ConfigVersioning.css';
import './ConfigChange.css';

interface Service { hostId: string; hostAddress: string; role: string; nodeId: number; serviceName?: string }
interface ConfigPayload { propertyCatalog?: { controller: string[] }; serviceTopology?: Service[]; dynamicConfigs?: Record<string, Record<string, { value: string | null; isReadOnly: boolean; isSensitive?: boolean; source?: string }>>; staticConfigs?: { configFiles?: Array<{ nodeId?: number; properties?: Record<string, unknown> }> } }
interface PreviewTarget { nodeId: number; oldValue: string | null; newValue: string | null; removesDynamicOverride?: boolean; effectiveValue?: string | null }
interface Preview { classification: string; key: string; targets: PreviewTarget[]; healthy: boolean; healthFailures: Record<string, string>; clusterLocked: boolean; properties?: Preview[] }
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
  const [chosenKeys, setChosenKeys] = useState<string[]>([]);
  const [search, setSearch] = useState('');
  const [reviewedChanges, setReviewedChanges] = useState<Record<string, string>>({});
  const [draftValues, setDraftValues] = useState<Record<string, string>>({});
  const [removedKeys, setRemovedKeys] = useState<string[]>([]);
  const [addedKeys, setAddedKeys] = useState<string[]>([]);
  const [propertyToAdd, setPropertyToAdd] = useState('');
  const [chosenStaticKeys, setChosenStaticKeys] = useState<string[]>([]);
  const [staticSearch, setStaticSearch] = useState('');
  const [reviewedStaticChanges, setReviewedStaticChanges] = useState<Record<string, string>>({});
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
  const [deletionPreview, setDeletionPreview] = useState<Preview | null>(null);
  const [deletionRequest, setDeletionRequest] = useState<object | null>(null);
  const deletionPanel = useRef<HTMLElement | null>(null);
  const errorPanel = useRef<HTMLDivElement | null>(null);
  const dynamicReviewPanel = useRef<HTMLDivElement | null>(null);
  const staticReviewPanel = useRef<HTMLDivElement | null>(null);
  useEffect(() => { if (error) errorPanel.current?.scrollIntoView({ behavior: 'smooth', block: 'center' }); }, [error]);
  useEffect(() => { if (preview) dynamicReviewPanel.current?.scrollIntoView({ behavior: 'smooth', block: 'center' }); }, [preview]);
  useEffect(() => { if (staticPreview) staticReviewPanel.current?.scrollIntoView({ behavior: 'smooth', block: 'center' }); }, [staticPreview]);
  useEffect(() => { if (deletionPreview) deletionPanel.current?.scrollIntoView({ behavior: 'smooth', block: 'start' }); }, [deletionPreview]);

  const refresh = useCallback(async () => {
    if (!id) return;
    setBusy('load'); setError(''); setDeletionPreview(null); setPreview(null);
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
  const targetIds = useMemo(() => scope === 'ALL' ? ids : selected, [scope, ids, selected]);
  const eligibleStaticTargets = useMemo(() => services.filter(service => targetIds.includes(service.nodeId)
    && !service.role.toLowerCase().includes('broker + controller')
    && !service.role.toLowerCase().includes('broker_controller')), [services, targetIds]);
  const staticTargetIds = useMemo(() => eligibleStaticTargets.map(service => service.nodeId), [eligibleStaticTargets]);
  const allKeys = useMemo(() => {
    const brokerIds = scope === 'ALL' ? ids : selected;
    const configs = payload?.dynamicConfigs || {};
    if (role !== 'BROKER' || brokerIds.length === 0) return [];
    const first = configs[String(brokerIds[0])] || {};
    return Object.keys(first).filter(name => brokerIds.every(brokerId => {
      const entry = configs[String(brokerId)]?.[name];
      return entry && !entry.isReadOnly && !entry.isSensitive && !protectedKeys.has(name) && !/(password|secret|token|jaas|credential|keystore|truststore)/i.test(name);
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
    if (!id || !payload || staticTargetIds.length === 0) return;
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
  const supportedStaticKeys = useMemo(() => {
    if (role === 'CONTROLLER') return payload?.propertyCatalog?.controller || [];
    if (!staticTargetIds.length) return [];
    const configs = payload?.dynamicConfigs || {};
    return Object.keys(configs[String(staticTargetIds[0])] || {}).filter(name => !protectedKeys.has(name)
      && !/(password|secret|token|jaas|credential|keystore|truststore)/i.test(name)
      && staticTargetIds.every(nodeId => {
        const entry = configs[String(nodeId)]?.[name];
        return entry?.isReadOnly === true && !entry.isSensitive;
      })).sort();
  }, [role, payload, staticTargetIds]);
  const fileKeys = Array.from(new Set(Object.values(fileValues || {}).flatMap(properties => Object.keys(properties))))
    .filter(name => !protectedKeys.has(name) && !/(password|secret|token|jaas|credential|keystore|truststore)/i.test(name))
    .filter(name => !Object.values(payload?.dynamicConfigs || {}).some(config => config[name]?.isSensitive))
    .filter(name => role !== 'BROKER' || staticTargetIds.every(nodeId =>
      payload?.dynamicConfigs?.[String(nodeId)]?.[name]?.isReadOnly === true)).sort();
  const staticVisibleKeys = Array.from(new Set([...fileKeys, ...staticAddedKeys.filter(name => supportedStaticKeys.includes(name))]))
    .filter(name => !staticRemovedKeys.includes(name)).sort();
  const staticAddableKeys = supportedStaticKeys.filter(name => !staticVisibleKeys.includes(name));

  const changeRequest = { role, scope, targetNodeIds: scope === 'ALL' ? ids : selected, changes: reviewedChanges };
  const requestPreview = async (keys = chosenKeys.filter(name => visibleKeys.includes(name))) => {
    const changes = Object.fromEntries(keys.map(name => [name, draftValues[name] ?? '']));
    if (!id) return;
    setBusy('preview'); setError(''); setPreview(null);
    try {
      if (!keys.length) throw new Error('Select at least one property to review.');
      const missing = keys.filter(name => !draftValues[name]?.trim());
      if (missing.length) throw new Error(`Enter a new value for ${missing.join(', ')}. Use Review deletion to remove a property.`);
      const response = await fetch(`/api/v1/clusters/${id}/config/change/preview`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...changeRequest, changes }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Preview failed.');
      if (result.classification !== 'DYNAMIC_ALLOWED') throw new Error('Review restart-required properties in the static section.');
      setReviewedChanges(changes); setPreview(result as Preview);
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
  const staticRequest = { role, scope: 'SPECIFIC', targetNodeIds: staticTargetIds, changes: reviewedStaticChanges };
  const reviewStatic = async (keys = chosenStaticKeys.filter(name => staticVisibleKeys.includes(name))) => {
    const changes = Object.fromEntries(keys.map(name => [name, staticDraftValues[name] ?? '']));
    if (!id) return;
    setBusy('static-preview'); setError(''); setStaticPreview(null);
    try {
      if (!keys.length) throw new Error('Select at least one property to review.');
      const missing = keys.filter(name => !staticDraftValues[name]?.trim());
      if (missing.length) throw new Error(`Enter a new value for ${missing.join(', ')}. Use Review deletion to remove a property.`);
      const response = await fetch(`/api/v1/clusters/${id}/config/change/preview`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...staticRequest, changes }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Static change preview failed.');
      if (result.classification !== 'ROLLING_RESTART_REQUIRED') throw new Error('Kafka classifies this property as dynamic. Use the broker dynamic change form.');
      setReviewedStaticChanges(changes); setStaticPreview(result as Preview);
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
  const reviewDeletion = async (keys: string[], isStatic: boolean) => {
    if (!id || !canManage) return;
    setBusy('delete-preview'); setError(''); setDeletionPreview(null); setPreview(null); setStaticPreview(null);
    const request = { operation: 'REMOVE', role, scope: 'SPECIFIC', targetNodeIds: isStatic ? staticTargetIds : targetIds, keys };
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/change/preview`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(request),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Unable to review property deletion.');
      setDeletionRequest(request); setDeletionPreview(result as Preview);
    } catch (reason) { setError(reason instanceof Error ? reason.message : 'Unable to review property deletion.'); }
    finally { setBusy(''); }
  };
  const applyDeletion = async () => {
    if (!id || !canManage || !deletionPreview || !deletionRequest) return;
    setBusy('delete-apply'); setError('');
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/change/apply`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(deletionRequest),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || 'Unable to queue property deletion.');
      navigate(`/jobs/${result.jobId}`);
    } catch (reason) { setError(reason instanceof Error ? reason.message : 'Unable to queue property deletion.'); }
    finally { setBusy(''); }
  };
  const toggle = (nodeId: number) => { setSelected(current => current.includes(nodeId) ? current.filter(id => id !== nodeId) : [...current, nodeId]); setPreview(null); setStaticPreview(null); setDeletionPreview(null); };
  const updateRole = (next: 'BROKER' | 'CONTROLLER') => { setRole(next); setSelected([]); setChosenKeys([]); setChosenStaticKeys([]); setAddedKeys([]); setRemovedKeys([]); setDraftValues({}); setPropertyToAdd(''); setStaticAddedKeys([]); setStaticRemovedKeys([]); setStaticDraftValues({}); setStaticAddKey(''); setPreview(null); setStaticPreview(null); setDeletionPreview(null); setError(''); };
  const staticControls = () => <>
      {targetIds.length !== staticTargetIds.length && <p className="cc-error">Combined broker/controller processes are excluded from role-only rolling changes.</p>}
      {staticLoading && <p className="cc-loading"><Loader2 className="spin" size={16} /> Reading active {role.toLowerCase()} properties from selected nodes…</p>}
      {staticReadError && <p className="cc-error">{staticReadError} <button type="button" onClick={() => void refresh()}>Retry</button></p>}
      {fileValues && <>
        <div className="cc-config-toolbar"><input type="search" aria-label="Search restart-required properties" placeholder="Search configuration properties…" value={staticSearch} onChange={event => setStaticSearch(event.target.value)} /><span>{chosenStaticKeys.filter(name => staticVisibleKeys.includes(name)).length} selected</span><button className="cc-primary" disabled={!canManage || !!busy || staticTargetIds.length === 0 || !chosenStaticKeys.some(name => staticVisibleKeys.includes(name))} onClick={() => void reviewStatic()}>Review selected changes</button><button className="cc-delete-button" disabled={!canManage || !!busy || !chosenStaticKeys.some(name => staticVisibleKeys.includes(name))} onClick={() => void reviewDeletion(chosenStaticKeys.filter(name => staticVisibleKeys.includes(name)), true)}>Review deletion</button></div>
        <div className="cc-table-wrap"><table className="cc-property-table">
          <thead><tr><th>Select</th><th>Property</th><th>Current value in file</th><th>New value</th><th>Actions</th></tr></thead>
          <tbody>{staticVisibleKeys.filter(name => name.toLowerCase().includes(staticSearch.toLowerCase().trim())).map(name => <tr key={name} className={staticPreview?.properties?.some(property => property.key === name) ? 'cc-reviewed-row' : ''}>
            <td><input type="checkbox" aria-label={`Select static ${name}`} checked={chosenStaticKeys.includes(name)} disabled={!canManage || !!busy} onChange={() => { setChosenStaticKeys(current => current.includes(name) ? current.filter(key => key !== name) : [...current, name]); setStaticPreview(null); }} /></td>
            <td><code>{name}</code></td>
            <td><div className="cc-value-list">{staticTargetIds.map(nodeId => {
              const current = fileValues[String(nodeId)]?.[name];
              return <div key={nodeId}><span>Node {nodeId}</span><code>{current === undefined ? 'Unset' : current === '' ? '(Empty)' : current}</code></div>;
            })}</div></td>
            <td><input aria-label={`New value for ${name}`} value={staticDraftValues[name] ?? ''} disabled={!canManage || !!busy} onChange={event => { setStaticDraftValues(current => ({ ...current, [name]: event.target.value })); setStaticPreview(null); }} placeholder="Enter new value" /></td>
            <td><div className="cc-row-actions"><button type="button" title={`Review ${name} and run precheck`} aria-label={`Review ${name} and run precheck`} disabled={!canManage || !!busy} onClick={() => void reviewStatic([name])}><Check size={17} /></button><button type="button" title={`Review deletion of ${name}`} aria-label={`Review deletion of ${name}`} disabled={!canManage || !!busy} onClick={() => {
              if (staticTargetIds.every(nodeId => fileValues[String(nodeId)]?.[name] === undefined)) {
                setStaticRemovedKeys(current => [...current, name]); setChosenStaticKeys(current => current.filter(key => key !== name)); setStaticPreview(null);
              } else void reviewDeletion([name], true);
            }}><Trash2 size={16} /></button></div></td>
          </tr>)}</tbody>
        </table></div>
        {staticSearch && !staticVisibleKeys.some(name => name.toLowerCase().includes(staticSearch.toLowerCase().trim())) && <p>No properties match your search.</p>}
        {staticVisibleKeys.length === 0 && <p className="cc-empty-properties">No restart-required properties were found in the selected active files. Add a property below.</p>}
        {canManage && <div className="cc-add-property"><ConfigPropertySelect label="Static property to add" value={staticAddKey}
          disabled={!!busy || !staticTargetIds.length} options={supportedStaticKeys.map(name => ({ value: name, disabled: staticVisibleKeys.includes(name) }))}
          onChange={setStaticAddKey} /><button type="button" disabled={!staticAddableKeys.includes(staticAddKey) || !!busy}
          onClick={() => { const name = staticAddKey; setStaticAddedKeys(current => [...current, name]); setStaticRemovedKeys(current => current.filter(key => key !== name)); setStaticAddKey(''); setStaticPreview(null); }}><Plus size={16} /> Add property</button></div>}
        <p className="cc-empty-properties">Choose from supported {role.toLowerCase()} settings. Sensitive and protected properties are excluded.</p>
      </>}
      {staticPreview && <div ref={staticReviewPanel} className="cc-preview"><h4><ShieldCheck size={17} /> Change preview · {staticPreview.key} · {staticPreview.classification}</h4><p>Current values were read from each node's active file. Agents verify them again before writing.</p>{(staticPreview.properties || [staticPreview]).map(property => <div key={property.key} className="cc-preview-property"><strong>{property.key}</strong><div className="cc-preview-table"><div><strong>Node</strong><strong>Current value</strong><strong>New value</strong></div>{property.targets.map(target => <div key={target.nodeId}><span>{target.nodeId}</span><code>{target.oldValue ?? 'Unset'}</code><code>{target.newValue === '' ? '(Empty)' : target.newValue}</code></div>)}</div>{property.targets.length === 0 && <p>Already matches the requested value.</p>}</div>)}<p>Cluster health: <strong>{staticPreview.healthy ? 'Passed' : 'Failed'}</strong>{staticPreview.clusterLocked ? ' · Another cluster job is active' : ''}</p>{!staticPreview.healthy && <p className="cc-error">{Object.values(staticPreview.healthFailures).join('; ')}</p>}<button className="cc-primary" disabled={!canManage || !staticPreview.healthy || staticPreview.clusterLocked || !!busy} onClick={() => void applyStatic()}>{busy === 'static-apply' ? 'Rechecking nodes and queueing…' : `Start rolling change on ${staticPreview.targets.length} node${staticPreview.targets.length === 1 ? '' : 's'}`}</button></div>}
    </>;

  return <div className="cc-page node-config-page versioned-config-page">
    <header className="cc-page-header node-config-header"><div><span className="cc-overline"><Settings size={15} /> Configuration change</span><h2>{clusterName}</h2><p>Select a Kafka role and targets, then review the changes before execution.</p></div><RefreshButton className="cc-refresh" onClick={() => void refresh()} disabled={!!busy}><RefreshCw size={16} className={busy === 'load' ? 'spin' : ''} /> Refresh topology</RefreshButton></header>
    {error && <div ref={errorPanel} className="cc-error" role="alert">{error}{error.includes('ELR') && <p>Kafka ELR requires min.insync.replicas to be changed at cluster-default or topic level. This per-broker form cannot apply that change. <button type="button" onClick={() => navigate(`/clusters/${id}/topics`)}>Open topics</button></p>}</div>}
    {busy === 'load' && <div className="cc-loading"><Loader2 className="spin" /> Loading live configuration…</div>}
    {busy && busy !== "load" && <div className="cc-loading" role="status"><Loader2 className="spin" /> Checking configuration and cluster health… Please wait.</div>}
    {payload && <>
      <section className="cc-card node-config-section"><div className="cc-section-heading node-config-section-title"><span>1</span><div><h3>Select service role</h3><p>Broker and controller changes use separate target lists.</p></div></div><div className="cc-role-grid"><button className={role === 'BROKER' ? 'active' : ''} disabled={!!busy} onClick={() => updateRole('BROKER')}>Brokers <small>{(payload.serviceTopology || []).filter(item => item.role.toLowerCase().includes('broker')).length} services</small></button><button className={role === 'CONTROLLER' ? 'active' : ''} disabled={!!busy} onClick={() => updateRole('CONTROLLER')}>Controllers <small>{(payload.serviceTopology || []).filter(item => item.role.toLowerCase().includes('controller')).length} services</small></button></div></section>
      <section className="cc-card node-config-section"><div className="cc-section-heading node-config-section-title"><span>2</span><div><h3>Select targets</h3><p>Choose all {role.toLowerCase()}s or only specific nodes.</p></div></div><div className="cc-scope"><label><input type="radio" disabled={!!busy} checked={scope === 'ALL'} onChange={() => { setScope('ALL'); setDeletionPreview(null); setPreview(null); setStaticPreview(null); }} /> All {role.toLowerCase()}s</label><label><input type="radio" disabled={!!busy} checked={scope === 'SPECIFIC'} onChange={() => { setScope('SPECIFIC'); setDeletionPreview(null); setPreview(null); setStaticPreview(null); }} /> Specific {role.toLowerCase()}s</label></div><div className="cc-target-list">{services.length === 0 ? <p>No {role.toLowerCase()} services were found in this cluster.</p> : services.map(service => <label key={`${service.nodeId}-${service.hostId}`}><input type="checkbox" checked={scope === 'ALL' || selected.includes(service.nodeId)} disabled={scope === 'ALL' || !!busy} onChange={() => toggle(service.nodeId)} /><span><strong>Node {service.nodeId}</strong><small>{service.hostAddress || service.hostId} · {service.role}</small></span></label>)}</div></section>
      {role === 'BROKER' ? <section className="cc-card node-config-section">
        <div className="cc-section-heading node-config-section-title"><span>3</span><div><h3>Set dynamic broker properties</h3><p>Search properties, select one or more, enter their new values and review the selected changes together.</p></div></div>
        <div className="cc-config-toolbar"><input type="search" aria-label="Search dynamic properties" placeholder="Search configuration properties…" value={search} onChange={event => setSearch(event.target.value)} /><span>{chosenKeys.filter(name => visibleKeys.includes(name)).length} selected</span><button className="cc-primary" disabled={!canManage || !!busy || targetIds.length === 0 || !chosenKeys.some(name => visibleKeys.includes(name))} onClick={() => void requestPreview()}>Review selected changes</button><button className="cc-delete-button" disabled={!canManage || !!busy || !chosenKeys.some(name => visibleKeys.includes(name))} onClick={() => void reviewDeletion(chosenKeys.filter(name => visibleKeys.includes(name)), false)}>Review deletion</button></div>
        <div className="cc-table-wrap"><table className="cc-property-table">
          <thead><tr><th>Select</th><th>Property</th><th>Current value</th><th>New value</th><th>Actions</th></tr></thead>
          <tbody>{visibleKeys.filter(name => name.toLowerCase().includes(search.toLowerCase().trim())).map(name => <tr key={name} className={preview?.properties?.some(property => property.key === name) ? 'cc-reviewed-row' : ''}>
            <td><input type="checkbox" aria-label={`Select dynamic ${name}`} checked={chosenKeys.includes(name)} disabled={!canManage || !!busy} onChange={() => { setChosenKeys(current => current.includes(name) ? current.filter(key => key !== name) : [...current, name]); setPreview(null); }} /></td>
            <td><code>{name}</code></td>
            <td><div className="cc-value-list">{brokerValues(name).map(target => <div key={target.nodeId}><span>Broker {target.nodeId}</span><code>{target.value === null ? 'Unset' : target.value === '' ? '(Empty)' : target.value}</code></div>)}</div></td>
            <td><input aria-label={`New value for ${name}`} value={draftValues[name] ?? ''} disabled={!canManage || !!busy} onChange={event => { setDraftValues(current => ({ ...current, [name]: event.target.value })); setPreview(null); }} placeholder="Enter new value" /></td>
            <td><div className="cc-row-actions"><button type="button" title={`Review ${name} and run precheck`} aria-label={`Review ${name} and run precheck`} disabled={!canManage || targetIds.length === 0 || !!busy} onClick={() => void requestPreview([name])}><Check size={17} /></button><button type="button" title={`Review deletion of ${name}`} aria-label={`Review deletion of ${name}`} disabled={!canManage || !!busy} onClick={() => {
              if (targetIds.every(nodeId => payload?.dynamicConfigs?.[String(nodeId)]?.[name]?.source === 'DEFAULT_CONFIG')) {
                setRemovedKeys(current => [...current, name]); setChosenKeys(current => current.filter(key => key !== name)); setPreview(null);
              } else void reviewDeletion([name], false);
            }}><Trash2 size={16} /></button></div></td>
          </tr>)}</tbody>
        </table></div>
        {search && !visibleKeys.some(name => name.toLowerCase().includes(search.toLowerCase().trim())) && <p>No properties match your search.</p>}
        {visibleKeys.length === 0 && <p className="cc-empty-properties">No configured properties to show. Use Add property to choose an editable Kafka property.</p>}
        {canManage && <div className="cc-add-property"><ConfigPropertySelect label="Dynamic property to add" disabled={!!busy || !targetIds.length}
          value={propertyToAdd} options={allKeys.map(name => ({ value: name, disabled: visibleKeys.includes(name) }))} onChange={setPropertyToAdd} />
          <button type="button" disabled={!addableKeys.includes(propertyToAdd) || !!busy} onClick={() => { setAddedKeys(current => [...current, propertyToAdd]); setRemovedKeys(current => current.filter(name => name !== propertyToAdd)); setPropertyToAdd(''); setPreview(null); }}><Plus size={16} /> Add property</button></div>}
        {preview && <div ref={dynamicReviewPanel} className="cc-preview"><h4><ShieldCheck size={17} /> Change preview · {preview.key} · {preview.classification}</h4>{(preview.properties || [preview]).map(property => <div key={property.key} className="cc-preview-property"><strong>{property.key}</strong><div className="cc-preview-table"><div><strong>Broker</strong><strong>Current value</strong><strong>New value</strong></div>{property.targets.map(target => <div key={target.nodeId}><span>{target.nodeId}</span><code>{target.oldValue ?? 'Unset'}</code><code>{target.newValue === '' ? '(Empty)' : target.newValue}</code></div>)}</div>{property.targets.length === 0 && <p>Already matches the requested value.</p>}</div>)}<p>Cluster health: <strong>{preview.healthy ? 'Passed' : 'Failed'}</strong>{preview.clusterLocked ? ' · Another cluster job is active' : ''}</p>{!preview.healthy && <p className="cc-error">{Object.values(preview.healthFailures).join('; ')}</p>}<button className="cc-primary" disabled={!canManage || !preview.healthy || preview.clusterLocked || !!busy} onClick={() => void apply()}>{busy === 'apply' ? 'Queueing…' : 'Apply to selected brokers'}</button></div>}
      </section> : <section className="cc-card node-config-section"><div className="cc-section-heading node-config-section-title"><span>3</span><div><h3>Controller configuration</h3><p>Choose a property, review every selected node and start a controlled rolling change.</p></div></div>{staticControls()}</section>}
      {role === 'BROKER' && <section className="cc-card node-config-section"><div className="cc-section-heading node-config-section-title"><span>4</span><div><h3>Broker properties requiring restart</h3><p>File changes are backed up and applied one node at a time, with Kafka health verification after each restart.</p></div></div>{staticControls()}</section>}
      {deletionPreview && <section ref={deletionPanel} className="cc-card cc-deletion-preview" aria-labelledby="deletion-review-title"><h3 id="deletion-review-title">Review property deletion</h3>
        <div className="cc-deletion-notice"><p>The original configuration file is backed up before deletion, with seven-day retention. Broker-level dynamic overrides are backed up and removed when present. Each selected process restarts once, with health checks between nodes.</p>
        <p>Removed settings use an inherited cluster value or Kafka default after restart. This does not delete topics or their data.</p></div>
        {(deletionPreview.properties || [deletionPreview]).map(property => <div className="cc-deletion-property" key={property.key}><h4><code>{property.key}</code></h4><div className="cc-deletion-table-wrap"><table className="cc-deletion-table"><thead><tr><th scope="col">Node</th><th scope="col">File value</th><th scope="col">After deletion</th></tr></thead><tbody>{property.targets.map(target => <tr key={target.nodeId}><td>{target.nodeId}</td><td><code>{target.oldValue === '' ? '(Empty)' : target.oldValue ?? 'Unset'}</code></td><td>Inherited / Kafka default{target.removesDynamicOverride && <small>Broker override also removed</small>}</td></tr>)}</tbody></table></div>{property.targets.length === 0 && <p>No nodes require this deletion.</p>}</div>)}
        <p className="cc-deletion-health">Cluster health and restart safety: <strong>{deletionPreview.healthy ? 'Passed' : 'Failed'}</strong>{deletionPreview.clusterLocked ? ' · Another cluster job is active' : ''}</p>
        {!deletionPreview.healthy && <p className="cc-error">{Object.values(deletionPreview.healthFailures).join('; ')}</p>}
        <div className="cc-deletion-actions"><button className="cc-refresh" disabled={!!busy} onClick={() => setDeletionPreview(null)}>Cancel deletion</button>
        <button className="cc-delete-button" disabled={!canManage || !deletionPreview.healthy || deletionPreview.clusterLocked || !!busy} onClick={() => void applyDeletion()}>{busy === 'delete-apply' ? 'Queueing…' : `Back up, delete and roll ${deletionPreview.targets.length} nodes`}</button></div>
      </section>}
    </>}
    {id && <ConfigChangeHistory clusterId={id} />}
  </div>;
}
