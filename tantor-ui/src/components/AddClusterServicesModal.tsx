import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate } from 'react-router-dom';
import { Check, Database, Loader2, Plug, X } from 'lucide-react';
import { KafkaConnectFields, SchemaRegistryFields, type ServiceArtifact } from './ServiceConfigurationFields';
import { defaultKafkaConnectConfig, defaultSchemaRegistryConfig, validateServiceHeap, validateServicePath } from './serviceConfiguration';
import { runtimeConfig } from '../config/runtimeConfig';
import '../pages/ClusterDeployment.css';
import './AddClusterServicesModal.css';

type Role = 'schema_registry' | 'kafka_connect';
interface Host { hostId?: string; hostname?: string; role?: string; canDeployServices?: boolean; reason?: string }
interface Cluster { id: string; name: string; mode?: string; hosts?: Host[] }
interface ServiceInstance { hostId: string; role: Role; groupId?: string; status?: string }
interface ClusterDetails extends Cluster {
  serviceInstances?: ServiceInstance[];
  bootstrapServers?: string; kafkaVersion?: string; installDirectory?: string;
  dataDirectory?: string; logDirectory?: string; config?: Record<string, unknown>;
}
interface PrecheckResult { status: 'RUNNING' | 'SUCCESS' | 'FAILED'; logOutput: string; errorMsg: string }
const choices = [
  { role: 'schema_registry' as const, title: 'Schema Registry', description: 'Manage and validate schemas for your Kafka data.', Icon: Database },
  { role: 'kafka_connect' as const, title: 'Kafka Connect', description: 'Connect Kafka to your data sources and destinations.', Icon: Plug },
];

export function AddClusterServicesModal({ cluster, onClose }: { cluster: Cluster; onClose: () => void }) {
  const navigate = useNavigate();
  const dialog = useRef<HTMLDivElement>(null);
  const [selected, setSelected] = useState<Role[]>([]);
  const [view, setView] = useState<'selection' | 'configuration' | 'prechecks'>('selection');
  const [schema, setSchema] = useState(() => defaultSchemaRegistryConfig(cluster.mode !== 'EXTERNAL'));
  const [connect, setConnect] = useState(() => cluster.mode === 'EXTERNAL' ? { ...defaultKafkaConnectConfig(), installDir: '/opt/tantor/kafka-connect', configDir: '/opt/tantor/kafka-connect/config', pluginDir: '/opt/tantor/kafka-connect/plugins', logDir: '/var/log/tantor/kafka-connect', workingDir: '/var/lib/tantor/kafka-connect' } : defaultKafkaConnectConfig());
  const [hostIds, setHostIds] = useState<Record<Role, string[]>>({ schema_registry: [], kafka_connect: [] });
  const [hosts, setHosts] = useState<Host[]>([]);
  const [existing, setExisting] = useState<string[]>([]);
  const [artifacts, setArtifacts] = useState<ServiceArtifact[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [deploying, setDeploying] = useState(false);
  const external = cluster.mode === 'EXTERNAL';
  const endpoint = external ? `/api/v1/ui/external-clusters/${cluster.id}/services` : `/api/v1/ui/clusters/${cluster.id}/services`;
  const [warning, setWarning] = useState('');
  const [agentIssues, setAgentIssues] = useState<string[]>([]);
  const [serviceStates, setServiceStates] = useState<{ id?: string; role: string; agentId?: string; hostname?: string; status: string; lastError?: string }[]>([]);
  const [details, setDetails] = useState<ClusterDetails | null>(null);
  const [prechecks, setPrechecks] = useState<Record<string, PrecheckResult>>({});
  const [checking, setChecking] = useState(false);
  const precheckRun = useRef(0);

  useEffect(() => {
    const controller = new AbortController();
    Promise.all([
      fetch(external ? endpoint : `/api/v1/ui/clusters/${cluster.id}`, { signal: controller.signal }),
      fetch('/api/v1/artifacts?status=AVAILABLE&size=100', { signal: controller.signal }),
    ]).then(async responses => {
      if (responses.some(response => !response.ok)) throw new Error('Could not load cluster configuration or artifacts. Close and reopen to retry.');
      const [details, uploaded] = await Promise.all(responses.map(response => response.json()));
      if (controller.signal.aborted) return;
      setExisting(details.serviceRoles || []);
      if (!external) setDetails(details as ClusterDetails);
      setWarning(details.deploymentWarning || '');
      setServiceStates(details.services || []);
      const inventory: Host[] = details.hosts || cluster.hosts || [];
      setAgentIssues(external ? inventory.filter(host => !host.canDeployServices).map(host => `${host.hostname || host.hostId}: ${host.reason || 'Service deployment is unavailable.'}`) : []);
      const eligible = inventory.filter(host => host.hostId && (!external || host.canDeployServices) && (!host.role || !['schema_registry', 'kafka_connect'].includes(host.role)));
      const unique = [...new Map(eligible.map(host => [host.hostId, host])).values()];
      setHosts(unique);
      if (unique.length === 1) setHostIds({
        schema_registry: (details.serviceInstances || []).some((instance: ServiceInstance) => instance.role === 'schema_registry' && instance.hostId === unique[0].hostId) ? [] : [unique[0].hostId!],
        kafka_connect: (details.serviceInstances || []).some((instance: ServiceInstance) => instance.role === 'kafka_connect' && instance.hostId === unique[0].hostId) ? [] : [unique[0].hostId!],
      });
      const worker = (details.serviceInstances || []).find((instance: ServiceInstance) => instance.role === 'kafka_connect');
      if (worker?.groupId) setConnect(current => ({ ...current, groupId: worker.groupId.endsWith('-' + cluster.id) ? worker.groupId.slice(0, -(cluster.id.length + 1)) : worker.groupId }));
      setArtifacts((uploaded.content || []).filter((artifact: ServiceArtifact) => artifact.status === 'AVAILABLE'));
    }).catch(reason => { if (!controller.signal.aborted) setError(reason.message); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [cluster, external, endpoint]);

  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const overflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    dialog.current?.focus();
    return () => { precheckRun.current += 1; document.body.style.overflow = overflow; previous?.focus(); };
  }, []);

  useEffect(() => {
    if (external) return;
    const unified = artifacts.find(a => a.version === details?.kafkaVersion && (a.fileName || a.filename || '').toLowerCase().startsWith('tantor-unified-kafka-'));
    if (unified) {
      if (!connect.artifactId) setConnect(current => ({ ...current, artifactId: unified.id }));
      if (!schema.artifactId) setSchema(current => ({ ...current, artifactId: unified.id }));
    }
  }, [artifacts, details?.kafkaVersion, external, connect.artifactId, schema.artifactId]);

  const artifactsFor = (role: Role) => artifacts.filter(artifact => artifact.serviceType === role.toUpperCase()
    || (artifact.fileName || artifact.filename || '').toLowerCase().startsWith('tantor-unified-kafka-'));
  const eligibleHosts = (role: Role) => hosts.filter(host => external || !details?.serviceInstances?.some(instance => instance.role === role && instance.hostId === host.hostId));
  const targets = selected.flatMap(role => hostIds[role].map(hostId => ({ role, hostId, key: role + ':' + hostId })));
  const issues = selected.flatMap(role => {
    const cfg = role === 'schema_registry' ? schema : connect;
    const label = role === 'schema_registry' ? 'Schema Registry' : 'Kafka Connect';
    const paths = [cfg.installDir, cfg.configDir, cfg.logDir, cfg.workingDir, ...(role === 'kafka_connect' ? [connect.pluginDir] : [])];
    return [
      !hostIds[role].length || hostIds[role].some(hostId => !eligibleHosts(role).some(host => host.hostId === hostId)) ? `${label}: select available hosts.` : '',
      !artifactsFor(role).some(artifact => artifact.id === cfg.artifactId) ? `${label}: select an available artifact.` : '',
      cfg.artifactId && !/^[0-9a-fA-F]{64}$/.test(artifactsFor(role).find(artifact => artifact.id === cfg.artifactId)?.sha256 || artifactsFor(role).find(artifact => artifact.id === cfg.artifactId)?.checksum || '') ? `${label}: artifact SHA-256 is missing.` : '',
      !Number.isInteger(cfg.port) || cfg.port < 1 || cfg.port > 65535 ? `${label}: enter a port between 1 and 65535.` : '',
      validateServiceHeap(cfg.heapMin, cfg.heapSize, label),
      ...paths.map(path => !external && !path.trim() ? '' : validateServicePath(path, `${label} directory`)),
      role === 'kafka_connect' && !/^[A-Za-z0-9._-]{1,200}$/.test(connect.groupId) ? 'Kafka Connect: enter a valid worker group ID (letters, numbers, dots, underscores or hyphens).' : '',
    ].filter(Boolean);
  });
  if (selected.length === 2 && hostIds.schema_registry.some(hostId => hostIds.kafka_connect.includes(hostId)) && schema.port === connect.port) issues.push('Services on the same host must use different REST ports.');
  const configured = selected.length > 0 && issues.length === 0 && !warning && !loading && !deploying;
  const ready = configured && view === 'prechecks' && !checking && targets.every(target => prechecks[target.key]?.status === 'SUCCESS');

  function precheckParameters(role: Role, hostId: string) {
    const cfg = role === 'schema_registry' ? schema : connect;
    const artifact = artifactsFor(role).find(item => item.id === cfg.artifactId)!;
    const configuration = details?.config || {};
    const base = String(configuration.kafka_install_base_dir || configuration.kafka_install_dir || details?.installDirectory || '/opt').replace(/\/$/, '');
    const kafkaInstall = base.endsWith('/kafka') ? base : base.split('/').pop()?.startsWith('kafka_')
      ? `${base.slice(0, base.lastIndexOf('/'))}/kafka` : `${base}/kafka`;
    const otherRole: Role = role === 'schema_registry' ? 'kafka_connect' : 'schema_registry';
    const other = selected.includes(otherRole) && hostIds[otherRole].includes(hostId)
      ? (otherRole === 'schema_registry' ? schema : connect) : null;
    return {
      artifact_url: `${runtimeConfig.artifactApiBasePath}/${artifact.id}/download`,
      checksum: artifact.sha256 || artifact.checksum || '',
      kafka_version: details?.kafkaVersion || '',
      bootstrap_servers: details?.bootstrapServers || '',
      kafka_install_dir: kafkaInstall,
      kafka_data_dir: String(configuration.data_dir || details?.dataDirectory || '/data/kafka'),
      kafka_log_dir: String(configuration.log_dir || details?.logDirectory || '/var/log/kafka'),
      rest_port: cfg.port, heap_size: cfg.heapSize, heap_xms: cfg.heapMin, heap_xmx: cfg.heapSize,
      install_dir: cfg.installDir.trim(), config_dir: cfg.configDir.trim(),
      log_dir: cfg.logDir.trim(), working_dir: cfg.workingDir.trim(),
      allow_deferred_kafka: false,
      other_install_dir: other?.installDir || '', other_config_dir: other?.configDir || '',
      other_log_dir: other?.logDir || '', other_working_dir: other?.workingDir || '',
      other_plugin_dir: other && 'pluginDir' in other ? other.pluginDir : '',
      ...(role === 'schema_registry'
        ? { schema_version: artifact.version || '', compatibility_level: schema.compatibility, schemas_topic: '_schemas' }
        : { connect_version: artifact.version || '', group_id: `${connect.groupId}-${cluster.id}`,
          plugin_dir: connect.pluginDir.trim(), min_free_disk_mb: 5120,
          offset_topic: 'connect-offsets',
          config_topic: 'connect-configs',
          status_topic: 'connect-status' }),
    };
  }

  async function runPrechecks() {
    if (!configured || external || !details?.bootstrapServers) {
      setError('Cluster bootstrap servers are unavailable for pre-checks.');
      return;
    }
    const run = ++precheckRun.current;
    setError('');
    setPrechecks({});
    setView('prechecks');
    setChecking(true);
    await Promise.all(targets.map(async ({ role, hostId, key }) => {
      const label = role === 'schema_registry' ? 'Schema Registry' : 'Kafka Connect';
      if (run === precheckRun.current) setPrechecks(current => ({ ...current, [key]: { status: 'RUNNING', logOutput: `Running ${label} pre-check…`, errorMsg: '' } }));
      try {
        const response = await fetch(`/api/v1/ui/hosts/${encodeURIComponent(hostId)}/precheck-${role === 'schema_registry' ? 'schema' : 'connect'}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(precheckParameters(role, hostId)),
        });
        const queued = await response.json().catch(() => ({}));
        if (!response.ok || !queued.taskId) throw new Error(queued.message || `Could not start ${label} pre-check.`);
        for (let attempt = 0; attempt < 90 && run === precheckRun.current; attempt++) {
          await new Promise(resolve => setTimeout(resolve, 2000));
          const result = await fetch(`/api/v1/ui/hosts/${encodeURIComponent(hostId)}/check-prerequisites/${queued.taskId}`);
          if (!result.ok) continue;
          const body = await result.json();
          const status = String(body.status || '').toUpperCase();
          if (status === 'SUCCESS' || status === 'FAILED') {
            if (run === precheckRun.current) setPrechecks(current => ({ ...current, [key]: {
              status, logOutput: body.logOutput || '', errorMsg: body.errorMsg || '',
            } }));
            return;
          }
        }
        throw new Error(`${label} pre-check timed out. Check the agent and retry.`);
      } catch (reason) {
        if (run === precheckRun.current) setPrechecks(current => ({ ...current, [key]: {
          status: 'FAILED', logOutput: '', errorMsg: reason instanceof Error ? reason.message : `${label} pre-check failed.`,
        } }));
      }
    }));
    if (run === precheckRun.current) setChecking(false);
  }

  async function deploy() {
    if (!(external ? configured && view === 'configuration' : ready)) return;
    setDeploying(true);
    setError('');
    try {
      const addons = Object.fromEntries(selected.map(role => {
        const cfg = role === 'schema_registry' ? schema : connect;
        const artifact = artifactsFor(role).find(item => item.id === cfg.artifactId)!;
        const instances = hostIds[role].map(hostId => ({
          enabled: true, host_id: hostId, artifact_id: artifact.id,
          checksum: artifact.sha256 || artifact.checksum || '', version: artifact.version || '',
          port: cfg.port, heap_size: cfg.heapSize, heap_xms: cfg.heapMin, heap_xmx: cfg.heapSize, install_dir: cfg.installDir.trim(),
          config_dir: cfg.configDir.trim(), log_dir: cfg.logDir.trim(), working_dir: cfg.workingDir.trim(),
          ...(role === 'schema_registry' ? { compatibility_level: schema.compatibility }
            : { group_id: connect.groupId, plugin_dir: connect.pluginDir.trim() }),
        }));
        return [external ? role : role === 'schema_registry' ? 'schema_registries' : 'kafka_connect_workers',
          external ? instances[0] : instances];
      }));
      const response = await fetch(endpoint, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(addons),
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.error || body.message || 'Could not start service deployment.');
      if (!body.jobId) throw new Error('The server did not return a deployment job. Refresh the Jobs page before retrying.');
      onClose();
      navigate(`/jobs/${body.jobId}`);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Could not start service deployment.');
    } finally { setDeploying(false); }
  }

  return createPortal(<div className="cd-modal-overlay add-services-overlay" onMouseDown={event => {
    if (event.target === event.currentTarget && !deploying) onClose();
  }}>
    <div className="cd-deployment-modal add-services-modal" role="dialog" aria-modal="true" aria-labelledby="add-services-title" tabIndex={-1} ref={dialog}
      onKeyDown={event => {
        if (event.key === 'Escape' && !deploying) onClose();
        if (event.key === 'Tab') {
          const items = dialog.current?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), select:not(:disabled)');
          if (!items?.length) return;
          const first = items[0], last = items[items.length - 1];
          if (event.shiftKey && (document.activeElement === first || document.activeElement === dialog.current)) { event.preventDefault(); last.focus(); }
          else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
        }
      }}>
      <header className="cd-deployment-modal-header">
        <div><h2 id="add-services-title">Add services to this cluster</h2><p>Select and configure services for <strong>{cluster.name}</strong>.</p></div>
        <button className="cd-icon-btn" aria-label="Close" disabled={deploying} onClick={onClose}><X size={22} /></button>
      </header>
      <div className="add-services-body">
        {external && <p>Deploy services through a Discovery agent linked to this cluster. Prerequisite checks run before installation; progress appears in Jobs. Existing Kafka configuration is preserved.</p>}
        {warning && <p role="alert" className="add-services-error">{warning}</p>}
        {agentIssues.length > 0 && <ul>{agentIssues.map(issue => <li key={issue}>{issue}</li>)}</ul>}
        {!loading && (external ? serviceStates.length > 0 : !!details?.serviceInstances?.length) &&
          <section className="add-services-existing" aria-labelledby="existing-services-title">
            <h3 id="existing-services-title">Existing service instances</h3>
            <div className="add-services-existing-scroll">
              <table>
                <thead><tr><th scope="col">Service</th><th scope="col">Hostname</th><th scope="col">Status</th></tr></thead>
                <tbody>{(external
                  ? serviceStates.map(service => ({ key: service.id || service.role, role: service.role,
                    hostname: service.hostname || hosts.find(host => host.hostId === service.agentId)?.hostname || service.agentId || 'Not reported',
                    status: service.status || 'UNKNOWN', error: service.lastError }))
                  : (details?.serviceInstances || []).map(instance => ({ key: instance.role + ':' + instance.hostId, role: instance.role,
                    hostname: hosts.find(host => host.hostId === instance.hostId)?.hostname || instance.hostId,
                    status: instance.status || 'UNKNOWN', error: undefined }))
                ).map(instance => <tr key={instance.key}>
                  <td><strong>{instance.role === 'schema_registry' ? 'Schema Registry' : 'Kafka Connect'}</strong></td>
                  <td>{instance.hostname}</td>
                  <td><span className={`add-services-instance-status ${instance.status.toUpperCase() === 'ONLINE' ? 'online' : ['OFFLINE', 'ERROR', 'FAILED'].includes(instance.status.toUpperCase()) ? 'offline' : 'unknown'}`}
                    title="Status reported by the backend; deployment success does not imply live connectivity.">{instance.status}</span>
                    {instance.error && <small className="add-services-error">{instance.error}</small>}
                  </td>
                </tr>)}</tbody>
              </table>
            </div>
          </section>}
        {loading ? <p role="status">Loading available services…</p> : <>
          <div className="cd-deployment-cards-wrapper"><div className="cd-deployment-choice-grid">
            {choices.map(({ role, title, description, Icon }) => <button key={role} type="button"
              className={`cd-deployment-card add-service-tile ${selected.includes(role) ? 'selected' : ''}`}
              aria-pressed={selected.includes(role)} disabled={deploying || !!warning || (external && existing.includes(role)) || eligibleHosts(role).length === 0}
              onClick={() => { precheckRun.current += 1; setChecking(false); setSelected(current => current.includes(role) ? current.filter(item => item !== role) : [...current, role]); setView('selection'); setPrechecks({}); setError(''); }}>
              <div className="add-service-tile-heading"><Icon size={24} />{selected.includes(role) && <Check size={20} />}</div>
              <h3>{title}</h3><p>{description}</p><span>{external && existing.includes(role) ? 'Already added' : selected.includes(role) ? 'Selected' : 'Select service'}</span>
            </button>)}
          </div></div>
          {hosts.length === 0 && <p role="status">No eligible Kafka hosts are available for this cluster.</p>}
          {view === 'configuration' && <fieldset disabled={deploying} className="add-services-fields">
            {selected.map(role => <section key={role}>
              {external ? <label className="cd-field"><span>{role === 'schema_registry' ? 'Schema Registry' : 'Kafka Connect'} host</span>
                <select value={hostIds[role][0] || ''} onChange={event => setHostIds(current => ({ ...current, [role]: event.target.value ? [event.target.value] : [] }))}>
                  <option value="">Select a cluster host</option>{hosts.map(host => <option key={host.hostId} value={host.hostId}>{host.hostname || host.hostId}</option>)}
                </select>
              </label> : <fieldset>
                <legend>{role === 'schema_registry' ? 'Schema Registry instances' : 'Connect workers'} — select hosts</legend>
                {eligibleHosts(role).map(host => <label key={host.hostId} className="cd-field">
                  <span><input type="checkbox" checked={hostIds[role].includes(host.hostId!)}
                    onChange={event => setHostIds(current => ({ ...current, [role]: event.target.checked
                      ? [...current[role], host.hostId!] : current[role].filter(id => id !== host.hostId) }))} /> {host.hostname || host.hostId}</span>
                </label>)}
                <p>Each selected host receives one instance. Settings below apply to all selected hosts.</p>
              </fieldset>}
              {role === 'schema_registry'
                ? <SchemaRegistryFields unifiedDefault={!external} cfg={schema} artifacts={artifactsFor(role)} onChange={patch => setSchema(current => ({ ...current, ...patch }))} />
                : <KafkaConnectFields showPluginNote={false} unifiedDefault={!external} cfg={connect} artifacts={artifactsFor(role)} onChange={patch => setConnect(current => ({ ...current, ...patch }))} />}
            </section>)}
            {issues.length > 0 && <ul className="add-services-validation" aria-live="polite">{[...new Set(issues)].map(issue => <li key={issue}>{issue}</li>)}</ul>}
          </fieldset>}
          {view === 'prechecks' && <section className="add-services-prechecks" aria-live="polite">
            <h3>Service pre-checks</h3>
            <p>These checks run on the selected host before deployment. The job will repeat them before installation.</p>
            {targets.map(({ role, hostId, key }) => {
              const result = prechecks[key];
              return <div className="add-services-precheck" key={key}>
                <strong>{role === 'schema_registry' ? 'Schema Registry' : 'Kafka Connect'} on {hosts.find(host => host.hostId === hostId)?.hostname || hostId} — {result?.status || 'Waiting'}</strong>
                {result?.errorMsg && <p className="add-services-error">{result.errorMsg}</p>}
                {result?.logOutput && <pre className="add-services-precheck-log">{result.logOutput}</pre>}
              </div>;
            })}
          </section>}
        </>}
        {error && <p role="alert" className="add-services-error">{error}</p>}
      </div>
      <footer className="add-services-footer">
        {view === 'selection' && <button className="cd-secondary-btn" disabled={loading || !selected.length || deploying} onClick={() => setView('configuration')}>Configuration</button>}
        {view === 'configuration' && <>
          <button className="cd-secondary-btn" disabled={deploying} onClick={() => setView('selection')}>Back</button>
          {external
            ? <button className="cd-primary-btn" disabled={!configured} onClick={deploy}>Check and deploy</button>
            : <button className="cd-primary-btn" disabled={!configured} onClick={runPrechecks}>Run pre-checks</button>}
        </>}
        {view === 'prechecks' && <>
          <button className="cd-secondary-btn" disabled={checking || deploying} onClick={() => { precheckRun.current += 1; setPrechecks({}); setView('configuration'); }}>Back to configuration</button>
          <button className="cd-secondary-btn" disabled={checking || deploying} onClick={runPrechecks}>Run pre-checks again</button>
          <button className="cd-primary-btn" disabled={!ready} onClick={deploy}>{deploying && <Loader2 size={16} className="spin" />}{deploying ? 'Starting deployment…' : 'Deploy'}</button>
        </>}
      </footer>
    </div>
  </div>, document.body);
}
