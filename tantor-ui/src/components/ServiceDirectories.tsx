import { RefreshButton } from './RefreshButton';
import { useEffect, useId, useState } from 'react';
import { ChevronDown, RefreshCw } from 'lucide-react';
import './ServiceDirectories.css';

interface ServicePath {
  instanceId?: string; serviceName?: string; deploymentStatus?: string; pathSource?: string; agentStatus?: string; reportedAt?: string;
  serviceType: string;
  hostId: string;
  hostname: string | null;
  ipAddress: string | null;
  installDir: string | null;
  configFile: string | null;
  pluginDir: string | null;
  workingDir: string | null;
  logDir: string | null;
}

interface DirectoryResult {
  clusterId: string;
  attempt: number;
  paths: ServicePath[];
  error?: string;
}

export function ServiceDirectories({ clusterId, serviceType, preview = false }: {
  clusterId?: string;
  serviceType: 'SCHEMA_REGISTRY' | 'KAFKA_CONNECT';
  preview?: boolean;
}) {
  const [result, setResult] = useState<DirectoryResult | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [expanded, setExpanded] = useState(false);
  const contentId = useId();
  useEffect(() => {
    if (!clusterId) return;
    const controller = new AbortController();
    fetch(`/api/v1/clusters/${clusterId}/overview`, { signal: controller.signal })
      .then(async response => {
        if (!response.ok) throw new Error('Unable to load service directories.');
        const data = await response.json() as { dataServicePaths?: ServicePath[] };
        if (!controller.signal.aborted) setResult({ clusterId, attempt, paths: data.dataServicePaths || [] });
      })
      .catch(() => {
        if (!controller.signal.aborted) setResult({ clusterId, attempt, paths: [], error: 'Unable to load service directories.' });
      });
    return () => controller.abort();
  }, [clusterId, attempt]);

  if (!clusterId) return null;
  const current = result?.clusterId === clusterId && result.attempt === attempt ? result : null;
  const paths = (current?.paths || []).filter(path => path.serviceType === serviceType)
    .slice().sort((a, b) => (a.hostname || a.hostId).localeCompare(b.hostname || b.hostId) || a.hostId.localeCompare(b.hostId));
  const serviceName = serviceType === 'SCHEMA_REGISTRY' ? 'Schema Registry' : 'Kafka Connect';

  return <section className="service-directories" aria-label={`${serviceName} directories`}>
    <header className="service-directories-heading">
      <h3>
        {preview ? `${serviceName} Directories` : <button className="service-directories-toggle" type="button" aria-expanded={expanded} aria-controls={contentId} onClick={() => setExpanded(value => !value)}>
          {serviceName} Directories
          <ChevronDown size={18} aria-hidden="true" className={expanded ? 'expanded' : ''} />
        </button>}
      </h3>
      <RefreshButton type="button" title="Refresh service directories" aria-label={`Refresh ${serviceName} directories`} disabled={!current} onClick={() => setAttempt(value => value + 1)}>
        <RefreshCw size={16} className={!current ? 'spin' : ''} aria-hidden="true" />
      </RefreshButton>
    </header>
    <div id={contentId} className="service-directories-content" hidden={!preview && !expanded}>
    {!current ? <p role="status">Loading service directories...</p>
      : current.error ? <p className="service-directories-error" role="alert">{current.error}</p>
        : paths.length === 0 ? <p>No directory information has been reported for this service.</p>
          : paths.map(path => <article className="service-directory-instance" key={path.instanceId || `${path.serviceType}-${path.hostId}-${path.configFile}`}>
            <h4>{path.hostname || path.hostId}{path.serviceName ? ` · ${path.serviceName}` : ''}</h4>
            <p>{path.ipAddress || 'IP not reported'}</p>
            {path.pathSource === 'AGENT_DISCOVERY' && <p>Agent-reported paths · Agent {path.agentStatus?.toLowerCase() || 'unknown'}{path.reportedAt ? ` · Last reported ${new Date(path.reportedAt).toLocaleString()}` : ''}</p>}
            {path.pathSource === 'DEPLOYMENT_CONFIGURATION' && <p>Saved deployment paths · Agent {path.agentStatus?.toLowerCase() || 'unknown'} · Deployment {path.deploymentStatus?.toLowerCase() || 'unknown'}</p>}
            <dl>{[
              ['Install directory', path.installDir],
              ['Configuration file', path.configFile],
              ...(serviceType === 'KAFKA_CONNECT' ? [['Plugin directory', path.pluginDir]] : []),
              ['Working directory', path.workingDir],
              ['Log directory', path.logDir],
            ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd><code>{value || 'Not reported'}</code></dd></div>)}</dl>
          </article>)}
    </div>
  </section>;
}
