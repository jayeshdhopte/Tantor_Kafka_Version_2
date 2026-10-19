import { useEffect, useId, useState } from 'react';
import { createPortal } from 'react-dom';
import { ChevronDown, Loader2, Settings, X } from 'lucide-react';
import { AnchoredMenu } from './AnchoredMenu';
import './ConfigChangeEntryModal.css';

interface ClusterHost { hostId?: string; hostname?: string; ipAddress?: string; role?: string }
interface ClusterInfo {
  id: string; name: string; mode: string; status: string; kafkaVersion: string;
  kafkaClusterId?: string; clusterId?: string; bootstrapServers?: string;
  hosts?: ClusterHost[];
}
interface Service { hostId: string; hostAddress: string; role: string; nodeId: number }

export function ConfigChangeEntryModal({ cluster, onClose, onContinue }: {
  cluster: ClusterInfo; onClose: () => void; onContinue: () => void;
}) {
  const [services, setServices] = useState<Service[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [bootstrapAnchor, setBootstrapAnchor] = useState<HTMLButtonElement | null>(null);
  const bootstrapListId = useId();
  useEffect(() => {
    const controller = new AbortController();
    fetch(`/api/v1/clusters/${cluster.id}/config`, { signal: controller.signal })
      .then(async response => {
        if (!response.ok) throw new Error('Unable to load current cluster topology.');
        const result = await response.json() as { serviceTopology?: Service[] };
        setServices(result.serviceTopology || []);
      })
      .catch(reason => { if (!controller.signal.aborted) setError(String(reason.message || reason)); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [cluster.id]);

  const brokers = services.filter(service => service.role.toLowerCase().includes('broker'));
  const controllers = services.filter(service => service.role.toLowerCase().includes('controller'));
  const nodes = new Set(services.map(service => service.hostId));
  const bootstrapEndpoints = (cluster.bootstrapServers || '').split(',').map(endpoint => endpoint.trim()).filter(Boolean);
  return createPortal(<div className="cc-entry-overlay" onClick={onClose}>
    <div className="cc-entry-dialog config-change-entry-dialog" role="dialog" aria-modal="true" aria-labelledby="cc-entry-title" onClick={event => event.stopPropagation()}>
      <header className="cc-entry-header"><div><span className="cc-entry-eyebrow"><Settings size={15} /> Configuration change</span><h2 id="cc-entry-title">{cluster.name}</h2><p>Review the cluster and its current node roles before selecting configuration targets.</p></div><button className="cc-entry-close" aria-label="Close" onClick={onClose}><X size={20} /></button></header>
      <div className="cc-entry-content">
        <div className="cc-entry-facts">
          <div><span>Cluster ID</span><strong title={cluster.kafkaClusterId || cluster.clusterId || cluster.id}>{cluster.kafkaClusterId || cluster.clusterId || cluster.id}</strong></div>
          <div><span>Source</span><strong>{cluster.mode === 'EXTERNAL' ? 'External' : 'Internal'}</strong></div>
          <div><span>Kafka version</span><strong>{cluster.kafkaVersion || 'Unknown'}</strong></div>
          <div><span>Status</span><strong>{cluster.status}</strong></div>
          <div><span>Brokers</span><strong>{loading ? '…' : brokers.length}</strong></div>
          <div><span>Controllers</span><strong>{loading ? '…' : controllers.length}</strong></div>
          <div><span>Nodes</span><strong>{loading ? '…' : nodes.size}</strong></div>
          <div><span>Bootstrap</span>{bootstrapEndpoints.length > 1 ? (
            <>
              <button type="button" className="cc-bootstrap-toggle" aria-label="Show Bootstrap endpoints" aria-expanded={Boolean(bootstrapAnchor)} aria-controls={bootstrapListId} onClick={event => { const anchor = event.currentTarget; setBootstrapAnchor(current => current ? null : anchor); }}>
                <span title={bootstrapEndpoints[0]}>{bootstrapEndpoints[0]}</span>
                <ChevronDown size={16} className={bootstrapAnchor ? 'expanded' : ''} aria-hidden="true" />
              </button>
              {bootstrapAnchor && <AnchoredMenu anchor={bootstrapAnchor} className="cc-bootstrap-menu" onClose={() => setBootstrapAnchor(null)} minWidth={280}>
                <section id={bootstrapListId} role="region" aria-label="Bootstrap endpoints" tabIndex={0}>
                  <h4>Bootstrap endpoints <span>{bootstrapEndpoints.length}</span></h4>
                  <ol>{bootstrapEndpoints.map((endpoint, index) => <li key={`${endpoint}-${index}`}><span>{index + 1}</span><code>{endpoint}</code></li>)}</ol>
                </section>
              </AnchoredMenu>}
            </>
          ) : <strong title={bootstrapEndpoints[0] || ''}>{bootstrapEndpoints[0] || 'See cluster details'}</strong>}</div>
        </div>
        <h3>Kafka services on this cluster</h3>
        {loading && <p className="cc-entry-message"><Loader2 size={16} className="spin" /> Loading current topology…</p>}
        {error && <p className="cc-entry-error" role="alert">{error}</p>}
        {!loading && !error && services.length === 0 && <p className="cc-entry-error">No Kafka services were found. Configuration change requires a known node topology.</p>}
        {!loading && !error && services.length > 0 && <div className="cc-entry-list">{services.map((service, index) => <div key={`${service.hostId}-${service.nodeId}-${index}`}><span className="cc-entry-node">Node {service.nodeId}</span><strong>{service.hostAddress || service.hostId}</strong><span>{service.role}</span></div>)}</div>}
      </div>
      <footer className="cc-entry-footer"><button className="cc-entry-secondary" onClick={onClose}>Cancel</button><button className="cc-entry-primary" disabled={loading || !!error || services.length === 0} onClick={onContinue}>Continue to configuration change</button></footer>
    </div>
  </div>, document.body);
}
