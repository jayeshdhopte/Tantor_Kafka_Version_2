import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { RefreshCw, X } from 'lucide-react';
import './ConfigChangeEntryModal.css';

interface Target { key: string; hostId: string; role: string; nodeId: number }
export function RollingRestartModal({ cluster, onClose, onStarted }: {
  cluster: { id: string; name?: string }; onClose: () => void; onStarted: (jobId: string) => void;
}) {
  const [targets, setTargets] = useState<Target[]>([]);
  const [scope, setScope] = useState('BROKERS');
  const [selected, setSelected] = useState<string[]>([]);
  const [combinedConfirmed, setCombinedConfirmed] = useState(false);
  const [singleConfirmed, setSingleConfirmed] = useState(false);
  const [loading, setLoading] = useState(true);
  const [starting, setStarting] = useState(false);
  const [error, setError] = useState('');
  useEffect(() => {
    const abort = new AbortController();
    fetch(`/api/v1/clusters/${cluster.id}/actions/rolling-restart/topology`, { signal: abort.signal })
      .then(async response => {
        const result = await response.json();
        if (!response.ok) throw new Error(result.message || result.error || 'Unable to load Kafka processes.');
        setTargets(result);
      }).catch(reason => { if (!abort.signal.aborted) setError(reason.message); })
      .finally(() => { if (!abort.signal.aborted) setLoading(false); });
    return () => abort.abort();
  }, [cluster.id]);
  const plan = targets.filter(target => scope === 'ALL' || (scope === 'BROKERS' && target.role.includes('broker'))
    || (scope === 'CONTROLLERS' && target.role.includes('controller')) || (scope === 'SELECTED' && selected.includes(target.key)));
  const combined = plan.some(target => target.role.includes('_'));
  const single = targets.length > 0 && new Set(targets.map(target => target.hostId)).size === 1;
  const start = async () => {
    setStarting(true); setError('');
    try {
      const response = await fetch(`/api/v1/clusters/${cluster.id}/actions/rolling-restart`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ scope, targets: selected, confirmCombined: combinedConfirmed, confirmSingleNode: singleConfirmed }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.message || result.error || 'Unable to start rolling restart.');
      onStarted(result.jobId);
    } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setStarting(false); }
  };
  return createPortal(<div className="cc-entry-overlay" onClick={() => { if (!starting) onClose(); }}>
    <div className="cc-entry-dialog" role="dialog" aria-modal="true" aria-labelledby="rolling-restart-title" onClick={event => event.stopPropagation()}>
      <header className="cc-entry-header"><div><span className="cc-entry-eyebrow"><RefreshCw size={15} /> Rolling restart</span>
        <h2 id="rolling-restart-title">{cluster.name || 'Kafka cluster'}</h2><p>Restart one Kafka process at a time after checking cluster health. Configuration remains unchanged.</p></div>
        <button className="cc-entry-close" aria-label="Close" disabled={starting} onClick={onClose}><X size={20} /></button></header>
      <div className="cc-entry-content">
        <label>Restart scope <select disabled={starting} value={scope} onChange={event => { setScope(event.target.value); setCombinedConfirmed(false); }}>
          <option value="BROKERS">Brokers</option><option value="CONTROLLERS">Controllers</option><option value="ALL">All Kafka processes</option><option value="SELECTED">Selected processes</option>
        </select></label>
        <h3>{scope === 'SELECTED' ? 'Select Kafka processes' : `Preview: ${plan.length} process(es)`}</h3>
        {loading && <p className="cc-entry-message">Loading topology…</p>}
        <div className="cc-entry-list">{(scope === 'SELECTED' ? targets : plan).map(target => <div key={target.key}>
          <span>{scope === 'SELECTED' && <input type="checkbox" aria-label={`Select ${target.role} ${target.nodeId} on ${target.hostId}`} disabled={starting} checked={selected.includes(target.key)} onChange={event => { setSelected(current => event.target.checked ? [...current, target.key] : current.filter(key => key !== target.key)); setCombinedConfirmed(false); }} />} Node {target.nodeId}</span>
          <strong>{target.hostId}</strong><span>{target.role}</span></div>)}</div>
        <p>Schema Registry, Kafka Connect and exporters are excluded. Health is checked before and after each restart; the active controller runs last among controller processes.</p>
        <p>You can keep viewing the cluster. Conflicting operations are locked while this job runs. If a node fails, the sequence stops and Retry / Resume keeps completed nodes.</p>
        {combined && <p><label><input type="checkbox" disabled={starting} checked={combinedConfirmed} onChange={event => setCombinedConfirmed(event.target.checked)} /> I understand combined processes restart both roles together.</label></p>}
        {single && <p><label><input type="checkbox" disabled={starting} checked={singleConfirmed} onChange={event => setSingleConfirmed(event.target.checked)} /> I accept interruption while this single-host cluster restarts.</label></p>}
        {error && <p className="cc-entry-error" role="alert">{error}</p>}
      </div>
      <footer className="cc-entry-footer"><button className="cc-entry-secondary" disabled={starting} onClick={onClose}>Cancel</button><button className="cc-entry-primary" disabled={loading || starting || plan.length === 0 || (combined && !combinedConfirmed) || (single && !singleConfirmed)} onClick={() => void start()}>{starting ? 'Starting…' : 'Start rolling restart'}</button></footer>
    </div>
  </div>, document.body);
}
