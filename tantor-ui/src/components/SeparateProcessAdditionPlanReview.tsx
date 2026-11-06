import { useEffect, useState } from 'react';

type Process = {
  role: 'controller' | 'broker'; nodeId: number | null; processRoles: string; configurationFile: string;
  serviceName: string; listenerPort: number | null; requiredDirectories: string[]; evidenceStatus: 'NOT_INSPECTED';
};
type Review = {
  schemaVersion: string; clusterId: string; hostId: string; kafkaClusterId: string; kafkaVersion: string;
  quorumMode: 'STATIC' | 'DYNAMIC' | 'UNKNOWN'; brokerRack: string; expiresAt: string;
  executionEnabled: false; status: 'PLANNING_ONLY'; maintenanceRequired: boolean; processes: Process[];
  steps: { code: string; name: string; scope: string; script: string }[];
  checks: { code: string; name: string; phase: string; status: string; expected: string; observed: string }[];
  recoveryRules: { checkpoint: string; requirement: string }[]; blockers: string[];
};

export function SeparateProcessAdditionPlanReview(props: { clusterId: string; hostId: string; brokerRack: string }) {
  return <SeparateReviewSession key={`${props.clusterId}:${props.hostId}:${props.brokerRack}`} {...props} />;
}

function SeparateReviewSession({ clusterId, hostId, brokerRack }: { clusterId: string; hostId: string; brokerRack: string }) {
  const [review, setReview] = useState<Review | null>(null);
  const [error, setError] = useState('');
  const [expired, setExpired] = useState(false);
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    void (async () => {
      try {
        const params = new URLSearchParams({ hostId, brokerRack });
        const response = await fetch(`/api/v1/ui/clusters/${encodeURIComponent(clusterId)}/add-node/separate-plan?${params}`, { signal: abort.signal });
        const body = await response.json();
        if (!response.ok) throw new Error(body?.message || 'Separate-process planning is unavailable.');
        if (!validReview(body, clusterId, hostId, brokerRack)) throw new Error('Unmatched separate-process planning response.');
        if (abort.signal.aborted) return;
        setReview(body);
        const remaining = Date.parse(body.expiresAt) - Date.now();
        if (remaining <= 0) setExpired(true); else timer = setTimeout(() => setExpired(true), Math.min(remaining, 2147483647));
      } catch (failure) {
        if (!abort.signal.aborted) setError(failure instanceof Error ? failure.message : 'Separate-process review failed.');
      }
    })();
    return () => { abort.abort(); if (timer) clearTimeout(timer); };
  }, [clusterId, hostId, brokerRack, revision]);

  return <section aria-label="Separate-process addition plan">
    <h3>Broker + Controller · separate-process planning</h3>
    <p>One fresh host, two Kafka JVMs. Both retain the existing Kafka cluster ID and version. The controller must be verified before the broker starts.</p>
    {error ? <p role="alert" className="add-node-remedy">{error}</p> : !review ? <p role="status">Reading the live quorum and planning both process identities…</p> : <>
      <dl className="add-node-review">
        <dt>Quorum</dt><dd>{review.quorumMode}</dd><dt>Kafka version</dt><dd>{review.kafkaVersion} · inherited by both processes</dd>
        <dt>Kafka cluster ID</dt><dd><code>{review.kafkaClusterId}</code></dd><dt>Broker rack</dt><dd>{review.brokerRack || 'Not specified'} · still requires live validation</dd>
        <dt>Impact</dt><dd>{review.maintenanceRequired ? 'Static script plan: planned broker/controller downtime. Execution unavailable.' : review.quorumMode === 'DYNAMIC' ? 'Observer admission, then broker start. Existing services remain running.' : 'Quorum mode unknown; no workflow selected.'}</dd>
      </dl>
      {expired && <p role="alert">This separate-process review expired. Refresh it for current observations.</p>}
      <h4>Two proposed processes · IDs are not reserved</h4>
      <div className="add-node-table-wrap"><table className="add-node-table"><thead><tr><th>Process / proposed ID</th><th>Configuration / proposed unit</th><th>Listener</th><th>Required directory separation</th></tr></thead><tbody>
        {review.processes.map(process => <tr key={process.role}>
          <th scope="row">{process.role} · {process.nodeId ?? 'Unavailable'}<small>process.roles={process.processRoles}</small></th>
          <td><code>{process.configurationFile}</code><br />{process.serviceName || 'Unresolved'}</td>
          <td>{process.listenerPort ?? 'Not observed'}<br /><small>Port binding not inspected</small></td>
          <td>{process.requiredDirectories.join(' · ')}<br /><small>Relative layout only; absolute paths, mounts and ownership are not inspected.</small></td>
        </tr>)}
      </tbody></table></div>
      <h4>Two-process readiness</h4>
      <p>A broker-only readiness pass does not validate this layout. Both JVMs need resource, port and storage checks.</p>
      <ul>{review.checks.filter(check => check.phase === 'SEPARATE').map(check => <li key={check.code}><strong>{check.name} · {check.status}</strong><p>{check.expected}<br />{check.observed}</p></li>)}</ul>
      <h4>Controller-first workflow</h4>
      <ol>{review.steps.map(step => <li key={step.code}><strong>{step.name}</strong><p>{step.scope}<br /><small>{step.script}</small></p></li>)}</ol>
      <h4>Failure and resume behavior required before execution</h4>
      <ul>{review.recoveryRules.map(rule => <li key={rule.checkpoint}><strong>{rule.checkpoint}</strong><p>{rule.requirement}</p></li>)}</ul>
      <h4>Execution blockers</h4><ul>{review.blockers.map(blocker => <li key={blocker}>{blocker}</li>)}</ul>
      <p>No host/ID reservation, job, agent command or membership publication is created by this review.</p>
    </>}
    <button className="add-node-button" onClick={() => { setReview(null); setError(''); setExpired(false); setRevision(value => value + 1); }}>Refresh separate-process plan</button>
    <button className="add-node-button primary" disabled>Separate-process execution unavailable</button>
  </section>;
}

function validReview(value: unknown, clusterId: string, hostId: string, brokerRack: string): value is Review {
  if (!value || typeof value !== 'object') return false;
  const body = value as Review;
  return body.schemaVersion === 'add-separate-review/v1' && body.clusterId === clusterId && body.hostId === hostId
    && typeof body.kafkaClusterId === 'string' && typeof body.kafkaVersion === 'string'
    && body.brokerRack === brokerRack && body.executionEnabled === false && body.status === 'PLANNING_ONLY'
    && ['STATIC', 'DYNAMIC', 'UNKNOWN'].includes(body.quorumMode) && body.maintenanceRequired === (body.quorumMode === 'STATIC')
    && Number.isFinite(Date.parse(body.expiresAt)) && Array.isArray(body.processes) && body.processes.length === 2
    && body.processes.every((process, index) => process && process.role === (index === 0 ? 'controller' : 'broker')
      && process.processRoles === process.role && process.configurationFile === `${process.role}.properties`
      && (process.nodeId === null || Number.isSafeInteger(process.nodeId) && process.nodeId > 0)
      && (process.listenerPort === null || Number.isInteger(process.listenerPort) && process.listenerPort > 0 && process.listenerPort <= 65535)
      && typeof process.serviceName === 'string' && Array.isArray(process.requiredDirectories) && process.requiredDirectories.every(path => typeof path === 'string') && process.evidenceStatus === 'NOT_INSPECTED')
    && Array.isArray(body.steps) && body.steps.every(step => step && typeof step.code === 'string' && typeof step.name === 'string' && typeof step.scope === 'string' && typeof step.script === 'string')
    && Array.isArray(body.checks) && body.checks.every(check => check && typeof check.code === 'string' && typeof check.name === 'string' && typeof check.phase === 'string' && typeof check.status === 'string')
    && Array.isArray(body.recoveryRules) && body.recoveryRules.every(rule => rule && typeof rule.checkpoint === 'string' && typeof rule.requirement === 'string')
    && Array.isArray(body.blockers) && body.blockers.every(blocker => typeof blocker === 'string');
}
