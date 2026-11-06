import { useEffect, useState } from 'react';
import { ParticipantInspectionTable, type InspectionParticipant } from './ParticipantInspectionTable';

type Review = {
  schemaVersion: string; clusterId: string; hostId: string; kafkaClusterId: string; kafkaVersion: string;
  quorumMode: 'STATIC' | 'DYNAMIC' | 'UNKNOWN'; proposedControllerId: number | null; maintenanceRequired: boolean;
  executionEnabled: false; status: string; expiresAt: string; contextHash: string;
  participants: InspectionParticipant[];
  steps: { code: string; name: string; scope: string; script: string }[]; blockers: string[];
};

export function ControllerAdditionPlanReview({ clusterId, hostId }: { clusterId: string; hostId: string }) {
  return <ControllerReviewSession key={`${clusterId}:${hostId}`} clusterId={clusterId} hostId={hostId} />;
}
function ControllerReviewSession({ clusterId, hostId }: { clusterId: string; hostId: string }) {
  const [review, setReview] = useState<Review | null>(null);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  const [expired, setExpired] = useState(false);
  useEffect(() => {
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    void (async () => {
      try {
        const r = await fetch(`/api/v1/ui/clusters/${encodeURIComponent(clusterId)}/add-node/controller-plan?hostId=${encodeURIComponent(hostId)}`, { signal: abort.signal });
        const body = await r.json();
        if (!r.ok) throw new Error(body?.message || 'Controller planning is unavailable.');
        if (body?.schemaVersion !== 'add-controller-review/v1' || body.clusterId !== clusterId || body.hostId !== hostId
          || body.executionEnabled !== false || !['STATIC', 'DYNAMIC', 'UNKNOWN'].includes(body.quorumMode)
          || body.maintenanceRequired !== (body.quorumMode === 'STATIC') || !Array.isArray(body.steps) || !Array.isArray(body.participants)
          || !Array.isArray(body.blockers) || !Number.isFinite(Date.parse(body.expiresAt))) throw new Error('Unmatched controller planning response.');
        if (abort.signal.aborted) return;
        setReview(body);
        const remaining = Date.parse(body.expiresAt) - Date.now();
        if (remaining <= 0) setExpired(true); else timer = setTimeout(() => setExpired(true), Math.min(remaining, 2147483647));
      } catch (e) { if (!abort.signal.aborted) setError(e instanceof Error ? e.message : 'Controller review failed.'); }
    })();
    return () => { abort.abort(); if (timer) clearTimeout(timer); };
  }, [clusterId, hostId, revision]);
  return <section aria-label="Controller addition plan">
    <h3>Controller addition · planning preview</h3>
    <p>This review does not reserve an ID, stop services, format storage, migrate the quorum or admit a controller.</p>
    {error ? <p role="alert" className="add-node-remedy">{error}</p> : !review ? <p role="status">Reading the live quorum and registered service inventory…</p> : <>
      <dl className="add-node-review">
        <dt>Quorum</dt><dd>{review.quorumMode}</dd><dt>Kafka version</dt><dd>{review.kafkaVersion} · inherited</dd>
        <dt>Kafka cluster ID</dt><dd><code>{review.kafkaClusterId}</code></dd>
        <dt>Proposed controller ID</dt><dd>{review.proposedControllerId ?? 'Unavailable'} · not reserved</dd>
        <dt>Impact</dt><dd>{review.maintenanceRequired ? 'Planned broker/controller downtime using the static maintenance workflow.' : review.quorumMode === 'DYNAMIC' ? 'Observer catch-up and voter admission; existing nodes remain running.' : 'Unknown quorum mode; no execution path selected.'}</dd>
      </dl>
      {expired && <p role="alert">This review expired. Refresh it before discussing the maintenance plan.</p>}
      <h4>Existing services in scope</h4>
      <ParticipantInspectionTable clusterId={clusterId} contextHash={review.contextHash} participants={review.participants} />
      <h4>Workflow mapped to your scripts</h4>
      <ol>{review.steps.map(s => <li key={s.code}><strong>{s.name}</strong><p>{s.scope}<br /><small>{s.script}</small></p></li>)}</ol>
      <h4>Execution blockers</h4><ul>{review.blockers.map(b => <li key={b}>{b}</li>)}</ul>
    </>}
    <button className="add-node-button" onClick={() => { setReview(null); setError(''); setExpired(false); setRevision(v => v + 1); }}>Refresh controller plan</button>
    <button className="add-node-button primary" disabled>Controller execution unavailable</button>
  </section>;
}
