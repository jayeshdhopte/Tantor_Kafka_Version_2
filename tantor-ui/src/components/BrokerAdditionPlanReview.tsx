import { useEffect, useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import type { ReadinessPreview } from '../hooks/useAddNodeReadiness';

type Review = {
  previewId: string; contextHash: string; expiresAt: string; executionEnabled: boolean; blocker: string;
  proposedPlan: { schemaVersion: string; hostId: string; contextHash: string; nodeId: number; kafkaClusterId: string;
    brokerPort: number; quorumMode: string; dataPaths: string[]; controllerEndpoints: string[]; artifactChecksum: string; brokerRack?: string };
  executionSteps: string[];
};

export function BrokerAdditionPlanReview({ clusterId, preview }: { clusterId: string; preview: ReadinessPreview }) {
  const navigate = useNavigate();
  const requestKey = useRef<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState('');
  const [result, setResult] = useState<{ key: string; review?: Review; error?: string } | null>(null);
  const key = `${preview.previewId}:${preview.contextHash}`;
  useEffect(() => {
    const abort = new AbortController();
    const run = async () => {
      try {
        const response = await fetch(`/api/v1/ui/clusters/${encodeURIComponent(clusterId)}/add-node/previews/${encodeURIComponent(preview.previewId)}/broker-plan`, { signal: abort.signal });
        const body = await response.json().catch(() => null);
        if (!response.ok) throw new Error(body?.message || `Could not prepare broker plan (${response.status}).`);
        const plan = body?.proposedPlan;
        if (body.previewId !== preview.previewId || body.contextHash !== preview.contextHash || body.expiresAt !== preview.expiresAt
          || typeof body.executionEnabled !== 'boolean' || !['add-broker/v1', 'add-broker/v2'].includes(plan?.schemaVersion) || plan.hostId !== preview.hostId
          || plan.contextHash !== preview.contextHash || !Number.isInteger(plan.nodeId) || plan.nodeId < 1
          || (plan.brokerRack || '') !== String(preview.brokerPlan.brokerRack || '')
          || Date.parse(body.expiresAt) <= Date.now()
          || !Array.isArray(plan.dataPaths) || !Array.isArray(body.executionSteps)) throw new Error('The broker plan does not match the selected readiness preview. Run fresh checks.');
        if (!abort.signal.aborted) setResult({ key, review: body });
      } catch (error) {
        if (!abort.signal.aborted) setResult({ key, error: error instanceof Error ? error.message : 'Could not prepare broker plan.' });
      }
    };
    void run();
    return () => abort.abort();
  }, [clusterId, key, preview.previewId, preview.contextHash, preview.expiresAt, preview.hostId, preview.brokerPlan.brokerRack]);
  const current = result?.key === key ? result : null;
  const blockedButton = <button className="add-node-button primary" disabled>Deploy Node</button>;
  // Parent readiness state invalidates/unmounts this review on its expiry timer.
  if (preview.status !== 'PASS') return <><p role="alert">The plan has expired or its readiness context changed. Run fresh checks.</p>{blockedButton}</>;
  if (!current) return <><p role="status">Preparing broker execution plan…</p>{blockedButton}</>;
  if (current.error) return <><p role="alert">{current.error}</p>{blockedButton}</>;
  const review = current.review!;
  const deploy = async () => {
    if (submitting || !review.executionEnabled || Date.parse(preview.expiresAt) <= Date.now()) return;
    setSubmitting(true); setSubmitError('');
    requestKey.current ||= crypto.randomUUID();
    try {
      const response = await fetch(`/api/v1/ui/clusters/${encodeURIComponent(clusterId)}/add-node/operations`, {
        method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': requestKey.current },
        body: JSON.stringify({ previewId: preview.previewId, contextHash: preview.contextHash }),
      });
      const body = await response.json().catch(() => null);
      if (!response.ok || !body?.jobId) throw new Error(body?.message || body?.error || `Broker submission failed (${response.status}). Retry keeps the same request key.`);
      navigate(`/jobs/${encodeURIComponent(body.jobId)}`);
    } catch (error) { setSubmitError(error instanceof Error ? error.message : 'Could not submit broker addition.'); }
    finally { setSubmitting(false); }
  };
  return <div>
    <dl className="add-node-review">
      <dt>Proposed broker ID</dt><dd>{review.proposedPlan.nodeId} · not reserved</dd>
      <dt>Broker configuration</dt><dd>{review.proposedPlan.schemaVersion === 'add-broker/v2' ? 'broker.properties' : 'server.properties (existing contract)'}</dd>
      <dt>Controller discovery</dt><dd>{review.proposedPlan.quorumMode} · existing controllers only</dd>
      <dt>Approved package SHA-256</dt><dd><code style={{ overflowWrap: 'anywhere' }}>{review.proposedPlan.artifactChecksum}</code></dd>
    </dl>
    <p>The execution flow will perform these steps:</p>
    <ol>{review.executionSteps.map(step => <li key={step}>{step}</li>)}</ol>
    <p>{review.blocker}</p>
    {submitError && <p role="alert">{submitError}</p>}
    <button className="add-node-button primary" disabled={!review.executionEnabled || submitting} onClick={() => void deploy()}>{submitting ? 'Submitting broker…' : 'Deploy Node'}</button>
  </div>;
}
