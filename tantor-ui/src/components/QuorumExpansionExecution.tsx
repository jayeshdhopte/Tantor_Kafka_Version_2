import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import type { ReadinessPreview } from '../hooks/useAddNodeReadiness';

export type CommonReadinessRefreshProps = { onRefreshCommon?: () => void; commonRunning?: boolean; commonError?: string };

type Props = { clusterId: string; hostId: string; layout: 'CONTROLLER' | 'SEPARATE'; brokerRack?: string;
  commonPreview: ReadinessPreview | null; runtimeTaskIds: string[]; expectedParticipants: number; maintenanceRequired: boolean; planCurrent?: boolean; planContextHash?: string } & CommonReadinessRefreshProps;
type Review = { schemaVersion: string; previewId: string; clusterId: string; hostId: string; contextHash: string;
  status: 'RUNNING' | 'PASS' | 'BLOCKED' | 'EXPIRED' | 'STALE'; expiresAt: string; executionEnabled: boolean; message: string;
  proposedPlan: { layout: string; quorumMode: string; kafkaVersion: string; kafkaClusterId: string;
    processes: { role: string; nodeId: number; dataPaths: string[]; metadataPath: string; logPath: string; port: number; heapXmx: string }[] } };

export function QuorumExpansionExecution(props: Props) {
  const context = JSON.stringify([props.clusterId, props.hostId, props.layout, props.brokerRack, props.commonPreview?.previewId,
    props.commonPreview?.contextHash, [...props.runtimeTaskIds].sort(), props.maintenanceRequired, props.planContextHash]);
  return <ExecutionSession key={context} {...props} />;
}

function ExecutionSession({ clusterId, hostId, layout, brokerRack = '', commonPreview, runtimeTaskIds, expectedParticipants, maintenanceRequired, planCurrent = true, onRefreshCommon, commonRunning = false, commonError = '' }: Props) {
  const navigate = useNavigate();
  const [acknowledged, setAcknowledged] = useState(false);
  const [review, setReview] = useState<Review | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [now, setNow] = useState(Date.now);
  const [revision, setRevision] = useState(0);
  const idempotencyKey = useRef(crypto.randomUUID());
  const request = useRef<AbortController | null>(null);
  const base = `/api/v1/ui/clusters/${encodeURIComponent(clusterId)}/add-node/expansions`;
  useEffect(() => () => request.current?.abort(), []);
  useEffect(() => { const timer = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(timer); }, []);
  const validReview = (value: Review) => value?.schemaVersion === 'add-quorum-review/v1' && value.clusterId === clusterId
    && value.hostId === hostId && /^[a-f0-9-]{36}$/.test(value.previewId) && /^[a-f0-9]{64}$/.test(value.contextHash)
    && Number.isFinite(Date.parse(value.expiresAt)) && ['RUNNING', 'PASS', 'BLOCKED', 'EXPIRED', 'STALE'].includes(value.status)
    && typeof value.executionEnabled === 'boolean' && typeof value.message === 'string' && value.proposedPlan?.layout === layout
    && Array.isArray(value.proposedPlan.processes) && value.proposedPlan.processes.length === (layout === 'SEPARATE' ? 2 : 1);
  const previewId = review?.previewId;
  const polling = review?.status === 'RUNNING';
  useEffect(() => {
    if (!previewId || !polling) return;
    const abort = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const response = await fetch(`${base}/previews/${encodeURIComponent(previewId)}`, { signal: abort.signal, cache: 'no-store' });
        const body = await response.json();
        if (!response.ok) throw new Error(body?.message || 'Expansion checks could not be read.');
        if (!validReview(body) || body.previewId !== previewId) throw new Error('Expansion evidence does not match this review.');
        if (abort.signal.aborted) return;
        setReview(body); setError('');
        if (body.status === 'RUNNING' && Date.parse(body.expiresAt) > Date.now()) timer = setTimeout(() => void poll(), 1500);
      } catch (failure) { if (!abort.signal.aborted) setError(failure instanceof Error ? failure.message : 'Expansion checks failed.'); }
    };
    void poll(); return () => { abort.abort(); if (timer) clearTimeout(timer); };
    // The keyed session freezes the selected host, layout and evidence references.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [base, previewId, polling, revision]);
  const commonFresh = Boolean(commonPreview && !commonRunning && !commonError && !['RUNNING', 'STALE', 'EXPIRED'].includes(commonPreview.status) && Date.parse(commonPreview.expiresAt) > now
    && commonPreview.checks.filter(check => check.phase === 'COMMON').length === 21
    && commonPreview.checks.filter(check => check.phase === 'COMMON').every(check => check.status === 'PASS'));
  const readyToCheck = planCurrent && commonFresh && expectedParticipants > 0 && runtimeTaskIds.length === expectedParticipants && (!maintenanceRequired || acknowledged);
  const check = async () => {
    if (!readyToCheck || busy || !commonPreview) return;
    const abort = new AbortController(); request.current = abort; setBusy(true); setError(''); setReview(null);
    try {
      const response = await fetch(`${base}/previews`, { method: 'POST', signal: abort.signal, headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ hostId, layout, commonPreviewId: commonPreview.previewId, runtimeTaskIds, brokerRack, maintenanceAcknowledged: acknowledged }) });
      const body = await response.json();
      if (!response.ok) throw new Error(body?.message || 'Expansion preflight could not be requested.');
      if (!validReview(body)) throw new Error('Unmatched expansion preflight response.');
      if (!abort.signal.aborted) { idempotencyKey.current = crypto.randomUUID(); setReview(body); }
    } catch (failure) { if (!abort.signal.aborted) setError(failure instanceof Error ? failure.message : 'Expansion preflight failed.'); }
    finally { if (!abort.signal.aborted) setBusy(false); }
  };
  const fresh = Boolean(review && Date.parse(review.expiresAt) > now);
  const deploy = async () => {
    if (!review || !fresh || !readyToCheck || review.status !== 'PASS' || !review.executionEnabled || busy) return;
    const abort = new AbortController(); request.current = abort; setBusy(true); setError('');
    try {
      const response = await fetch(base, { method: 'POST', signal: abort.signal,
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey.current },
        body: JSON.stringify({ previewId: review.previewId, contextHash: review.contextHash }) });
      const body = await response.json();
      if (!response.ok) throw new Error(body?.message || 'Expansion could not be submitted.');
      if (!/^[a-f0-9-]{36}$/.test(body?.jobId) || !/^[a-f0-9-]{36}$/.test(body?.operationId)) throw new Error('The deployment response is incomplete. Retry to recover the same operation.');
      if (!abort.signal.aborted) navigate(`/jobs/${encodeURIComponent(body.jobId)}`);
    } catch (failure) { if (!abort.signal.aborted) setError(failure instanceof Error ? failure.message : 'Submission failed. Retry uses the same operation key.'); }
    finally { if (!abort.signal.aborted) setBusy(false); }
  };
  return <section aria-label="Expansion execution review">
    <h4>Final layout checks and deployment</h4>
    <p>Approved participant observations: {runtimeTaskIds.length} / {expectedParticipants}. {commonFresh ? 'Common checks are current.' : 'Run fresh common checks before proceeding.'}</p>
    {onRefreshCommon && <button className="add-node-button" disabled={busy || commonRunning} onClick={onRefreshCommon}>{commonRunning ? 'Refreshing common checks…' : 'Refresh common checks'}</button>}
    <p><small>Refreshing common checks keeps participant observations that are still valid. Expired observations must be inspected again.</small></p>
    {commonError && <p role="alert" className="add-node-remedy">{commonError}</p>}
    {!planCurrent && <p role="status">Refresh the planning review before validating this layout.</p>}
    {runtimeTaskIds.length !== expectedParticipants && <p role="status">Inspect each existing participant above. Only fresh approved observations count toward validation.</p>}
    {maintenanceRequired && <label><input type="checkbox" checked={acknowledged} disabled={busy || Boolean(review)} onChange={event => setAcknowledged(event.target.checked)} /> I have scheduled downtime for existing brokers and controllers, following the static quorum script.</label>}
    <button className="add-node-button" disabled={!readyToCheck || busy || polling && fresh} onClick={() => void check()}>{busy || polling && fresh ? 'Checking…' : 'Validate selected layout'}</button>
    {review && <><p role="status">{fresh ? review.status : 'EXPIRED'} · {review.message}</p>
      <p>Kafka {review.proposedPlan.kafkaVersion} · cluster <code>{review.proposedPlan.kafkaClusterId}</code> · {review.proposedPlan.quorumMode}</p>
      <ul>{review.proposedPlan.processes.map(process => <li key={process.role}>{process.role} · node ID {process.nodeId} · heap {process.heapXmx}<br />{process.dataPaths.join(', ')}{process.metadataPath && ` · metadata ${process.metadataPath}`}<br />Logs: {process.logPath}</li>)}</ul>
      {review.status === 'PASS' && !review.executionEnabled && <p>Execution remains held until release acceptance is complete.</p>}
    </>}
    {error && <><p role="alert" className="add-node-remedy">{error}</p>{polling && <button className="add-node-button" onClick={() => setRevision(value => value + 1)}>Refresh check status</button>}</>}
    <button className="add-node-button primary" disabled={busy || !readyToCheck || !fresh || review?.status !== 'PASS' || !review.executionEnabled} onClick={() => void deploy()}>Deploy {layout === 'SEPARATE' ? 'broker and controller' : 'controller'}</button>
  </section>;
}
