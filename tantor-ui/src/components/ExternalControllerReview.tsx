import { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { newRequestKey } from '../utils/addNodeRequestKey';

type Preview = { previewId: string; contextHash: string; expiresAt: string; status: string; proposedControllerId: number | null; kafkaClusterId: string; kafkaVersion: string; serviceName: string; configPath: string; executionEnabled: boolean; blockers: string[]; planDigest: string; participants: { serviceId: string; agentId: string; role: string; nodeId: number; status: string }[] };
type Operation = { operationId: string; jobId: string; state: string; error?: string; inventoryPublished?: boolean };
class ControllerRequestError extends Error { status: number; constructor(message: string, status: number) { super(message); this.status = status; } }
async function request<T>(url: string, signal: AbortSignal, body?: unknown): Promise<T> {
  const response = await fetch(url, { method: body === undefined ? 'GET' : 'POST', signal, headers: body === undefined ? {} : { 'Content-Type': 'application/json' }, ...(body === undefined ? {} : { body: JSON.stringify(body) }), cache: 'no-store' });
  const value = await response.json().catch(() => null);
  if (!response.ok) throw new ControllerRequestError(value?.message || value?.error || 'Controller request unavailable. Preserve the current operation.', response.status);
  return value as T;
}

export function ExternalControllerReview({ clusterId, commonPreviewId, commonFresh, onLockChange }: { clusterId: string; commonPreviewId: string; commonFresh: boolean; onLockChange: (locked: boolean) => void }) {
  const base = `/api/v1/ui/external-clusters/${encodeURIComponent(clusterId)}/add-node`;
  const [preview, setPreview] = useState<Preview | null>(null);
  const [operation, setOperation] = useState<Operation | null>(null);
  const [port, setPort] = useState(9093);
  const [parcels, setParcels] = useState('{}');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [unknown, setUnknown] = useState(false);
  const [now, setNow] = useState(Date.now);
  const key = useRef('');
  const abort = useRef<AbortController | null>(null);
  useEffect(() => () => abort.current?.abort(), []);
  useEffect(() => { const timer = window.setInterval(() => setNow(Date.now()), 1000); return () => window.clearInterval(timer); }, []);
  const previewId = preview?.previewId;
  const checking = preview?.status === 'CHECKING';
  useEffect(() => {
    if (!previewId || !checking || operation) return;
    const controller = new AbortController();
    const timer = window.setInterval(() => { void request<Preview>(`${base}/controller-previews/${encodeURIComponent(previewId)}`, controller.signal).then(setPreview).catch(failure => { if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : 'Controller checks unavailable.'); }); }, 2000);
    return () => { window.clearInterval(timer); controller.abort(); };
  }, [base, previewId, checking, operation]);
  const operationId = operation?.operationId;
  const operationState = operation?.state;
  useEffect(() => {
    if (!operationId || ['SUCCEEDED', 'CANCELLED_SAFE'].includes(operationState || '')) return;
    const controller = new AbortController();
    const timer = window.setInterval(() => { void request<Operation>(`${base}/controller-operations/${encodeURIComponent(operationId)}`, controller.signal).then(setOperation).catch(failure => { if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : 'Track the existing controller job.'); }); }, 3000);
    return () => { window.clearInterval(timer); controller.abort(); };
  }, [base, operationId, operationState]);
  const run = async () => {
    setBusy(true); setError(''); setPreview(null); key.current = '';
    const controller = new AbortController(); abort.current?.abort(); abort.current = controller;
    try {
      const directories: unknown = JSON.parse(parcels);
      if (!directories || Array.isArray(directories) || typeof directories !== 'object' || Object.values(directories).some(value => typeof value !== 'string')) throw new Error('Participant parcel directories must be a JSON object mapping agent IDs to approved paths.');
      setPreview(await request<Preview>(`${base}/controller-previews`, controller.signal, { commonPreviewId, controllerPort: port, participantParcelDirs: directories }));
    } catch (failure) { if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : 'Controller checks failed.'); }
    finally { if (!controller.signal.aborted) setBusy(false); }
  };
  const deploy = async () => {
    if (!preview?.executionEnabled || preview.status !== 'PASS' || Date.parse(preview.expiresAt) <= Date.now() || operation || unknown || busy) return;
    key.current ||= newRequestKey(); setBusy(true); setError(''); onLockChange(true);
    const controller = new AbortController(); abort.current = controller;
    try {
      const outcome = await request<Operation>(`${base}/controller-operations`, controller.signal, { previewId: preview.previewId, contextHash: preview.contextHash, planDigest: preview.planDigest, requestKey: key.current });
      if (!outcome.operationId || !outcome.jobId || !outcome.state) throw new Error('Controller submission outcome is incomplete. Confirm the original request.');
      setOperation(outcome); onLockChange(true);
    } catch (failure) { if (!controller.signal.aborted) { const uncertain = !(failure instanceof ControllerRequestError && [400, 403, 404, 409, 422].includes(failure.status)); setUnknown(uncertain); if (!uncertain) { key.current = ''; onLockChange(false); } setError(failure instanceof Error ? failure.message : 'Controller submission outcome is unknown.'); } }
    finally { if (!controller.signal.aborted) setBusy(false); }
  };
  const confirm = async () => {
    setBusy(true); setError(''); const controller = new AbortController(); abort.current = controller;
    try {
      const outcome = await request<Operation>(`${base}/controller-operations/request/${encodeURIComponent(key.current)}`, controller.signal);
      if (!outcome.operationId || !outcome.jobId) throw new Error('Controller submission has not been confirmed. Keep the original request key.');
      setOperation(outcome); setUnknown(false); onLockChange(true);
    } catch (failure) { if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : 'Controller submission remains unconfirmed.'); }
    finally { if (!controller.signal.aborted) setBusy(false); }
  };
  const frozen = busy || checking || Boolean(operation) || unknown;
  return <div>
    <h3>Dynamic controller addition</h3>
    <p>The new controller uses the existing Kafka cluster ID and exact version. It starts as an observer, catches up, then joins the voters. Existing brokers and controllers stay running. Static quorum and separate-process addition are unavailable in this phase.</p>
    <p>Controller installation, configuration, storage, logs and JVM heap come from the profile reviewed in Common checks. These selected-layout checks independently verify the controller profile and every existing Kafka process.</p>
    <div className="add-node-fields">
      <label>Controller listener port<input aria-label="Controller listener port" type="number" min={1} max={65535} value={port} disabled={frozen} onChange={event => { setPort(Number(event.target.value)); setPreview(null); }} /></label>
      <label className="full">Participant cached parcel directories<textarea aria-label="Participant cached parcel directories" value={parcels} disabled={frozen} onChange={event => { setParcels(event.target.value); setPreview(null); }} /><small>Use {'{}'} when participants use the same cached parcel directory as the candidate. Otherwise map enrolled agent IDs to their actual approved parcel directories.</small></label>
    </div>
    {!operation && !unknown && <button className="add-node-button" disabled={!commonFresh || frozen} onClick={() => void run()}>{busy || checking ? 'Checking controller readiness…' : 'Run controller checks'}</button>}
    {preview && <>
      <p role="status">Controller readiness: {Date.parse(preview.expiresAt) <= now ? 'EXPIRED' : preview.status}.</p>
      <dl className="add-node-review"><dt>Quorum mode</dt><dd>DYNAMIC</dd><dt>Controller node ID</dt><dd>{preview.proposedControllerId ?? 'Awaiting observations'}</dd><dt>Kafka cluster ID</dt><dd>{preview.kafkaClusterId || 'Awaiting observations'}</dd><dt>Kafka version</dt><dd>{preview.kafkaVersion || 'Awaiting observations'}</dd><dt>Service</dt><dd>{preview.serviceName}</dd><dt>Configuration</dt><dd>{preview.configPath || 'Awaiting observations'}</dd></dl>
      <table className="add-node-table"><thead><tr><th>External agent</th><th>Role / node ID</th><th>Runtime observation</th></tr></thead><tbody>{preview.participants.map(participant => <tr key={participant.serviceId}><td>{participant.agentId}</td><td>{participant.role} · {participant.nodeId}</td><td>{participant.status}</td></tr>)}</tbody></table>
      {preview.blockers.map(blocker => <p className="add-node-notice warning" key={blocker}>{blocker}</p>)}
    </>}
    {error && <p role="alert" className="add-node-notice error">{error}</p>}
    {operation ? <p role="status">Controller operation {operation.state}. <Link to={`/jobs/${encodeURIComponent(operation.jobId)}`}>Track controller execution</Link>. {operation.state === 'SUCCEEDED' ? <>Voter identity, stable health and inventory publication verified. <Link to={`/clusters/${encodeURIComponent(clusterId)}/nodes`}>View cluster nodes</Link>.</> : <>Observer startup alone does not confirm voter admission. {operation.error}</>}</p>
      : unknown ? <button className="add-node-button" disabled={busy} onClick={() => void confirm()}>Check controller submission status</button>
        : <button className="add-node-button primary" disabled={busy || checking || !preview?.executionEnabled || preview.status !== 'PASS' || Date.parse(preview.expiresAt) <= now} onClick={() => void deploy()}>Deploy controller</button>}
  </div>;
}
