import { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { newRequestKey } from '../utils/addNodeRequestKey';
type Preview = { previewId: string; contextHash: string; expiresAt: string; status: string; proposedControllerId: number | null; kafkaClusterId: string; kafkaVersion: string; serviceName: string; configPath: string; executionEnabled: boolean; blockers: string[]; planDigest: string; separateProcesses?: boolean; proposedBrokerId?: number; brokerServiceName?: string; brokerConfigPath?: string; processes?: { role: string; nodeId: number; serviceName: string; configPath: string; port: number; jmxPort: number; dataPaths: string[]; metadataPath: string; logPath: string; heapXmx: string }[]; participants: { serviceId: string; agentId: string; role: string; nodeId: number; status: string }[] };
type Operation = { operationId: string; jobId: string; state: string; error?: string; inventoryPublished?: boolean };
class ControllerRequestError extends Error { status: number; constructor(message: string, status: number) { super(message); this.status = status; } }
async function request<T>(url: string, signal: AbortSignal, body?: unknown): Promise<T> {
  const response = await fetch(url, { method: body === undefined ? 'GET' : 'POST', signal, headers: body === undefined ? {} : { 'Content-Type': 'application/json' }, ...(body === undefined ? {} : { body: JSON.stringify(body) }), cache: 'no-store' });
  const value = await response.json().catch(() => null);
  if (!response.ok) throw new ControllerRequestError(value?.message || value?.error || 'Controller request unavailable. Preserve the current operation.', response.status);
  return value as T;
}
export function ExternalControllerReview({ clusterId, commonPreviewId, commonFresh, onLockChange, separateProcesses = false }: { separateProcesses?: boolean; clusterId: string; commonPreviewId: string; commonFresh: boolean; onLockChange: (locked: boolean) => void }) {
  const base = `/api/v1/ui/external-clusters/${encodeURIComponent(clusterId)}/add-node`;
  const [preview, setPreview] = useState<Preview | null>(null);
  const [operation, setOperation] = useState<Operation | null>(null);
  const [port, setPort] = useState(9093);
  const [controllerJmx, setControllerJmx] = useState('');
  const [brokerJmx, setBrokerJmx] = useState('');
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
      setPreview(await request<Preview>(`${base}/controller-previews`, controller.signal, { commonPreviewId, controllerPort: port, participantParcelDirs: directories, separateProcesses, ...(controllerJmx === '' ? {} : { controllerJmxPort: Number(controllerJmx) }), ...(brokerJmx === '' ? {} : { brokerJmxPort: Number(brokerJmx) }) }));
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
    <h3>{separateProcesses ? 'Broker + Controller - separate processes' : 'Dynamic controller addition'}</h3>
    <p>The new controller uses the existing Kafka cluster ID and exact version. It starts as an observer, catches up, then joins the voters. Existing brokers and controllers stay running. Static quorum expansion is unavailable in this external workflow.</p>
    {separateProcesses && <p>One fresh host, two separate Kafka JVMs. Both retain the existing cluster ID and exact version, with separate IDs, broker.service/controller.service, configuration files, storage and logs. Both layouts are prepared first; the controller must be verified as a voter before the broker starts. Neither process is published until both pass stable health verification. Recovery preserves the admitted controller.</p>}
    <p>Controller installation, configuration, storage, logs and JVM heap come from the profile reviewed in Common checks. These selected-layout checks independently verify the controller profile and every existing Kafka process.</p>
    <div className="add-node-fields">
      <label>Controller listener port<input aria-label="Controller listener port" type="number" min={1} max={65535} value={port} disabled={frozen} onChange={event => { setPort(Number(event.target.value)); setPreview(null); }} /></label>
      <label>Controller monitoring port<input aria-label="Controller monitoring port" type="number" min={0} max={65535} placeholder="Inherit" value={controllerJmx} disabled={frozen} onChange={event => { setControllerJmx(event.target.value); setPreview(null); }} /><small>Leave empty to inherit. An existing exporter cannot be disabled.</small></label>
      {separateProcesses && <label>Broker monitoring port<input aria-label="Broker monitoring port" type="number" min={0} max={65535} placeholder="Inherit" value={brokerJmx} disabled={frozen} onChange={event => { setBrokerJmx(event.target.value); setPreview(null); }} /><small>Listener and monitoring ports must be distinct across both processes.</small></label>}
      <label className="full">Participant cached parcel directories<textarea aria-label="Participant cached parcel directories" value={parcels} disabled={frozen} onChange={event => { setParcels(event.target.value); setPreview(null); }} /><small>Use {'{}'} when participants use the same cached parcel directory as the candidate. Otherwise map enrolled agent IDs to their actual approved parcel directories.</small></label>
    </div>
    {!operation && !unknown && <button className="add-node-button" disabled={!commonFresh || frozen} onClick={() => void run()}>{busy || checking ? 'Checking selected-layout readiness…' : separateProcesses ? 'Run separate-process checks' : 'Run controller checks'}</button>}
    {preview && <>
      <p role="status">{separateProcesses ? 'Separate-process readiness' : 'Controller readiness'}: {Date.parse(preview.expiresAt) <= now ? 'EXPIRED' : preview.status}.</p>
      <dl className="add-node-review"><dt>Quorum mode</dt><dd>DYNAMIC</dd><dt>Controller node ID</dt><dd>{preview.proposedControllerId ?? 'Awaiting observations'}</dd><dt>Kafka cluster ID</dt><dd>{preview.kafkaClusterId || 'Awaiting observations'}</dd><dt>Kafka version</dt><dd>{preview.kafkaVersion || 'Awaiting observations'}</dd><dt>Service</dt><dd>{preview.serviceName}</dd><dt>Configuration</dt><dd>{preview.configPath || 'Awaiting observations'}</dd>{separateProcesses && <><dt>Broker node ID</dt><dd>{preview.proposedBrokerId ?? 'Awaiting observations'}</dd><dt>Broker service</dt><dd>{preview.brokerServiceName || 'broker.service'}</dd><dt>Broker configuration</dt><dd>{preview.brokerConfigPath || 'Awaiting observations'}</dd></>}</dl>
      {preview.processes && preview.processes.length > 0 && <div className="add-node-table-wrap"><table className="add-node-table"><thead><tr><th>Process / ID</th><th>Service / configuration</th><th>Ports / maximum heap</th><th>Storage / logs</th></tr></thead><tbody>{preview.processes.map(process => <tr key={process.role}><td>{process.role} / {process.nodeId}</td><td>{process.serviceName}<br />{process.configPath}</td><td>Listener {process.port}; monitoring {process.jmxPort || 'disabled'}<br />{process.heapXmx}</td><td>Data: {process.dataPaths.join(', ')}<br />Metadata: {process.metadataPath || 'first data directory'}<br />Logs: {process.logPath}</td></tr>)}</tbody></table></div>}
      <table className="add-node-table"><thead><tr><th>External agent</th><th>Role / node ID</th><th>Runtime observation</th></tr></thead><tbody>{preview.participants.map(participant => <tr key={participant.serviceId}><td>{participant.agentId}</td><td>{participant.role} · {participant.nodeId}</td><td>{participant.status}</td></tr>)}</tbody></table>
      {preview.blockers.map(blocker => <p className="add-node-notice warning" key={blocker}>{blocker}</p>)}
    </>}
    {error && <p role="alert" className="add-node-notice error">{error}</p>}
    {operation ? <p role="status">{separateProcesses ? 'Broker + Controller operation' : 'Controller operation'} {operation.state}. <Link to={`/jobs/${encodeURIComponent(operation.jobId)}`}>{separateProcesses ? 'Track broker and controller execution' : 'Track controller execution'}</Link>. {operation.state === 'SUCCEEDED' ? <>{separateProcesses ? 'Both process identities, voter membership, unfenced broker registration, stable health and atomic inventory publication verified.' : 'Voter identity, stable health and inventory publication verified.'} <Link to={`/clusters/${encodeURIComponent(clusterId)}/nodes`}>View cluster nodes</Link>.</> : <>{separateProcesses ? 'Controller admission alone does not confirm broker registration or publication of both processes.' : 'Observer startup alone does not confirm voter admission.'} {operation.error}</>}</p>
      : unknown ? <button className="add-node-button" disabled={busy} onClick={() => void confirm()}>{separateProcesses ? 'Check separate-process submission status' : 'Check controller submission status'}</button>
        : <button className="add-node-button primary" disabled={busy || checking || !preview?.executionEnabled || preview.status !== 'PASS' || Date.parse(preview.expiresAt) <= now} onClick={() => void deploy()}>{separateProcesses ? 'Deploy broker and controller' : 'Deploy controller'}</button>}
  </div>;
}
