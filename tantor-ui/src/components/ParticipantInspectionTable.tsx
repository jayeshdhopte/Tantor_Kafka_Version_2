import { useEffect, useRef, useState } from 'react';

export type InspectionParticipant = {
  hostId: string; role: string; nodeId: number; serviceName: string; configurationPath: string;
  evidenceStatus: string; serviceId?: string | null; inspectionAllowed?: boolean;
};
type Evidence = { schemaVersion: string; hostId: string; nodeId: number; role: string; serviceName: string; configPath: string;
  configSha256: string; unitSha256: string; pid: number; serviceActive: boolean; identityVerified: boolean;
  versionVerified: false; executionReady: false; observedAt: string; blockers: string[] };
type View = { taskId: string; serviceId: string; hostId: string; status: string; expiresAt: string;
  executionReady: false; message: string; evidence: Evidence | null };

export function ParticipantInspectionTable({ clusterId, contextHash, participants }: { clusterId: string; contextHash: string; participants: InspectionParticipant[] }) {
  return <div className="add-node-table-wrap"><table className="add-node-table"><thead><tr><th>Host / role / ID</th><th>Recorded service references</th><th>Runtime observation</th></tr></thead><tbody>
    {participants.map((participant, i) => <tr key={`${participant.hostId}:${participant.role}:${participant.nodeId}:${i}`}>
      <th scope="row">{participant.hostId} · {participant.role} · {participant.nodeId}</th>
      <td>{participant.serviceName || 'Service discovery required'}<br />{participant.configurationPath || 'Configuration discovery required'}</td>
      <td><ParticipantObservation key={`${contextHash}:${participant.serviceId}`} clusterId={clusterId} participant={participant} /></td>
    </tr>)}
  </tbody></table></div>;
}

function ParticipantObservation({ clusterId, participant }: { clusterId: string; participant: InspectionParticipant }) {
  const [view, setView] = useState<View | null>(null);
  const [taskId, setTaskId] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [expired, setExpired] = useState(false);
  const [revision, setRevision] = useState(0);
  const submission = useRef<AbortController | null>(null);
  useEffect(() => () => submission.current?.abort(), []);
  const { serviceId, hostId, nodeId, role } = participant;
  useEffect(() => {
    if (!taskId || !serviceId) return;
    const abort = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const response = await fetch(`/api/v1/ui/clusters/${encodeURIComponent(clusterId)}/add-node/process-inspections/${encodeURIComponent(taskId)}`, { signal: abort.signal, cache: 'no-store' });
        const body = await response.json();
        if (!response.ok) throw new Error(body?.message || 'Process observation is unavailable.');
        if (!validView(body, participant, taskId)) throw new Error('Unmatched process inspection evidence.');
        if (abort.signal.aborted) return;
        setView(body); setExpired(Date.parse(body.expiresAt) <= Date.now()); setError('');
        if (body.status === 'RUNNING' && Date.parse(body.expiresAt) > Date.now()) timer = setTimeout(() => void poll(), 1500);
      } catch (failure) { if (!abort.signal.aborted) setError(failure instanceof Error ? failure.message : 'Process observation failed.'); }
    };
    void poll();
    return () => { abort.abort(); if (timer) clearTimeout(timer); };
  }, [clusterId, serviceId, participant, taskId, revision]);
  useEffect(() => {
    if (!view) return;
    const timer = setTimeout(() => setExpired(true), Math.max(0, Math.min(Date.parse(view.expiresAt) - Date.now(), 2147483647)));
    return () => clearTimeout(timer);
  }, [view]);

  const begin = async () => {
    if (!serviceId || !participant.inspectionAllowed || busy) return;
    submission.current?.abort(); const abort = new AbortController(); submission.current = abort;
    setBusy(true); setError(''); setExpired(false); setTaskId(''); setView(null);
    try {
      const response = await fetch(`/api/v1/ui/clusters/${encodeURIComponent(clusterId)}/add-node/process-inspections`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ serviceId }), signal: abort.signal,
      });
      const body = await response.json();
      if (!response.ok) throw new Error(body?.message || 'Process inspection could not be requested.');
      if (!validView(body, participant)) throw new Error('Unmatched process inspection request.');
      if (abort.signal.aborted) return;
      setView(body); setExpired(Date.parse(body.expiresAt) <= Date.now()); setTaskId(body.taskId);
    } catch (failure) { if (!abort.signal.aborted) setError(failure instanceof Error ? failure.message : 'Inspection request failed.'); }
    finally { if (!abort.signal.aborted) setBusy(false); }
  };
  const allowed = Boolean(serviceId && participant.inspectionAllowed);
  const isExpired = expired;
  const pending = view?.status === 'RUNNING' && !isExpired;
  return <div aria-label={`Process inspection ${hostId} ${role} ${nodeId}`}>
    {!view && <small>{participant.evidenceStatus}</small>}
    {view && <p role="status">{isExpired ? 'Observation expired; request fresh evidence.' : view.status === 'OBSERVED' ? 'Runtime identity observed; execution remains unavailable.' : view.status}<br /><small>{view.message}</small></p>}
    {!isExpired && view?.evidence && <p><small>PID {view.evidence.pid} · config SHA-256 {view.evidence.configSha256.slice(0, 12)}…<br />Kafka artifact/version and quorum proof remain pending.</small></p>}
    {error && <p role="alert" className="add-node-remedy">{error}</p>}
    <button className="add-node-button" disabled={!allowed || busy || pending} onClick={() => void begin()}>{busy || pending ? 'Inspecting recorded process…' : 'Inspect recorded process'}</button>
    {error && taskId && <button className="add-node-button" onClick={() => { setError(''); setRevision(value => value + 1); }}>Refresh observation status</button>}
    {!allowed && <small>Inspection unavailable: verify permitted hosts, agent version and recorded service paths.</small>}
  </div>;
}

function validView(value: unknown, p: InspectionParticipant, taskId?: string): value is View {
  if (!value || typeof value !== 'object') return false;
  const view = value as View;
  if (typeof view.taskId !== 'string' || !/^[a-f0-9-]{36}$/.test(view.taskId) || (taskId && view.taskId !== taskId)
    || view.serviceId !== p.serviceId || view.hostId !== p.hostId || view.executionReady !== false || typeof view.message !== 'string'
    || !['RUNNING', 'OBSERVED', 'BLOCKED', 'STALE', 'EXPIRED'].includes(view.status) || !Number.isFinite(Date.parse(view.expiresAt))) return false;
  if (view.evidence === null) return view.status !== 'OBSERVED';
  const e = view.evidence;
  return Boolean(e && e.schemaVersion === 'add-node-process/v1' && e.hostId === p.hostId && e.nodeId === p.nodeId && typeof e.role === 'string'
    && normalizedRole(e.role) === normalizedRole(p.role) && e.configPath === p.configurationPath
    && e.serviceName === (p.serviceName.endsWith('.service') ? p.serviceName : `${p.serviceName}.service`)
    && e.executionReady === false && e.versionVerified === false && Number.isSafeInteger(e.pid) && e.pid >= 0
    && typeof e.configSha256 === 'string' && typeof e.unitSha256 === 'string' && Number.isFinite(Date.parse(e.observedAt))
    && (view.status !== 'OBSERVED' || e.identityVerified === true && e.serviceActive === true));
}
function normalizedRole(role: string) { return (role === 'broker_controller' ? 'broker,controller' : role).split(',').map(value => value.trim()).sort().join(','); }
