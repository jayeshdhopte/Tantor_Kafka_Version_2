import { useEffect, useState } from 'react';

type Operation = { operationId: string; jobId: string; state: string; reservationHeld: boolean; reason: string; checkpoints: Record<string, string> };
export function QuorumExpansionOperationStatus({ clusterId, operationId, jobId, failed, canManage, onChanged }: {
  clusterId: string; operationId: string; jobId: string; failed: boolean; canManage: boolean; onChanged: () => void;
}) {
  const [operation, setOperation] = useState<Operation | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [revision, setRevision] = useState(0);
  const base = `/api/v1/ui/clusters/${encodeURIComponent(clusterId)}/add-node/expansions/${encodeURIComponent(operationId)}`;
  useEffect(() => {
    const abort = new AbortController(); let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const response = await fetch(base, { signal: abort.signal, cache: 'no-store' }); const body = await response.json();
        if (!response.ok) throw new Error(body?.message || 'Operation ownership could not be read.');
        if (body.operationId !== operationId || body.jobId !== jobId || typeof body.reservationHeld !== 'boolean'
          || !['ACCEPTED', 'EXECUTING', 'RECOVERY_REQUIRED', 'SUCCEEDED', 'CANCELLED_SAFE'].includes(body.state)
          || !body.checkpoints || typeof body.checkpoints !== 'object') throw new Error('Unmatched expansion ownership response.');
        if (abort.signal.aborted) return;
        setOperation(body); setError('');
        if (body.reservationHeld) timer = setTimeout(() => void poll(), 3000);
      } catch (failure) { if (!abort.signal.aborted) setError(failure instanceof Error ? failure.message : 'Ownership unavailable.'); }
    };
    void poll(); return () => { abort.abort(); if (timer) clearTimeout(timer); };
  }, [base, jobId, operationId, revision]);
  const mutation = operation && Object.entries(operation.checkpoints).some(([step, state]) => state !== 'PENDING'
    && ['PREPARE', 'STAGE_VOTERS', 'STOP', 'APPLY_VOTERS', 'FORMAT', 'START', 'ADMIT', 'PUBLISH'].includes(step.split(':')[1]));
  const cancel = async () => {
    if (!operation?.reservationHeld || mutation || busy || !failed || !canManage) return;
    setBusy(true); setError('');
    try {
      const response = await fetch(`${base}/cancel-safe`, { method: 'POST' }); const body = await response.json();
      if (!response.ok) throw new Error(body?.message || 'Safe cancellation was refused. Reservations remain held.');
      setRevision(value => value + 1); onChanged();
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'Cancellation could not be confirmed.'); }
    finally { setBusy(false); }
  };
  return <section aria-label="Add Node operation ownership" className="overview-alert">
    {operation && <div><strong>{operation.state.replaceAll('_', ' ')}</strong><p>{operation.reservationHeld
      ? 'This operation retains the cluster, affected hosts and proposed node IDs. Resume the same job to reconcile progress.'
      : operation.state === 'SUCCEEDED' ? 'Membership publication is complete. The assigned node IDs remain recorded.' : 'Cancelled before mutation. Host and ID reservations were released.'}</p>
      {operation.reason && <p>{operation.reason}</p>}
      {failed && canManage && operation.reservationHeld && !mutation && <button className="btn btn-outline-primary" disabled={busy} onClick={() => void cancel()}>Cancel before changes</button>}
      {failed && mutation && <p>Changes have begun. Safe cancellation is unavailable; resume reconciles existing receipts and storage.</p>}
    </div>}
    {error && <div role="alert">{error}<button className="btn" onClick={() => setRevision(value => value + 1)}>Refresh operation</button></div>}
  </section>;
}
