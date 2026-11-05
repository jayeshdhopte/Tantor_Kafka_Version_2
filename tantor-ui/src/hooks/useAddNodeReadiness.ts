import { useEffect, useRef, useState } from 'react';

export type ReadinessCheck = {
  code: string; name: string; phase: 'COMMON' | 'BROKER'; status: 'PASS' | 'FAIL' | 'UNKNOWN' | 'BLOCKED';
  expected: unknown; observed: unknown; remedy: string; observedAt: string; source: string;
};
export type ReadinessPreview = {
  previewId: string; clusterId: string; hostId: string; contextHash: string;
  checkedAt: string; expiresAt: string; status: 'RUNNING' | 'PASS' | 'BLOCKED' | 'EXPIRED' | 'STALE';
  checks: ReadinessCheck[]; brokerPlan: Record<string, unknown>; deploymentEnabled: false;
};

export function useAddNodeReadiness(clusterId: string, hostId: string, inventoryRevision: number, brokerRack = '') {
  const [preview, setPreview] = useState<ReadinessPreview | null>(null);
  const [error, setError] = useState('');
  const [running, setRunning] = useState(false);
  const [now, setNow] = useState(Date.now);
  const request = useRef<AbortController | null>(null);
  const context = `${clusterId}:${hostId}:${inventoryRevision}:${brokerRack}`;
  const currentContext = useRef(context);
  useEffect(() => {
    currentContext.current = context;
    request.current?.abort();
    return () => { request.current?.abort(); };
  }, [context]);
  useEffect(() => {
    if (!preview) return;
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [preview]);
  const [resultContext, setResultContext] = useState('');
  const visible = resultContext === context ? preview : null;
  const visibleError = resultContext === context ? error : '';
  const busy = resultContext === context && running;

  const run = async () => {
    if (!hostId || busy) return;
    request.current?.abort();
    const abort = new AbortController(); request.current = abort;
    const startedContext = context;
    setResultContext(context); setRunning(true); setError(''); setPreview(null); setNow(Date.now());
    const accept = async (response: Response) => {
      const data = await response.json().catch(() => null);
      if (!response.ok) throw new Error(data?.message || data?.detail || data?.error || `Readiness request failed (${response.status}).`);
      if (!data || data.clusterId !== clusterId || data.hostId !== hostId || !data.previewId || !Array.isArray(data.checks)
        || !data.brokerPlan || typeof data.brokerPlan !== 'object'
        || String(data.brokerPlan?.brokerRack || '') !== brokerRack
        || !data.contextHash || !Number.isFinite(Date.parse(data.expiresAt)) || !Number.isFinite(Date.parse(data.checkedAt))
        || !['RUNNING', 'PASS', 'BLOCKED', 'EXPIRED', 'STALE'].includes(data.status)
        || data.checks.some((check: ReadinessCheck) => !check.code || !['COMMON', 'BROKER'].includes(check.phase) || !['PASS', 'FAIL', 'UNKNOWN', 'BLOCKED'].includes(check.status))) {
        throw new Error('The server returned invalid readiness evidence. Update the backend and rerun checks.');
      }
      return data as ReadinessPreview;
    };
    try {
      const base = `/api/v1/ui/clusters/${encodeURIComponent(clusterId)}/add-node/previews`;
      let data = await accept(await fetch(base, { method: 'POST', signal: abort.signal, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ hostId, brokerRack }) }));
      while (!abort.signal.aborted && currentContext.current === startedContext) {
        setPreview(data); setNow(Date.now());
        if (Date.parse(data.expiresAt) <= Date.now() || ['EXPIRED', 'STALE'].includes(data.status)) break;
        if (data.status !== 'RUNNING') setRunning(false);
        // Continue validating context after completion until evidence expires.
        await new Promise(resolve => window.setTimeout(resolve, data.status === 'RUNNING' ? 1500 : 10000));
        if (abort.signal.aborted || currentContext.current !== startedContext) break;
        data = await accept(await fetch(`${base}/${encodeURIComponent(data.previewId)}`, { signal: abort.signal }));
      }
    } catch (failure) {
      if (!abort.signal.aborted && currentContext.current === startedContext) setError(failure instanceof Error ? failure.message : 'Readiness observation failed.');
    } finally {
      if (!abort.signal.aborted && currentContext.current === startedContext) setRunning(false);
    }
  };

  const fresh = Boolean(visible && !visibleError && now < Date.parse(visible.expiresAt) && !['RUNNING', 'EXPIRED', 'STALE'].includes(visible.status));
  const phasePassed = (phase: 'COMMON' | 'BROKER') => {
    const checks = visible?.checks.filter(check => check.phase === phase) || [];
    const required = phase === 'COMMON' ? 21 : 11;
    const codes = new Set(checks.filter(check => check.status === 'PASS').map(check => check.code));
    return fresh && Array.from({ length: required }, (_, i) => `${phase === 'COMMON' ? 'C' : 'B'}${String(i + 1).padStart(2, '0')}`).every(code => codes.has(code))
      && checks.every(check => check.status === 'PASS');
  };
  return { preview: visible, error: visibleError, running: busy, fresh, expired: Boolean(visible && now >= Date.parse(visible.expiresAt)), commonPassed: phasePassed('COMMON'), brokerPassed: phasePassed('BROKER'), run };
}
