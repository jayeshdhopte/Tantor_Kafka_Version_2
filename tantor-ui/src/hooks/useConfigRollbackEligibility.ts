import { useCallback, useEffect, useState } from 'react';

type Eligibility = { jobId: string; allowed: boolean; reason: string };

/** A terminal failed job still needs fresh recovery eligibility after other jobs finish. */
export function useConfigRollbackEligibility(jobId: string | undefined, enabled: boolean) {
  const [result, setResult] = useState<Eligibility | null>(null);
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(() => setRevision(value => value + 1), []);

  useEffect(() => {
    if (!jobId || !enabled) return;
    const controller = new AbortController();
    let loading = false;
    const load = async () => {
      if (loading) return;
      loading = true;
      try {
        const response = await fetch(`/api/v1/ui/jobs/${jobId}/rollback-eligibility`, { signal: controller.signal });
        if (!response.ok) throw new Error('Unable to check rollback eligibility.');
        const data = await response.json() as { allowed?: unknown; reason?: unknown };
        if (typeof data.allowed !== 'boolean' || typeof data.reason !== 'string'
          || (!data.allowed && !data.reason.trim())) throw new Error('Invalid rollback eligibility.');
        if (!controller.signal.aborted) setResult({ jobId, allowed: data.allowed, reason: data.reason });
      } catch {
        if (!controller.signal.aborted) setResult({ jobId, allowed: false,
          reason: 'Unable to verify rollback eligibility. Refresh this page before retrying.' });
      } finally { loading = false; }
    };
    void load();
    const timer = window.setInterval(() => { void load(); }, 10000);
    const onFocus = () => { void load(); };
    window.addEventListener('focus', onFocus);
    return () => {
      controller.abort();
      window.clearInterval(timer);
      window.removeEventListener('focus', onFocus);
    };
  }, [jobId, enabled, revision]);

  if (!enabled) return { allowed: true, reason: '', refresh };
  return result?.jobId === jobId ? { ...result, refresh }
    : { allowed: false, reason: 'Checking whether this configuration rollback is still valid...', refresh };
}
