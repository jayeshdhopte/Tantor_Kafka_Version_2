import { useEffect, useRef } from 'react';

// Schedule after completion so slow reads cannot accumulate overlapping polls.
export function useAutoRefresh(refresh: () => Promise<unknown> | void, seconds: number, enabled: boolean, context: string) {
  const latest = useRef(refresh);
  useEffect(() => { latest.current = refresh; }, [refresh]);
  useEffect(() => {
    if (!seconds || !enabled) return;
    let cancelled = false;
    let timer: number;
    const schedule = () => { timer = window.setTimeout(() => { void tick(); }, seconds * 1000); };
    const tick = async () => {
      try { if (!cancelled && document.visibilityState !== 'hidden') await latest.current(); }
      catch { /* The page owns its error message; continue polling after failures. */ }
      finally { if (!cancelled) schedule(); }
    };
    schedule();
    return () => { cancelled = true; window.clearTimeout(timer); };
  }, [seconds, enabled, context]);
}

