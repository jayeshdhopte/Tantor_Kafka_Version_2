import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { liveRefreshIntervals, readLiveRefreshSettings, startLivePolling, type LiveRefreshSettings } from '../utils/liveRefresh';

export function useLiveRefresh({ scope, context = '', refresh, paused = false }: {
  scope: string; context?: string; refresh: (signal: AbortSignal) => Promise<unknown>; paused?: boolean;
}) {
  const key = `tantor:live:${scope}`;
  const saved = useMemo(() => readLiveRefreshSettings(key), [key]);
  const [selection, setSelection] = useState<(LiveRefreshSettings & { key: string }) | null>(null);
  const current = selection?.key === key ? selection : saved;
  const latest = useRef({ refresh, paused });
  useLayoutEffect(() => { latest.current = { refresh, paused }; }, [refresh, paused]);
  useEffect(() => {
    if (!current.enabled || paused) return;
    return startLivePolling(current.seconds, signal => latest.current.refresh(signal),
      () => !latest.current.paused && document.visibilityState === 'visible');
  }, [current.enabled, current.seconds, key, context, paused]);

  const onChange = (enabled: boolean, seconds = current.seconds) => {
    if (!liveRefreshIntervals.some(option => option === seconds)) return;
    const next = { enabled, seconds };
    setSelection({ key, ...next });
    try { window.localStorage.setItem(key, JSON.stringify(next)); }
    catch { /* Refresh still works when preference storage is unavailable. */ }
  };
  return { enabled: current.enabled, seconds: current.seconds, onChange };
}
