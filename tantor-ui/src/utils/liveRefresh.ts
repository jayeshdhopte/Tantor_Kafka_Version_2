export const liveRefreshIntervals = [5, 10, 15, 30, 60] as const;
export type LiveRefreshSettings = { enabled: boolean; seconds: number };

export function readLiveRefreshSettings(key: string): LiveRefreshSettings {
  try {
    const saved = JSON.parse(window.localStorage.getItem(key) || 'null');
    if (saved && liveRefreshIntervals.some(seconds => seconds === saved.seconds)) {
      return { enabled: saved.enabled === true, seconds: saved.seconds };
    }
  } catch { /* Storage can be unavailable in restricted browser sessions. */ }
  return { enabled: false, seconds: 15 };
}

export function startLivePolling(seconds: number, refresh: (signal: AbortSignal) => Promise<unknown>, canRefresh: () => boolean) {
  let stopped = false;
  let request: AbortController | null = null;
  const timer = window.setInterval(async () => {
    if (stopped || request || !canRefresh()) return;
    const controller = new AbortController();
    request = controller;
    try {
      await refresh(controller.signal);
    } catch { /* Page loaders report errors; a later tick can retry. */ }
    finally { if (request === controller) request = null; }
  }, seconds * 1000);
  return () => {
    stopped = true;
    window.clearInterval(timer);
    request?.abort();
  };
}
