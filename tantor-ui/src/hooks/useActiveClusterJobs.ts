import { useEffect, useState } from 'react';

export interface ActiveClusterJob { id: string; clusterId: string; type: string; status: string; progress: string }
export function useActiveClusterJobs() {
  const [jobs, setJobs] = useState<ActiveClusterJob[]>([]);
  useEffect(() => {
    const abort = new AbortController();
    const poll = async () => {
      try {
        const response = await fetch('/api/v1/ui/jobs/active-clusters', { signal: abort.signal });
        if (response.ok) setJobs(await response.json());
      } catch { /* Keep the last observed lock until the next successful check. */ }
    };
    void poll();
    const interval = window.setInterval(() => void poll(), 3000);
    return () => { abort.abort(); window.clearInterval(interval); };
  }, []);
  return jobs;
}

export function currentJobNode(job: ActiveClusterJob) {
  try { return String(JSON.parse(job.progress).message || 'Waiting for execution.'); }
  catch { return 'Waiting for execution.'; }
}
