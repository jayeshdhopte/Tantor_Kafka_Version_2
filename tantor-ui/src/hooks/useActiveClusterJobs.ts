import { useEffect, useState } from 'react';

export interface ActiveClusterJob { id: string; clusterId: string; type: string; operation?: string; status: string; progress: string }

export function locksClusterView(job: ActiveClusterJob) {
  return job.type === 'ROLLING_RESTART' || job.type === 'EXTERNAL_ROLLING_RESTART'
    || job.type === 'EXTERNAL_SERVICES' || job.operation === 'ADD_SERVICE';
}

export function clusterJobLabel(job: ActiveClusterJob) {
  return job.operation === 'ADD_SERVICE' || job.type === 'EXTERNAL_SERVICES' ? 'Adding service' : 'Rolling restart';
}
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
