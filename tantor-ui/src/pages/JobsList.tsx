import { useEffect, useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { RefreshCw } from 'lucide-react';
import './JobsList.css';

type Job = {
  id: string;
  type: string;
  status: string;
  startTime: string;
  endTime: string;
  createdAt: string;
};

export function JobsList() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true); // tracks initial load
  const [refreshing, setRefreshing] = useState(false); // tracks manual/auto refreshes
  const navigate = useNavigate();

  const fetchJobs = useCallback(async (isManual = false) => {
    if (isManual) {
      setRefreshing(true);
    } else {
      setLoading(true);
    }
    try {
      const res = await fetch('/api/v1/ui/jobs');
      if (res.ok) {
        const data = await res.json();
        setJobs(data);
      }
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void (async () => { await fetchJobs(false); })();
    const interval = setInterval(() => { void (async () => { await fetchJobs(true); })(); }, 5000);
    return () => clearInterval(interval);
  }, [fetchJobs]);

  const formatStatus = (status: string) => {
    switch (status.toUpperCase().replace(' - ', '_').replace(' ', '_')) {
      case 'SUCCESS': return 'Success';
      case 'FAILED': return 'Failed';
      case 'IN_PROGRESS': return 'In - progress';
      case 'PARTIAL_SUCCESS': return 'Partial Success';
      case 'ROLLED_BACK': return 'Rolled Back';
      default: return status.replace('_', ' ');
    }
  };

  const getStatusClass = (status: string) => {
    switch (status.toUpperCase().replace(' - ', '_').replace(' ', '_')) {
      case 'SUCCESS': return 'badge-success';
      case 'FAILED': return 'badge-failed';
      case 'IN_PROGRESS': return 'badge-in-progress';
      case 'PARTIAL_SUCCESS': return 'badge-warning';
      case 'ROLLED_BACK': return 'badge-rolled-back';
      default: return 'badge-pending';
    }
  };

  return (
    <div className="jobs-list-page animate-fade-in">
      <header className="page-header-jobs">
        <div className="header-titles">
          <h2>Job History</h2>
          <p>Track all asynchronous operations in the cluster.</p>
        </div>
        <button className="refresh-btn" onClick={() => fetchJobs(true)} disabled={loading || refreshing} title="Refresh">
          <RefreshCw size={14} className={refreshing ? 'spin' : ''} />
        </button>
      </header>

      {loading && jobs.length === 0 ? (
        <div className="loading-state">
          <RefreshCw className="spin" size={32} />
          <p>Loading jobs...</p>
        </div>
      ) : (
        <div className="table-container">
          <table className="jobs-table">
            <thead>
              <tr>
                <th>Job ID</th>
                <th>Job Type</th>
                <th>Status</th>
                <th>Started</th>
                <th>Ended</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map(job => (
                <tr key={job.id}>
                  <td>{job.id.slice(0, 8)}</td>
                  <td className="job-type">
                    {job.type.charAt(0).toUpperCase() + job.type.slice(1).toLowerCase()}
                  </td>
                  <td>
                    <div className={`status-pill ${getStatusClass(job.status)}`}>
                      {formatStatus(job.status)}
                    </div>
                  </td>
                  <td>{job.startTime ? new Date(job.startTime).toLocaleString('en-GB', { month: '2-digit', day: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' }).replace(',', ', ') : 'N/A'}</td>
                  <td>{job.endTime ? new Date(job.endTime).toLocaleString('en-GB', { month: '2-digit', day: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' }).replace(',', ', ') : 'N/A'}</td>
                  <td>
                    <button className="btn-outline-primary" onClick={() => navigate(`/jobs/${job.id}`)}>
                      View Details
                    </button>
                  </td>
                </tr>
              ))}
              {jobs.length === 0 && (
                <tr>
                  <td colSpan={6} className="empty-state">No jobs found.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
