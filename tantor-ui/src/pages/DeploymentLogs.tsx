import { useEffect, useRef, useState, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { CheckCircle2, Clock, Copy, Loader2, RefreshCw, Terminal, XCircle, RotateCcw, PlayCircle, Trash2, Download, ChevronDown } from 'lucide-react';
import { retryTask, resumeTask, rollbackTask, cleanupTask } from '../lib/api';
import { confirmAction, notifyAction } from '../components/confirmUtils';
import './DeploymentLogs.css';

interface Task {
  id: string;
  hostId: string;
  command: string;
  status: string;
  logOutput: string;
  errorMsg: string;
  currentStep?: string;
  failedReason?: string;
  stepLogs?: string; // JSON String from backend representing Map<string, string>
  createdAt: string;
  updatedAt: string;
}

interface ClusterInfo {
  status: string;
}

const activeStatus = (status: string) => ['PENDING', 'RUNNING', 'VALIDATING', 'IN_PROGRESS'].includes(String(status).toUpperCase());

const friendlyFailure = (task: Task) => {
  if (task.failedReason && !task.failedReason.includes('Exception') && task.failedReason.length < 500) return task.failedReason;
  const error = `${task.errorMsg || ''} ${task.failedReason || ''}`.toLowerCase();
  if (error.includes('404') || error.includes('download')) return 'Kafka could not be downloaded. Verify the artifact repository URL and network access from this host.';
  if (error.includes('checksum')) return 'The Kafka package failed its integrity check. Distribute the binary again and retry.';
  if (error.includes('permission denied')) return 'The agent lacks permission to write files or manage the Kafka service.';
  if (error.includes('no space left')) return 'The target host does not have enough disk space.';
  if (error.includes('systemctl') || error.includes('service')) return 'Kafka files were prepared, but the Kafka service could not start.';
  if (error.includes('port') || error.includes('listening')) return 'Kafka did not become reachable on its configured port.';
  return `Deployment stopped during ${task.currentStep || 'an unknown step'}. Review the technical log below.`;
};

export function DeploymentLogs() {
  const { id } = useParams<{ id: string }>();
  const [cluster, setCluster] = useState<ClusterInfo | null>(null);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [selectedTaskId, setSelectedTaskId] = useState('');
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [copyNotice, setCopyNotice] = useState('');
  const [actionLoading, setActionLoading] = useState(false);
  const [isConsoleMaximized, setIsConsoleMaximized] = useState(false);
  const logBodyRef = useRef<HTMLDivElement>(null);
  const copyNoticeTimerRef = useRef<number | null>(null);

  const fetchTasks = useCallback(async (manual = false) => {
    if (manual) setRefreshing(true);
    try {
      const [clusterRes, tasksRes] = await Promise.all([
        fetch(`/api/v1/ui/clusters/${id}`, { cache: 'no-store' }),
        fetch(`/api/v1/ui/clusters/${id}/tasks`, { cache: 'no-store' }),
      ]);
      if (clusterRes.ok) setCluster(await clusterRes.json());
      if (tasksRes.ok) {
        const nextTasks: Task[] = await tasksRes.json();
        const availableTasks = Array.isArray(nextTasks) ? nextTasks : [];
        setTasks(availableTasks);
        setSelectedTaskId(current => current && availableTasks.some(task => task.id === current) ? current : availableTasks[0]?.id || '');
      } else {
        throw new Error('Tasks request failed');
      }
    } catch (error) {
      console.error('Failed to load deployment logs', error);
      setTasks([]);
      setSelectedTaskId('');
    } finally {
      setLoading(false);
      if (manual) setRefreshing(false);
    }
  }, [id]);

  useEffect(() => {
    void (async () => { await fetchTasks(); })();
  }, [fetchTasks]);

  const shouldPoll = tasks.some(task => activeStatus(task.status))
    || ['PENDING', 'RUNNING', 'VALIDATING', 'DELETING'].includes(cluster?.status || '');

  useEffect(() => {
    if (!shouldPoll) return;
    const interval = window.setInterval(() => { fetchTasks(); }, 3000);
    return () => window.clearInterval(interval);
  }, [fetchTasks, shouldPoll]);

  const selectedTask = tasks.find(task => task.id === selectedTaskId) || tasks[0];

  useEffect(() => {
    if (logBodyRef.current) logBodyRef.current.scrollTop = logBodyRef.current.scrollHeight;
  }, [selectedTask?.logOutput, selectedTask?.stepLogs]);

  useEffect(() => () => {
    if (copyNoticeTimerRef.current !== null) window.clearTimeout(copyNoticeTimerRef.current);
  }, []);

  const handleCopyLogs = async () => {
    if (!selectedTask) return;
    try {
      await navigator.clipboard.writeText(selectedTask.logOutput || selectedTask.errorMsg || '');
      setCopyNotice('Copied to clipboard');
      if (copyNoticeTimerRef.current !== null) window.clearTimeout(copyNoticeTimerRef.current);
      copyNoticeTimerRef.current = window.setTimeout(() => setCopyNotice(''), 2500);
    } catch (error) {
      console.error('Failed to copy deployment logs', error);
      notifyAction('Unable to copy logs to the clipboard.');
    }
  };

  const statusIcon = (status: string) => {
    if (status === 'SUCCESS') return <CheckCircle2 size={15} />;
    if (status === 'FAILED') return <XCircle size={15} />;
    if (activeStatus(status)) return <Loader2 size={15} className="spin" />;
    return <Clock size={15} />;
  };

  if (loading && tasks.length === 0) {
    return <div className="state-center"><Loader2 className="spin" /> Loading deployment logs...</div>;
  }

  if (!selectedTask) {
    return (
      <div className="empty-state deployment-log-empty">
        <Terminal size={32} />
        <h3>No deployment logs</h3>
        <p>No task has been executed for this cluster.</p>
      </div>
    );
  }

  const handleRetry = async () => {
    if (!id || !selectedTask) return;
    setActionLoading(true);
    try {
      await retryTask(id, selectedTask.id);
      fetchTasks();
    } catch (e) {
      console.error(e);
      notifyAction("Failed to retry task.");
    } finally {
      setActionLoading(false);
    }
  };

  const handleRollback = async () => {
    if (!id || !selectedTask) return;
    if (!(await confirmAction("Are you sure you want to rollback this deployment? (Services will be stopped but logs and configs remain)"))) return;
    
    setActionLoading(true);
    try {
      await rollbackTask(id, selectedTask.id);
      fetchTasks();
    } catch (e) {
      console.error(e);
      notifyAction("Failed to trigger rollback.");
    } finally {
      setActionLoading(false);
    }
  };

  const handleResume = async () => {
    if (!id || !selectedTask) return;
    setActionLoading(true);
    try {
      await resumeTask(id, selectedTask.id);
      fetchTasks();
    } catch (e) {
      console.error(e);
      notifyAction("Failed to resume task.");
    } finally {
      setActionLoading(false);
    }
  };

  const handleCleanup = async () => {
    if (!id || !selectedTask) return;
    if (!(await confirmAction("Are you sure you want to completely clean up this deployment? (All files and logs on the node will be deleted)"))) return;
    
    setActionLoading(true);
    try {
      await cleanupTask(id, selectedTask.id);
      fetchTasks();
    } catch (e) {
      console.error(e);
      notifyAction("Failed to trigger cleanup.");
    } finally {
      setActionLoading(false);
    }
  };

  const handleDownloadLogs = () => {
    if (!selectedTask) return;
    const content = selectedTask.logOutput || selectedTask.errorMsg || '';
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `tantor-deployment-${selectedTask.id}.log`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const isFailed = selectedTask.status === 'FAILED';
  const renderLogs = (logsText: string) => {
    if (!logsText) return 'No output recorded.';
    return logsText.split('\n').map((line, idx) => {
      let className = 'log-line';
      const lowerLine = line.toLowerCase();
      if (lowerLine.includes('completed')) {
        className += ' log-success';
      } else if (lowerLine.includes('failed') || lowerLine.includes('error')) {
        className += ' log-error';
      }
      return <div key={idx} className={className}>{line || ' '}</div>;
    });
  };

  return (
    <div className="deployment-log-view animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-4)', alignSelf: 'stretch' }}>
      
      {/* Title Row with Task output & refresh icon */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
          <h2 style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-bold)', fontSize: 'var(--text-md)', lineHeight: '22px', color: 'var(--text-primary)', margin: 0 }}>Task output</h2>
          <span style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', lineHeight: '19px', color: 'var(--text-tertiary)' }}>
            {tasks.length} task{tasks.length === 1 ? '' : 's'} recorded
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
          <button 
            onClick={handleCopyLogs}
            style={{
              boxSizing: 'border-box',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: 'var(--space-2)',
              width: '40px',
              height: '40px',
              border: '1px solid var(--border-default)',
              borderRadius: 'var(--radius-md)',
              background: "var(--bg-surface)",
              cursor: 'pointer'
            }}
            title="Copy logs"
          >
            <Copy size={16} style={{ color: 'var(--text-tertiary)' }} />
          </button>
          <button 
            onClick={() => fetchTasks(true)}
            disabled={loading || refreshing}
            style={{
              boxSizing: 'border-box',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: 'var(--space-2)',
              width: '40px',
              height: '40px',
              border: '1px solid var(--border-default)',
              borderRadius: 'var(--radius-md)',
              background: "var(--bg-surface)",
              cursor: 'pointer'
            }}
            title="Refresh"
          >
            <RefreshCw size={16} className={loading || refreshing ? 'spin' : ''} style={{ color: 'var(--text-tertiary)' }} />
          </button>
        </div>
      </div>

      {copyNotice && (
        <div className="deployment-copy-toast" role="status" aria-live="polite">
          <CheckCircle2 size={16} />
          {copyNotice}
        </div>
      )}

      {/* Task dropdown selector */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-4)', width: '100%' }}>
        <span style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-bold)', fontSize: 'var(--text-md)', lineHeight: '22px', color: 'var(--text-primary)', width: '33px' }}>
          Task
        </span>
        <div style={{ position: 'relative', flexGrow: 1 }}>
          <select 
            id="deployment-task" 
            value={selectedTask.id} 
            onChange={event => setSelectedTaskId(event.target.value)}
            style={{
              boxSizing: 'border-box',
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
              padding: '10px 16px',
              width: '100%',
              height: '40px',
              background: "var(--bg-surface)",
              border: '1px solid var(--border-focus)',
              borderRadius: 'var(--radius-md)',
              fontFamily: 'Satoshi, sans-serif',
              fontWeight: 'var(--font-regular)',
              fontSize: 'var(--text-base)',
              color: 'var(--border-focus)',
              appearance: 'none',
              cursor: 'pointer'
            }}
          >
            {tasks.map(task => (
              <option key={task.id} value={task.id}>
                {`${task.command} - ${task.hostId} - ${task.status}`}
              </option>
            ))}
          </select>
          <div style={{ position: 'absolute', right: '16px', top: '10px', pointerEvents: 'none', color: 'var(--border-focus)' }}>
            <ChevronDown size={20} />
          </div>
        </div>
      </div>

      {/* Metadata Table */}
      <div style={{
        boxSizing: 'border-box',
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        border: '1px solid var(--border-default)',
        borderRadius: 'var(--radius-md)',
        overflow: 'hidden'
      }}>
        {/* Table Header */}
        <div style={{ display: 'flex', background: 'var(--bg-neutral-light)', borderBottom: '1px solid var(--border-default)', height: '54px', alignItems: 'center' }}>
          <div style={{ width: '56px', padding: 'var(--space-4)', display: 'flex', alignItems: 'center', justifyContent: 'center' }} />
          <div style={{ flex: 1, padding: 'var(--space-4)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', color: 'var(--button-primary-active)' }}>Status</div>
          <div style={{ flex: 1, padding: 'var(--space-4)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', color: 'var(--button-primary-active)' }}>Host</div>
          <div style={{ flex: 1, padding: 'var(--space-4)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', color: 'var(--button-primary-active)' }}>Started</div>
          <div style={{ flex: 1, padding: 'var(--space-4)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', color: 'var(--button-primary-active)' }}>Updated</div>
        </div>
        {/* Table Body Row */}
        <div 
          style={{ display: 'flex', background: "var(--bg-surface)", height: '52px', alignItems: 'center', cursor: 'default' }}
        >
          <div style={{ width: '56px', padding: '14px 16px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-tertiary)' }}>
            {statusIcon(selectedTask.status)}
          </div>
          <div style={{ flex: 1, padding: '14px 16px', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', color: 'var(--text-heading)' }}>
            {selectedTask.status}
          </div>
          <div style={{ flex: 1, padding: '14px 16px', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', color: 'var(--text-heading)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={selectedTask.hostId}>
            {selectedTask.hostId}
          </div>
          <div style={{ flex: 1, padding: '14px 16px', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', color: 'var(--text-heading)' }}>
            {new Date(selectedTask.createdAt).toLocaleString()}
          </div>
          <div style={{ flex: 1, padding: '14px 16px', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', color: 'var(--text-heading)' }}>
            {new Date(selectedTask.updatedAt).toLocaleString()}
          </div>
        </div>
      </div>

      {/* Full width Log CodeBlock console */}
      <div style={{ display: 'flex', flexDirection: 'column', width: '100%' }}>
        {/* Dark purple header bar */}
        <div style={{
          display: 'flex',
          justifyContent: 'flex-end',
          alignItems: 'center',
          padding: '10px 20px',
          height: '44px',
          background: 'var(--button-primary-active)',
          borderRadius: '8px 8px 0px 0px'
        }}>
          <button 
            onClick={() => setIsConsoleMaximized(!isConsoleMaximized)}
            style={{
              background: 'none',
              border: 'none',
              cursor: 'pointer',
              color: "var(--text-light)",
              display: 'flex',
              alignItems: 'center',
              padding: 0
            }}
            title={isConsoleMaximized ? "Collapse logs" : "Expand logs"}
          >
            {isConsoleMaximized ? (
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ color: "var(--text-light)" }}>
                <path d="M13 7v4h4" />
                <path d="M11 17v-4H7" />
              </svg>
            ) : (
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ color: "var(--text-light)" }}>
                <path d="M17 11V7H13" />
                <path d="M13 15V11H9" />
              </svg>
            )}
          </button>
        </div>
        
        {/* Log body */}
        <div style={{
          boxSizing: 'border-box',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'flex-start',
          padding: '20px',
          width: '100%',
          minHeight: isConsoleMaximized ? '650px' : '245px',
          maxHeight: isConsoleMaximized ? 'none' : '400px',
          overflowY: 'auto',
          background: '#000000',
          borderRadius: '0px 0px 16px 16px'
        }} ref={logBodyRef}>
          <div className="logs-text" style={{
            width: '100%',
            fontFamily: 'Source Code Pro, monospace',
            fontSize: 'var(--text-base)',
            lineHeight: '20px',
            color: "var(--text-light)",
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-all'
          }}>
            {renderLogs(selectedTask.logOutput || selectedTask.errorMsg || '')}
          </div>
        </div>
      </div>

      {/* Action buttons if failed */}
      {isFailed && selectedTask.command === 'INSTALL_KAFKA' && (
        <div className="deployment-action-bar">
           <button className="btn-primary" onClick={handleRetry} disabled={actionLoading}>
             {actionLoading ? <Loader2 size={16} className="spin" /> : <RefreshCw size={16} />}
             Retry
           </button>
           <button className="btn-secondary" onClick={handleResume} disabled={actionLoading}>
             {actionLoading ? <Loader2 size={16} className="spin" /> : <PlayCircle size={16} />}
             Resume
           </button>
           <button className="btn-warning" onClick={handleRollback} disabled={actionLoading}>
             {actionLoading ? <Loader2 size={16} className="spin" /> : <RotateCcw size={16} />}
             Rollback
           </button>
           <button className="btn-danger" onClick={handleCleanup} disabled={actionLoading}>
             {actionLoading ? <Loader2 size={16} className="spin" /> : <Trash2 size={16} />}
             Cleanup
           </button>
           <button className="btn-secondary" onClick={handleDownloadLogs}>
             <Download size={16} /> Download Logs
           </button>
        </div>
      )}

      {/* Technical details if error */}
      {selectedTask.errorMsg && (
        <div className="deployment-task-error">
          <strong>What happened</strong>
          <span>{friendlyFailure(selectedTask)}</span>
          {selectedTask.currentStep && <small>Failed step: {selectedTask.currentStep}</small>}
          <details>
            <summary>Technical error</summary>
            <pre>{selectedTask.errorMsg}</pre>
          </details>
        </div>
      )}



    </div>
  );
}
