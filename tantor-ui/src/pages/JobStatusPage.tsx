import { useEffect, useState, useRef, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, RefreshCw, AlertTriangle, Undo2, Check, MoreVertical } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { confirmAction } from '../components/confirmUtils';
import './JobStatusPage.css';

type Job = {
  id: string;
  type: string;
  status: string;
  requestedBy: string;
  startTime: string;
  endTime: string;
  progress: string;
  logs: string;
  rollbackSupported: boolean;
  retryCount: number;
};

type JobStep = {
  id: string;
  stepOrder: number;
  name: string;
  targetId: string;
  status: string;
  agentTaskId?: string;
  logs?: string;
  retryCount: number;
  startTime?: string;
  endTime?: string;
};

type Host = {
  id: string;
  agentName?: string;
  hostname?: string;
  ipAddresses?: string;
};

type HostDisplay = {
  name: string;
  ip: string;
};

const UUID_PATTERN = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';

function getFirstIp(ipAddresses?: string): string {
  if (!ipAddresses) return '';
  try {
    const parsed = JSON.parse(ipAddresses);
    if (Array.isArray(parsed)) return String(parsed[0] || '');
  } catch {
    // Fall back to a plain IP value if this server does not return JSON.
  }
  return ipAddresses.replace(/[[\]"]/g, '').split(',')[0]?.trim() || '';
}

function getBusinessStepName(rawName: string): string {
  if (!rawName) return '';

  if (rawName.startsWith('Deploy ')) {
    const match = rawName.match(/Deploy (.+) node (\d+)/i);
    if (match) {
      let role = match[1].toLowerCase();
      if (role === 'broker_controller') role = 'Broker/Controller';
      else if (role === 'broker') role = 'Broker';
      else if (role === 'controller') role = 'Controller';
      else if (role === 'zookeeper') role = 'ZooKeeper';
      else role = role.charAt(0).toUpperCase() + role.slice(1);
      
      return `Provision ${role} Node ${match[2]}`;
    }
    return 'Provision Kafka Node';
  }

  if (rawName.startsWith('Check controller connectivity')) {
    return 'Verify Internal Connectivity';
  }

  if (rawName.includes('Verify ZooKeeper quorum')) {
    return 'Validate ZooKeeper Quorum Health';
  }

  if (rawName.includes('Verify KRaft leader')) {
    return 'Validate KRaft Cluster Health';
  }

  if (rawName === 'PREFLIGHT') return 'Preflight Validation';
  if (rawName === 'BACKUP_ALL') return 'Global Configuration Backup';
  if (rawName === 'WRITE_CONFIG') return 'Write Configuration';
  if (rawName === 'RESTART_SERVICE') return 'Restart Service';
  if (rawName === 'HEALTH_CHECK') return 'Health Check';
  if (rawName === 'ROLLBACK') return 'Rollback Configuration';
  if (rawName === 'FINAL_VERIFY') return 'Final Verification';

  return rawName;
}

export function JobStatusPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const { canManage } = usePermissions();
  const [job, setJob] = useState<Job | null>(null);
  const [steps, setSteps] = useState<JobStep[]>([]);
  const [hostsById, setHostsById] = useState<Record<string, HostDisplay>>({});
  const [loading, setLoading] = useState(true);
  const [isLogsExpanded, setIsLogsExpanded] = useState(false);
  const [showLogs, setShowLogs] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  const logsEndRef = useRef<HTMLDivElement>(null);

  const fetchJob = useCallback(async () => {
    try {
      const res = await fetch(`/api/v1/ui/jobs/${id}`);
      if (res.ok) {
        setJob(await res.json());
      }
      const stepsRes = await fetch(`/api/v1/ui/jobs/${id}/steps`);
      if (stepsRes.ok) setSteps(await stepsRes.json());
      const hostsRes = await fetch('/api/v1/ui/hosts');
      if (hostsRes.ok) {
        const hosts: Host[] = await hostsRes.json();
        setHostsById(Object.fromEntries(hosts.map(host => [
          host.id,
          {
            name: host.hostname || host.agentName || host.id,
            ip: getFirstIp(host.ipAddresses)
          }
        ])));
      }
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    void (async () => { await fetchJob(); })();
    const interval = setInterval(() => {
      if (!['SUCCESS', 'FAILED', 'PARTIAL_SUCCESS', 'ROLLED_BACK', 'ROLLBACK_FAILED'].includes(job?.status || '')) {
        void (async () => { await fetchJob(); })();
      }
    }, 2000);
    return () => clearInterval(interval);
  }, [fetchJob, job?.status]);

  useEffect(() => {
    if (logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [job?.logs, isLogsExpanded]);

  const handleRetry = async () => {
    try {
      const res = await fetch(`/api/v1/ui/jobs/${id}/retry`, { method: 'POST' });
      if (res.ok) fetchJob();
    } catch (err) {
      console.error(err);
    }
  };

  const handleRollback = async () => {
    if (!(await confirmAction('Rollback all successfully completed steps for this job?'))) return;
    try {
      const res = await fetch(`/api/v1/ui/jobs/${id}/rollback`, { method: 'POST' });
      if (res.ok) fetchJob();
    } catch (err) {
      console.error(err);
    }
  };

  if (loading && !job) {
    return (
      <div className="job-status-page loading-state">
        <RefreshCw className="spin" size={32} />
        <p>Loading job details...</p>
      </div>
    );
  }

  if (!job) {
    return (
      <div className="job-status-page">
        <button className="btn btn-ghost" onClick={() => navigate('/jobs')}><ArrowLeft size={16}/> Back to Jobs</button>
        <h2>Job not found</h2>
      </div>
    );
  }

  const isFinished = ['SUCCESS', 'FAILED', 'PARTIAL_SUCCESS', 'ROLLED_BACK', 'ROLLBACK_FAILED'].includes(job.status);
  
  let displaySteps = steps;
  
  if (steps.length === 0 && job?.logs) {
    const reconstructed: JobStep[] = [];
    const lines = job.logs.split('\n');
    let order = 0;
    
    for (const line of lines) {
      const compMatch = line.match(/\]\s*(.+?)\s+completed/);
      if (compMatch) {
        const targetMatch = line.match(new RegExp(`\\b(?:on|from)\\s+(${UUID_PATTERN})\\b`, 'i'));
        reconstructed.push({
          id: `recon-${order}`,
          stepOrder: order++,
          name: compMatch[1],
          targetId: targetMatch?.[1] || '',
          status: 'SUCCESS',
          retryCount: 0
        });
      }
      
      const failMatch = line.match(/\]\s*Job execution failed:\s*(.+)/);
      if (failMatch) {
        reconstructed.push({
          id: `recon-${order}`,
          stepOrder: order++,
          name: failMatch[1].length > 45 ? failMatch[1].substring(0, 45) + '...' : failMatch[1],
          targetId: '',
          status: 'FAILED',
          retryCount: 0
        });
      }
    }
    
    if (reconstructed.length > 0) {
      displaySteps = reconstructed;
    }
  }

  const formatStatus = (status: string) => {
    switch (status) {
      case 'SUCCESS': return 'Success';
      case 'FAILED': return 'Failed';
      case 'IN_PROGRESS': return 'In - progress';
      case 'PARTIAL_SUCCESS': return 'Partial Success';
      case 'ROLLED_BACK': return 'Rolled Back';
      default: return status.replace('_', ' ');
    }
  };

  const getStatusClass = (status: string) => {
    switch (status) {
      case 'SUCCESS': return 'badge-success';
      case 'FAILED': return 'badge-failed';
      case 'IN_PROGRESS': return 'badge-in-progress';
      case 'PARTIAL_SUCCESS': return 'badge-warning';
      case 'ROLLED_BACK': return 'badge-rolled-back';
      default: return 'badge-pending';
    }
  };

  const getStepIcon = (status: string, size = 20) => {
    const isCompleted = ['SUCCESS', 'ROLLED_BACK'].includes(status);
    const isFailed = ['FAILED', 'ROLLBACK_FAILED'].includes(status);
    const isRunning = ['IN_PROGRESS', 'ROLLING_BACK'].includes(status);
    
    let bgColor = 'var(--border-default)'; // Pending / Default
    if (isCompleted) bgColor = '#1F845A';
    else if (isFailed) bgColor = 'var(--color-danger)';
    else if (isRunning) bgColor = 'var(--button-primary)';

    if (isRunning) {
      return <RefreshCw className="spin step-icon-progress" size={20} style={{ color: 'var(--button-primary)', flexShrink: 0 }} />;
    }

    return (
      <div style={{
        width: `${size}px`,
        height: `${size}px`,
        borderRadius: '64px',
        background: bgColor,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexShrink: 0
      }}>
        <Check size={size - 8} color="#FFFFFF" strokeWidth={3} />
      </div>
    );
  };

  const renderLogs = (logsText: string) => {
    if (!logsText) return 'Waiting for execution logs...';
    return logsText.split('\n').map((line, lineIndex) => {
      let className = 'log-line';
      const lowerLine = line.toLowerCase();
      if (lowerLine.includes('completed')) {
        className += ' log-success';
      } else if (lowerLine.includes('failed') || lowerLine.includes('error')) {
        className += ' log-error';
      }
      const displayLine = line.replace(
        new RegExp(UUID_PATTERN, 'gi'),
        targetId => {
          const host = hostsById[targetId];
          if (!host) return targetId;
          return host.ip ? `${host.name} (${host.ip})` : host.name;
        }
      );
      return <div key={lineIndex} className={className}>{displayLine || ' '}</div>;
    });
  };

  const getStepDisplayName = (step: JobStep): string => {
    const businessName = getBusinessStepName(step.name);
    const targetId = step.targetId || step.name.match(new RegExp(UUID_PATTERN, 'i'))?.[0];
    if (!targetId) return businessName;

    const host = hostsById[targetId];
    if (!host) return businessName;
    const hostLabel = host.ip ? `${host.name} (${host.ip})` : host.name;
    return `${businessName} - ${hostLabel}`;
  };

  const totalSteps = displaySteps.length;
  const completedStepsCount = displaySteps.filter(s => ['SUCCESS', 'ROLLED_BACK'].includes(s.status)).length;

  return (
    <div className="job-status-page animate-fade-in">
      <div className="page-header-actions">
        <div className="back-nav" onClick={() => navigate('/jobs')} style={{ cursor: 'pointer' }}>
          <ArrowLeft size={16} /> 
          <div className="back-text">
            <span className="back-label">Job ID</span>
            <span className="back-id">{job.id.slice(0, 8)}</span>
          </div>
        </div>
        <div className="action-buttons">
          {canManage && (job.status === 'FAILED' || job.status === 'PARTIAL_SUCCESS') && (
            <button className="btn btn-outline-primary" onClick={handleRetry}>
              <RefreshCw size={14} style={{ marginRight: '6px' }} /> Retry Job
            </button>
          )}
          {canManage && job.rollbackSupported && ['SUCCESS', 'FAILED', 'PARTIAL_SUCCESS'].includes(job.status) && (
            <button className="btn btn-outline-primary" onClick={handleRollback}>
              <Undo2 size={14} style={{ marginRight: '6px' }} /> Rollback
            </button>
          )}
        </div>
      </div>
      
      {job.status === 'FAILED' && steps.some(s => s.name === 'ROLLBACK' && s.status === 'SUCCESS') && (
        <div className="overview-alert error" style={{ marginBottom: '16px' }}>
          <AlertTriangle size={17} />
          <span>Previously successful nodes remain changed. Failed node was rolled back.</span>
        </div>
      )}

      <div className="job-summary-wrapper">
        <div className="summary-bar-card">
          <div className="summary-col">
            <div className="summary-label">Status</div>
            <div className={`status-pill ${getStatusClass(job.status)}`}>
              {formatStatus(job.status)}
            </div>
          </div>
          <div className="summary-divider" />
          <div className="summary-col">
            <div className="summary-label">Job ID</div>
            <div className="summary-value truncate-id">{job.id}</div>
          </div>
          <div className="summary-divider" />
          <div className="summary-col">
            <div className="summary-label">Started</div>
            <div className="summary-value date-val">
              {job.startTime ? new Date(job.startTime).toLocaleString() : '-'}
            </div>
          </div>
          <div className="summary-divider" />
          <div className="summary-col">
            <div className="summary-label">Updated</div>
            <div className="summary-value date-val">
              {job.endTime ? new Date(job.endTime).toLocaleString() : (job.startTime ? new Date(job.startTime).toLocaleString() : '-')}
            </div>
          </div>
        </div>
      </div>

      {!isLogsExpanded && (
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
          <h3 className="panel-title" style={{ textAlign: 'left', margin: 0, color: 'var(--button-primary)', fontSize: '18px', fontWeight: 'var(--font-semibold)' }}>Deployment Steps</h3>
          <div style={{ position: 'relative' }}>
            <button 
              onClick={() => setShowDropdown(!showDropdown)} 
              style={{ 
                background: "var(--bg-surface)", 
                border: '1px solid var(--border-default)', 
                borderRadius: 'var(--radius-md)', 
                width: '32px', 
                height: '32px', 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'center', 
                cursor: 'pointer' 
              }}
            >
              <MoreVertical size={16} color="var(--text-tertiary)" />
            </button>
            {showDropdown && (
              <div style={{ 
                position: 'absolute', 
                right: 0, 
                top: '36px', 
                background: "var(--bg-surface)", 
                border: '1px solid var(--border-default)', 
                borderRadius: 'var(--radius-md)', 
                boxShadow: '0 2px 8px rgba(0,0,0,0.1)', 
                zIndex: 10,
                minWidth: '120px'
              }}>
                <button 
                  onClick={() => { setShowLogs(!showLogs); setShowDropdown(false); }} 
                  style={{ 
                    width: '100%', 
                    padding: '8px 16px', 
                    textAlign: 'left', 
                    background: 'none', 
                    border: 'none', 
                    cursor: 'pointer',
                    fontFamily: 'Satoshi',
                    fontSize: 'var(--text-base)',
                    color: 'var(--button-primary-active)'
                  }}
                >
                  {showLogs ? 'Hide logs' : 'View logs'}
                </button>
              </div>
            )}
          </div>
        </div>
      )}
      <div className={`job-main-layout ${isLogsExpanded ? 'logs-expanded' : ''} ${!showLogs ? 'full-width-steps' : ''}`}>
        {!isLogsExpanded && (
          <div className="job-sidebar" style={{ marginLeft: !showLogs ? 0 : '16px', width: !showLogs ? '100%' : 'auto' }}>
            <div style={{
              border: '1px solid var(--border-default)',
              borderRadius: 'var(--radius-md)',
              padding: 'var(--space-4)',
              background: "var(--bg-surface)",
              display: 'flex',
              flexDirection: 'column',
              gap: 'var(--space-4)',
              boxSizing: 'border-box',
              marginLeft: '16px'
            }}>
              {totalSteps > 0 && (
                <div style={{ textAlign: 'left', fontSize: 'var(--text-xs)', lineHeight: '16px', color: 'var(--text-tertiary)', fontFamily: 'Satoshi', fontWeight: 'var(--font-medium)' }}>
                  {completedStepsCount} of {totalSteps} steps
                </div>
              )}
              <div className="steps-list" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-6)' }}>
                {displaySteps.map((step) => {
                  const isCompleted = ['SUCCESS', 'ROLLED_BACK'].includes(step.status);
                  const isFailed = ['FAILED', 'ROLLBACK_FAILED'].includes(step.status);
                  const isRunning = ['IN_PROGRESS', 'ROLLING_BACK'].includes(step.status);
                  
                  const progress = isCompleted ? 100 : isRunning ? 50 : 0;
                  const stepDisplayName = getStepDisplayName(step);
                  
                  let nameColor = 'var(--text-tertiary)'; // Unstarted default
                  let statusColor = 'var(--text-tertiary)'; // Unstarted default
                  let barColor = 'var(--border-default)'; // Unstarted default track
                  
                  if (isCompleted) {
                    nameColor = 'var(--button-primary-active)';
                    statusColor = '#1F845A';
                    barColor = '#098C60';
                  } else if (isFailed) {
                    nameColor = 'var(--button-primary-active)';
                    statusColor = 'var(--color-danger)';
                    barColor = 'var(--color-danger)';
                  } else if (isRunning) {
                    nameColor = 'var(--button-primary-active)';
                    statusColor = 'var(--button-primary)';
                    barColor = 'var(--button-primary)';
                  }

                  return (
                    <div key={step.id} style={{ display: 'flex', gap: 'var(--space-2)', alignItems: 'flex-start', padding: '0px', width: '100%' }}>
                      {getStepIcon(step.status, 20)}
                      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <span style={{
                            fontFamily: 'Satoshi',
                            fontSize: 'var(--text-xs)',
                            fontWeight: 'var(--font-medium)',
                            lineHeight: '16px',
                            color: nameColor,
                            flex: '1 1 auto',
                            minWidth: 0,
                            whiteSpace: 'normal',
                            overflowWrap: 'anywhere',
                            paddingRight: '12px'
                          }} title={stepDisplayName}>
                            {stepDisplayName}
                          </span>
                          <span style={{
                            fontFamily: 'Satoshi', 
                            fontSize: 'var(--text-xs)', 
                            fontWeight: 'var(--font-medium)', 
                            lineHeight: '16px',
                            color: statusColor,
                            width: '96px',
                            textAlign: 'right',
                            flexShrink: 0
                          }}>
                            {progress}% Completed
                          </span>
                        </div>
                        <div className="step-progress-track" style={{ height: '8px', background: 'var(--border-default)', borderRadius: '2px', overflow: 'hidden', width: '100%', display: 'flex' }}>
                          {isCompleted || isFailed || isRunning ? (
                            <>
                              <div 
                                className="step-progress-fill" 
                                style={{ 
                                  width: `${progress}%`, 
                                  background: barColor, 
                                  height: '100%', 
                                  borderRadius: progress === 100 ? '2px' : '2px 0px 0px 2px', 
                                  transition: 'width 0.5s ease' 
                                }} 
                              />
                              {progress < 100 && (
                                <div style={{ flex: 1, background: 'var(--border-default)', height: '100%', borderRadius: '0px 2px 2px 0px' }} />
                              )}
                            </>
                          ) : (
                            // Not Started / Pending step progress bar: partitioned rectangle 63 and rectangle 64
                            <>
                              <div style={{ width: '3px', background: 'var(--border-default)', height: '100%', borderRadius: '2px 0px 0px 2px' }} />
                              <div style={{ flex: 1, background: 'var(--border-default)', height: '100%', borderRadius: '0px 2px 2px 0px', marginLeft: '0px' }} />
                            </>
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })}
                {displaySteps.length === 0 && <div className="empty-state" style={{ textAlign: 'center', color: 'var(--text-tertiary)' }}>No steps recorded</div>}
              </div>
            </div>
          </div>
        )}

        {(showLogs || isLogsExpanded) && (
          <div className="job-logs-container">
            <div className="logs-header">
              <div className="logs-header-title">
                <span>Live Logs</span>
                {!isFinished && <RefreshCw size={14} className="spin log-spin" />}
              </div>
              <button className="btn icon-only toggle-expand-btn" onClick={() => setIsLogsExpanded(!isLogsExpanded)} title={isLogsExpanded ? "Collapse" : "Expand"}>
                {isLogsExpanded ? (
                  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M 19 10 h -5 v -5" />
                    <path d="M 5 14 h 5 v 5" />
                  </svg>
                ) : (
                  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M 14 5 h 5 v 5" />
                    <path d="M 10 19 H 5 v -5" />
                  </svg>
                )}
              </button>
            </div>
            <div className="logs-content">
              <pre className="logs-text">
                {renderLogs(job.logs)}
              </pre>
              <div ref={logsEndRef} />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
