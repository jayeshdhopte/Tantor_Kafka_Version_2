import { Download, RefreshCw, Loader2, Save, UploadCloud, X, Plus, Trash2, Server, GitCompare, FileCheck } from 'lucide-react';
import { useNavigate, useParams } from 'react-router-dom';
import { useState, useCallback, useEffect, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { usePermissions } from '../hooks/usePermissions';
import './ConfigEditor.css';
import './ConfigVersioning.css';

interface StaticConfigFile {
  id: string;
  serviceId?: string;
  hostId?: string;
  nodeId?: number;
  label: string;
  description?: string;
  path: string;
  role?: string;
  properties: Record<string, unknown>;
}

interface ServiceTopologyItem {
  hostId: string;
  hostAddress: string;
  role: string;
  nodeId: number;
  systemdUnit: string;
  configPath: string;
}

interface ConfigPayload {
  serviceTopology?: ServiceTopologyItem[];
  staticConfigs: { configFiles?: StaticConfigFile[] };
}

interface ConfigDiff {
  key: string;
  type: 'ADDED' | 'REMOVED' | 'MODIFIED';
  oldValue: string;
  newValue: string;
}

interface PreviewResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
  diff: ConfigDiff[];
}

interface ConfigVersion {
  id: string;
  configVersion: number;
  component: string;
  configFileName: string;
  status: string;
  approvalRequired: boolean;
  createdBy: string;
  approvedBy?: string;
  createdAt: string;
  appliedAt?: string;
  rollbackVersion?: number;
  jobId?: string;
}

interface ConfigDialog {
  message: string;
  confirmLabel?: string;
  cancelLabel?: string;
  onConfirm?: () => void;
}

const editableVersionStatuses = new Set(['VALIDATED', 'APPROVED', 'FAILED']);

export function InternalConfigEditor() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { canManage } = usePermissions();
  const [payload, setPayload] = useState<ConfigPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [working, setWorking] = useState('');
  const [selectedHostId, setSelectedHostId] = useState('');
  const [selectedFileId, setSelectedFileId] = useState('');
  const [baselineProperties, setBaselineProperties] = useState<Record<string, string>>({});
  const [draftProperties, setDraftProperties] = useState<Record<string, string>>({});
  const [newKey, setNewKey] = useState('');
  const [newValue, setNewValue] = useState('');
  const [restart, setRestart] = useState(true);
  const approvalRequired = false;
  const [preview, setPreview] = useState<PreviewResult | null>(null);
  const [versions, setVersions] = useState<ConfigVersion[]>([]);
  const [dialog, setDialog] = useState<ConfigDialog | null>(null);

  const showNotice = (message: string) => setDialog({ message, confirmLabel: 'OK' });

  const fetchConfigs = useCallback(async () => {
    setLoading(true);
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config`);
      if (response.ok) setPayload(await response.json());
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => { void (async () => { await fetchConfigs(); })(); }, [fetchConfigs]);

  const files = payload?.staticConfigs.configFiles || [];
  const hosts = useMemo(() => {
    const unique = new Map<string, { id: string; address: string; services: number }>();
    const topo = payload?.serviceTopology || [];
    topo.forEach(service => {
      const current = unique.get(service.hostId);
      unique.set(service.hostId, {
        id: service.hostId,
        address: service.hostAddress,
        services: (current?.services || 0) + 1,
      });
    });
    return Array.from(unique.values());
  }, [payload?.serviceTopology]);

  useEffect(() => {
    Promise.resolve().then(() => {
      if (!selectedHostId && hosts[0]) setSelectedHostId(hosts[0].id);
    });
  }, [hosts, selectedHostId]);

  const hostFiles = files.filter((file: StaticConfigFile) => !selectedHostId || file.hostId === selectedHostId);
  const selectedFile = hostFiles.find(file => file.id === selectedFileId) || hostFiles[0];

  useEffect(() => {
    Promise.resolve().then(() => {
      if (selectedFile && selectedFile.id !== selectedFileId) setSelectedFileId(selectedFile.id);
    });
  }, [selectedFile, selectedFileId]);

  useEffect(() => {
    if (!selectedFile) return;
    const properties = Object.fromEntries(
      Object.entries(selectedFile.properties || {}).map(([key, value]) => [key, String(value ?? '')])
    );
    Promise.resolve().then(() => {
      setBaselineProperties(properties);
      setDraftProperties(properties);
      setPreview(null);
    });
  }, [selectedFile]);

  const fetchVersions = useCallback(async (serviceId?: string) => {
    if (!serviceId) { setVersions([]); return; }
    const response = await fetch(`/api/v1/clusters/${id}/config/versions?serviceId=${serviceId}`);
    if (response.ok) setVersions(await response.json() as ConfigVersion[]);
  }, [id]);

  useEffect(() => { void (async () => { await fetchVersions(selectedFile?.serviceId); })(); }, [fetchVersions, selectedFile?.serviceId]);

  const selectHost = (hostId: string) => {
    setSelectedHostId(hostId);
    setSelectedFileId(files.find(file => file.hostId === hostId)?.id || '');
  };

  const mutateDraft = (updater: (current: Record<string, string>) => Record<string, string>) => {
    if (!canManage) return;
    setDraftProperties(current => updater(current));
    setPreview(null);
  };

  const addProperty = () => {
    if (!canManage) return;
    const key = newKey.trim();
    if (!key || !/^[A-Za-z0-9._-]+$/.test(key)) {
      showNotice('Enter a valid Kafka property key.');
      return;
    }
    mutateDraft(current => ({ ...current, [key]: newValue }));
    setNewKey('');
    setNewValue('');
  };

  const versionRequest = {
    currentProperties: baselineProperties,
    properties: draftProperties,
    configFileName: selectedFile?.path,
    approvalRequired,
    restart,
  };

  const reviewChange = async () => {
    if (!canManage) return;
    if (!selectedFile?.serviceId) return;
    setWorking('preview');
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/services/${selectedFile.serviceId}/versions/preview`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(versionRequest),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) { showNotice(data.message || 'Unable to validate configuration.'); return; }
      const localDiff: ConfigDiff[] = Array.from(new Set([
        ...Object.keys(baselineProperties),
        ...Object.keys(draftProperties),
      ])).flatMap(key => {
        const oldValue = baselineProperties[key];
        const newValue = draftProperties[key];
        if (oldValue === newValue) return [];
        return [{
          key,
          type: oldValue === undefined ? 'ADDED' : newValue === undefined ? 'REMOVED' : 'MODIFIED',
          oldValue: oldValue ?? '',
          newValue: newValue ?? '',
        } as ConfigDiff];
      });
      setPreview({ ...data, diff: Array.isArray(data.diff) && data.diff.length > 0 ? data.diff : localDiff });
    } finally {
      setWorking('');
    }
  };

  const saveVersion = async () => {
    if (!canManage) return;
    if (!selectedFile?.serviceId || !preview?.valid) return;
    setWorking('save');
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/services/${selectedFile.serviceId}/versions`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(versionRequest),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) { showNotice(data.message || 'Unable to save configuration version.'); return; }
      await fetchVersions(selectedFile.serviceId);
      showNotice(`Version v${data.configVersion} saved. The active production file has not been changed.`);
    } finally {
      setWorking('');
    }
  };

  const performVersionAction = async (version: ConfigVersion, action: 'apply' | 'rollback') => {
    if (!canManage) return;
    setWorking(`${action}-${version.id}`);
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/versions/${version.id}/${action}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: action === 'apply' ? JSON.stringify({ restart }) : undefined,
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) { showNotice(data.message || `${action} failed.`); return; }
      await fetchVersions(selectedFile?.serviceId);
      if (action === 'apply' && data.jobId) {
        showNotice('The configuration apply job has started. View job is now available in Version History.');
      }
      if (action === 'rollback') {
        showNotice(`Rollback saved as new version v${data.configVersion}. Approve it if required, then apply it.`);
      }
    } finally {
      setWorking('');
    }
  };

  const versionAction = (version: ConfigVersion, action: 'apply' | 'rollback') => {
    if (action !== 'apply') {
      void performVersionAction(version, action);
      return;
    }
    const message = restart
      ? `Apply configuration v${version.configVersion} and perform a controlled rolling service restart? Kafka on the affected node will be restarted and verified by the job.`
      : `Apply configuration v${version.configVersion} without restarting Kafka? Static properties will not become active until a later restart.`;
    setDialog({
      message,
      cancelLabel: 'Cancel',
      confirmLabel: 'OK',
      onConfirm: () => { setDialog(null); void performVersionAction(version, action); },
    });
  };

  const latestApplyableVersion = useMemo(() => {
    return [...versions]
      .sort((a, b) => b.configVersion - a.configVersion)
      .find(v => editableVersionStatuses.has(v.status));
  }, [versions]);

  if (loading && !payload) {
    return <div className="state-center"><Loader2 className="spin" /> Loading node configurations...</div>;
  }

  const activeVersion = versions.find(version => version.status === 'APPLIED');

  const flowButtonStyle = (disabled: boolean) => ({
    display: 'inline-flex',
    alignItems: 'center',
    gap: '6px',
    padding: '6px 12px',
    border: '1px solid #7C3AED',
    borderRadius: '6px',
    background: "var(--bg-surface)",
    color: '#7C3AED',
    fontWeight: 650,
    fontSize: 'var(--text-xs)',
    cursor: disabled ? 'not-allowed' : 'pointer',
    opacity: disabled ? 0.5 : 1,
    transition: 'all 0.2s',
  });

  return (
    <div className="node-config-page versioned-config-page">
      <header className="node-config-header">
        <div><h2>Versioned Configuration Change</h2><p>Review, validate and save an immutable version before anything reaches a node.</p></div>
        <button onClick={fetchConfigs} disabled={loading} aria-label="Refresh configuration" title="Refresh"><RefreshCw size={14} className={loading ? 'spin' : ''} /></button>
      </header>

      <div className="config-flow-strip" style={{
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
        background: 'none',
        border: 'none',
        padding: '0px',
        marginBottom: '1.25rem'
      }}>
        <button 
          onClick={reviewChange} 
          disabled={!!working || !selectedFile?.serviceId} 
          style={flowButtonStyle(!!working || !selectedFile?.serviceId)}
        >
          <GitCompare size={14} /> Diff
        </button>
        <button 
          onClick={reviewChange} 
          disabled={!!working || !selectedFile?.serviceId} 
          style={flowButtonStyle(!!working || !selectedFile?.serviceId)}
        >
          <FileCheck size={14} /> Validate
        </button>
        <button 
          onClick={saveVersion} 
          disabled={!preview?.valid || !!working} 
          style={flowButtonStyle(!preview?.valid || !!working)}
        >
          <Download size={14} /> Save version
        </button>
        <button 
          onClick={() => latestApplyableVersion && versionAction(latestApplyableVersion, 'apply')} 
          disabled={!latestApplyableVersion || !!working} 
          style={flowButtonStyle(!latestApplyableVersion || !!working)}
        >
          <UploadCloud size={14} /> Backup &amp; apply
        </button>
      </div>

      <section className="node-config-section config-selection-step">
        <div className="node-config-section-title">
          <span style={{ border: '1px solid #7C3AED', background: "var(--bg-surface)", color: '#7C3AED' }}>1</span>
          <div><h3>Select node</h3><p>Each VM may contain one or more Kafka services.</p></div>
        </div>
        <div className="node-config-hosts">
          {hosts.map(host => (
            <button 
              key={host.id} 
              className={selectedHostId === host.id ? 'active' : ''} 
              onClick={() => selectHost(host.id)}
              style={selectedHostId === host.id ? {
                background: '#FAF5FF',
                borderColor: '#D8B4FE',
                color: '#6B21A8'
              } : {}}
            >
              <Server size={16} /><span><strong>{host.address}</strong><small>{host.id} - {host.services} service{host.services === 1 ? '' : 's'}</small></span>
            </button>
          ))}
        </div>
      </section>

      <section className="node-config-section config-selection-step">
        <div className="node-config-section-title">
          <span style={{ border: '1px solid #7C3AED', background: "var(--bg-surface)", color: '#7C3AED' }}>2</span>
          <div><h3>Select Configuration File</h3><p>Only files belonging to the selected node are shown.</p></div>
        </div>
        <div className="node-config-files">
          {hostFiles.map(file => (
            <button 
              key={file.id} 
              className={selectedFile?.id === file.id ? 'active' : ''} 
              onClick={() => setSelectedFileId(file.id)}
              style={selectedFile?.id === file.id ? {
                background: '#FAF5FF',
                borderColor: '#D8B4FE',
                color: '#6B21A8'
              } : {}}
            >
              <Server size={16} /><span><strong>{file.label}</strong><small>{file.path}</small></span>
            </button>
          ))}
        </div>
      </section>

      {selectedFile ? <>
        <section className="node-config-section editor">
          <div className="node-config-editor-head">
            <div><h3>{selectedFile.label}</h3><p>{selectedFile.path}</p></div>
            <div className="config-options">
              {canManage && <label><input type="checkbox" checked={restart} onChange={event => { setRestart(event.target.checked); setPreview(null); }} /> Restart after apply</label>}
            </div>
          </div>

          <div className="node-config-table-wrap">
            <table className="node-config-table">
              <thead><tr><th>Property</th><th>Value</th><th>Action</th></tr></thead>
              <tbody>
                {Object.entries(draftProperties).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => {
                  return <tr key={key}>
                    <td><code>{key}</code></td>
                    <td><input value={value} disabled={!canManage} onChange={event => mutateDraft(current => ({ ...current, [key]: event.target.value }))} style={{ border: '1px solid var(--border-subtle)', borderRadius: '6px' }} /></td>
                    <td>{canManage && <button title={`Remove ${key}`} onClick={() => mutateDraft(current => { const next = { ...current }; delete next[key]; return next; })} style={{ border: '1px solid #fee2e2', color: '#ef4444', background: "var(--bg-surface)" }}><Trash2 size={14} /></button>}</td>
                  </tr>;
                })}
              </tbody>
            </table>
          </div>

          {canManage && (
            <div className="node-config-add" style={{ display: 'grid', gridTemplateColumns: 'minmax(180px, 0.8fr) minmax(220px, 1.2fr) auto', gap: 'var(--space-2)', marginTop: '1rem' }}>
              <input 
                placeholder="property.key" 
                value={newKey} 
                onChange={event => setNewKey(event.target.value)} 
                style={{ border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)', padding: '8px 12px' }}
              />
              <input 
                placeholder="value" 
                value={newValue} 
                onChange={event => setNewValue(event.target.value)} 
                style={{ border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)', padding: '8px 12px' }}
              />
              <button 
                onClick={addProperty}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '6px',
                  padding: '8px 16px',
                  borderRadius: 'var(--radius-md)',
                  border: '1px solid var(--button-primary)',
                  background: "var(--bg-surface)",
                  color: 'var(--button-primary)',
                  fontWeight: 'var(--font-medium)',
                  fontSize: 'var(--text-sm)',
                  cursor: 'pointer'
                }}
              >
                <Plus size={14} /> Add property
              </button>
            </div>
          )}

          <div className="node-config-footer" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '1.25rem' }}>
            <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-muted)' }}>
              Target: {selectedFile.hostId}Node: {selectedFile.nodeId}Service: {selectedFile.role}
            </span>
            {canManage && (
              <button 
                onClick={reviewChange} 
                disabled={!!working}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '6px',
                  height: '38px',
                  padding: '0 20px',
                  borderRadius: 'var(--radius-md)',
                  background: 'var(--button-primary)',
                  color: "var(--text-light)",
                  border: 'none',
                  fontSize: 'var(--text-sm)',
                  fontWeight: 'var(--font-medium)',
                  cursor: working ? 'not-allowed' : 'pointer'
                }}
              >
                {working === 'preview' ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />} 
                Review &amp; validate
              </button>
            )}
          </div>
        </section>

        <section className="node-config-section config-review" style={{ borderColor: 'var(--border-subtle)', marginTop: '1.5rem' }}>
          <div className="node-config-section-title">
            <span style={{ border: '1px solid #7C3AED', background: "var(--bg-surface)", color: '#7C3AED' }}>3</span>
            <div><h3>Old vs New</h3><p>The server validates this exact snapshot again when the version is saved.</p></div>
          </div>
          {preview ? <>
          {preview.errors.length > 0 && <div className="config-messages error">{preview.errors.map(message => <p key={message}>{message}</p>)}</div>}
          {preview.warnings.length > 0 && <div className="config-messages warning">{preview.warnings.map(message => <p key={message}>{message}</p>)}</div>}
          <div className="config-diff-list">
            {preview.diff.map(item => (
              <div className="config-diff-item" key={item.key}>
                <div className="config-diff-meta">
                  <span>{item.type}</span>
                  <strong>{item.key}</strong>
                </div>
                <div className="config-diff-values">
                  <div>{item.oldValue || 'empty'}</div>
                  <div>{item.newValue || 'empty'}</div>
                </div>
              </div>
            ))}
          </div>
          <div className="config-review-footer" style={{ marginTop: '20px', borderTop: '1px solid #f1f5f9', paddingTop: '16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-muted)' }}>{preview.valid ? 'Validation passed. Saving creates history only; it does not apply the file.' : 'Fix validation errors before saving.'}</span>
            {canManage && (
              <button 
                onClick={saveVersion} 
                disabled={!preview.valid || !!working}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '6px',
                  height: '38px',
                  padding: '0 16px',
                  borderRadius: 'var(--radius-md)',
                  background: 'var(--button-primary)',
                  color: "var(--text-light)",
                  border: 'none',
                  fontSize: 'var(--text-sm)',
                  fontWeight: 'var(--font-medium)',
                  cursor: !preview.valid || !!working ? 'not-allowed' : 'pointer',
                  opacity: !preview.valid || !!working ? 0.5 : 1
                }}
              >
                {working === 'save' ? <Loader2 size={14} className="spin" /> : <Save size={14} />}
                Save as new Version
              </button>
            )}
          </div>
          </> : <p className="config-review-placeholder">Review and validate the draft to compare it with the saved configuration.</p>}
        </section>

        <section className="node-config-section config-history">
          <div className="node-config-section-title">
            <span>4</span>
            <div><h3>Version History</h3><p>Backups, approvals, applies and rollback lineage remain auditable.</p></div>
          </div>
          {versions.length > 0 ? (
            <div className="version-list">
              {[...versions].sort((a, b) => b.configVersion - a.configVersion).map(version => (
                <article key={version.id} className={version.id === activeVersion?.id ? 'active-version' : ''}>
                  <div className="version-main">
                    <strong>v{version.configVersion}</strong>
                    <span className={`version-status ${version.status.toLowerCase()}`}>{version.status.replaceAll('_', ' ')}</span>
                    {version.rollbackVersion != null && <span className="rollback-tag">Rollback of v{version.rollbackVersion}</span>}
                    <small>created by {version.createdBy || 'Unknown'} | {new Date(version.createdAt).toLocaleString()}</small>
                  </div>
                  <div className="version-actions">
                    {version.status !== 'APPLIED' && canManage ? (
                      <button
                        type="button"
                        className="primary"
                        disabled={!!working}
                        onClick={() => versionAction(version, 'apply')}
                      >
                        {working === `apply-${version.id}` ? <Loader2 size={14} className="spin" /> : <UploadCloud size={14} />} Apply
                      </button>
                    ) : version.status === 'APPLIED' ? (
                      <button type="button" className="view-job highlighted" onClick={() => navigate('/jobs')}>View job</button>
                    ) : null}
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <p className="version-history-empty">No saved configuration versions yet.</p>
          )}
        </section>
      </> : <div className="empty-state">No managed configuration file is available for this node.</div>}

      {dialog && createPortal(
        <div className="config-dialog-backdrop" role="dialog" aria-modal="true" aria-labelledby="config-dialog-title">
          <div className="config-dialog">
            <div className="config-dialog-banner">
              <button
                type="button"
                className="config-dialog-close"
                aria-label="Close dialog"
                onClick={() => setDialog(null)}
              >
                <X size={22} />
              </button>
            </div>
            <div className="config-dialog-body">
              <h3 id="config-dialog-title">{window.location.host} says</h3>
              <p>{dialog.message}</p>
              <div className="config-dialog-actions">
                {dialog.cancelLabel && (
                  <button type="button" className="cancel" onClick={() => setDialog(null)}>{dialog.cancelLabel}</button>
                )}
                <button
                  type="button"
                  className="confirm"
                  onClick={() => dialog.onConfirm ? dialog.onConfirm() : setDialog(null)}
                >
                  {dialog.confirmLabel || 'OK'}
                </button>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}
    </div>
  );
}
