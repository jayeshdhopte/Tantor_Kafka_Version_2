import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Loader2, Play, RefreshCw, Save, ShieldAlert } from 'lucide-react';
import { InternalConfigEditor } from './InternalConfigEditor';
import { usePermissions } from '../hooks/usePermissions';
import { notifyAction } from '../components/confirmUtils';
import './ConfigEditor.css';
import './ConfigVersioning.css';

interface ClusterInfo {
  id: string;
  mode?: string;
}

interface ServiceTopologyItem {
  hostId: string;
  hostAddress: string;
  role: string;
  nodeId: number;
  systemdUnit: string;
  configPath: string;
  isBroker: boolean;
  isController: boolean;
  serviceName: string;
  agentStatus: string;
  managedStatus: string;
  canExecuteTasks: boolean;
}

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

interface ConfigPayload {
  serviceTopology?: ServiceTopologyItem[];
  staticConfigs: { configFiles?: StaticConfigFile[] };
}

interface StagedProperty {
  key: string;
  oldValue: string;
  newValue: string;
}

interface StagedChange {
  nodeId: number;
  host: string;
  serviceName: string;
  configFilePath: string;
  properties: StagedProperty[];
}

export function ConfigEditor() {
  const { id } = useParams<{ id: string }>();
  const [cluster, setCluster] = useState<ClusterInfo | null>(null);
  const [loadingCluster, setLoadingCluster] = useState(true);

  useEffect(() => {
    let cancelled = false;
    Promise.resolve().then(() => setLoadingCluster(true));
    fetch(`/api/v1/ui/clusters/${id}`)
      .then(response => response.ok ? response.json() : null)
      .then(data => {
        if (!cancelled) setCluster(data);
      })
      .catch(() => {
        if (!cancelled) setCluster(null);
      })
      .finally(() => {
        if (!cancelled) setLoadingCluster(false);
      });
    return () => { cancelled = true; };
  }, [id]);

  if (loadingCluster) {
    return <div className="state-center"><Loader2 className="spin" /> Loading cluster configuration...</div>;
  }

  if (cluster?.mode === 'EXTERNAL') {
    return <ExternalConfigEditor />;
  }

  return <InternalConfigEditor />;
}

function ExternalConfigEditor() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { canManage } = usePermissions();
  
  const [loading, setLoading] = useState(false);
  const [topology, setTopology] = useState<ServiceTopologyItem[]>([]);
  const [configFiles, setConfigFiles] = useState<StaticConfigFile[]>([]);
  
  const [fetchedProperties, setFetchedProperties] = useState<Record<number, Record<string, string>>>({});
  const [readingConfig, setReadingConfig] = useState(false);
  const [readStatus, setReadStatus] = useState<string>('');

  const [selectedNodeId, setSelectedNodeId] = useState<number | null>(null);
  
  // staged changes by nodeId
  const [stagedChanges, setStagedChanges] = useState<Record<number, StagedChange>>({});
  
  // currently editing draft
  const [draftProperties, setDraftProperties] = useState<Record<string, string>>({});
  const [rollingRestart, setRollingRestart] = useState(true);
  const [applying, setApplying] = useState(false);

  const fetchConfigs = async () => {
    setLoading(true);
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config`);
      if (response.ok) {
        const payload: ConfigPayload = await response.json();
        setTopology(payload.serviceTopology || []);
        setConfigFiles(payload.staticConfigs.configFiles || []);
      } else {
        notifyAction('Failed to fetch cluster configuration');
      }
    } catch {
      notifyAction('Error fetching config');
    } finally {
      setLoading(false);
    }
  };

  const selectedNode = useMemo(() => {
    if (selectedNodeId === null) return null;
    return topology.find(t => t.nodeId === selectedNodeId);
  }, [selectedNodeId, topology]);

  const selectedFile = useMemo(() => {
    if (!selectedNode) return null;
    return configFiles.find(f => f.nodeId === selectedNode.nodeId);
  }, [selectedNode, configFiles]);

  const selectNode = (nodeId: number) => {
    // save current draft before switching
    if (selectedNode && selectedFile) {
      saveDraftToStaged();
    }
    setSelectedNodeId(nodeId);
    
    // If not already fetched, and the node has an agent, fetch it!
    const node = topology.find(t => t.nodeId === nodeId);
    if (node && node.canExecuteTasks && !fetchedProperties[nodeId]) {
      readConfigFromAgent(nodeId);
    }
  };

  const readConfigFromAgent = async (nodeId: number) => {
    setReadingConfig(true);
    setReadStatus('Initiating read_config task...');
    try {
      const startRes = await fetch(`/api/v1/clusters/${id}/config/read`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nodeId })
      });
      if (!startRes.ok) {
        setReadStatus('Failed to start task or agent offline.');
        setReadingConfig(false);
        return;
      }
      const startData = await startRes.json();
      if (!startData.taskId) {
        setReadStatus('Task failed to start.');
        setReadingConfig(false);
        return;
      }
      const taskId = startData.taskId;

      let complete = false;
      while (!complete) {
        await new Promise(r => setTimeout(r, 2000));
        const statusRes = await fetch(`/api/v1/ui/external-clusters/tasks/${taskId}`);
        if (!statusRes.ok) break;
        const statusData = await statusRes.json();
        if (statusData.status === 'SUCCESS') {
           setFetchedProperties(prev => ({ ...prev, [nodeId]: statusData.data || {} }));
           setReadStatus('');
           complete = true;
        } else if (statusData.status === 'FAILED') {
           setReadStatus('Task failed: ' + (statusData.message || ''));
           complete = true;
        } else {
           setReadStatus('Waiting for agent to read file...');
        }
      }
    } catch {
       setReadStatus('Error reading config.');
    } finally {
       setReadingConfig(false);
    }
  };

  const saveDraftToStaged = () => {
    if (!selectedNode || !selectedFile) return;
    const baseProperties = Object.fromEntries(
      Object.entries(selectedFile.properties || {}).map(([key, value]) => [key, String(value ?? '')])
    );
    const changed: StagedProperty[] = [];
    
    // Find modified or added
    for (const [k, v] of Object.entries(draftProperties)) {
      if (baseProperties[k] !== v) {
        changed.push({ key: k, oldValue: baseProperties[k] || '', newValue: v });
      }
    }
    
    // Find deleted
    for (const [k, v] of Object.entries(baseProperties)) {
      if (!(k in draftProperties)) {
        changed.push({ key: k, oldValue: v, newValue: '' });
      }
    }

    setStagedChanges(prev => {
      const next = { ...prev };
      if (changed.length > 0) {
        next[selectedNode.nodeId] = {
          nodeId: selectedNode.nodeId,
          host: selectedNode.hostAddress,
          serviceName: selectedNode.serviceName,
          configFilePath: selectedNode.configPath,
          properties: changed,
        };
      } else {
        delete next[selectedNode.nodeId];
      }
      return next;
    });
  };

  useEffect(() => {
    Promise.resolve().then(() => {
      if (!selectedFile) {
        setDraftProperties({});
        return;
      }
    const liveProps = fetchedProperties[selectedFile.nodeId!] || selectedFile.properties || {};
    const baseProps = Object.fromEntries(
      Object.entries(liveProps).map(([key, value]) => [key, String(value ?? '')])
    );
    // Apply staged changes to draft
    const staged = stagedChanges[selectedFile.nodeId!];
    if (staged) {
      staged.properties.forEach(p => {
        if (p.newValue === '') {
          delete baseProps[p.key];
        } else {
          baseProps[p.key] = p.newValue;
        }
      });
    }
    setDraftProperties(baseProps);
    });
  }, [selectedFile, selectedNodeId, fetchedProperties, stagedChanges]); // re-run when node changes

  const mutateDraft = (key: string, value: string | null) => {
    if (!canManage) return;
    setDraftProperties(prev => {
      const next = { ...prev };
      if (value === null) delete next[key];
      else next[key] = value;
      return next;
    });
  };

  const applyChanges = async () => {
    if (!canManage) return;
    // save current screen
    saveDraftToStaged();
    
    const changesArray = Object.values(stagedChanges);
    if (changesArray.length === 0) {
      notifyAction('No changes staged.');
      return;
    }
    
    setApplying(true);
    try {
      const response = await fetch(`/api/v1/clusters/${id}/config/rolling-apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          changes: changesArray,
          rollingRestart: rollingRestart
        })
      });
      const data = await response.json().catch(() => ({}));
      if (response.ok && data.jobId) {
        navigate(`/jobs/${data.jobId}`);
      } else {
        notifyAction(data.message || 'Failed to apply configuration.');
      }
    } catch {
      notifyAction('Error applying changes.');
    } finally {
      setApplying(false);
    }
  };

  const stagedNodesCount = Object.keys(stagedChanges).length;

  return (
    <div className="node-config-page versioned-config-page">
      <header className="node-config-header">
        <div>
          <h2>Staged Rolling Configuration Update</h2>
          <p>Stage changes across multiple nodes and apply them safely with a rolling restart.</p>
        </div>
        <button onClick={fetchConfigs} disabled={loading} className="primary">
          <RefreshCw size={14} className={loading ? 'spin' : ''} /> Fetch Roles
        </button>
      </header>

      {topology.length > 0 && (
        <section className="node-config-section">
          <div className="node-config-section-title">
            <span>1</span>
            <div><h3>Select Node to Edit</h3><p>Select a node to stage configuration changes.</p></div>
          </div>
          
          <div className="node-config-hosts">
            <select 
              value={selectedNodeId || ''} 
              onChange={e => selectNode(Number(e.target.value))}
              className="node-select"
              style={{ padding: 'var(--space-2)', width: '100%', maxWidth: '400px', borderRadius: '4px', border: '1px solid #ccc' }}
            >
              <option value="" disabled>Select a node to edit...</option>
              {topology.map(node => (
                <option key={node.nodeId} value={node.nodeId}>
                  {node.hostAddress} (Node {node.nodeId}) - {node.role} {node.canExecuteTasks ? '' : '[No Task Agent]'} {stagedChanges[node.nodeId] ? '(Staged)' : ''}
                </option>
              ))}
            </select>
          </div>
        </section>
      )}

      {selectedNode && selectedFile && (
        <section className="node-config-section editor-section">
          <div className="node-config-section-title">
            <span>2</span>
            <div>
              <h3>Edit Properties: Node {selectedNode.nodeId}</h3>
              <p>File: <code>{selectedFile.path}</code></p>
            </div>
          </div>
          
          {readingConfig ? (
            <div className="empty-state" style={{ padding: '2rem', textAlign: 'center' }}>
              <Loader2 className="spin" size={32} style={{ margin: '0 auto 1rem', display: 'block', color: '#6366f1' }} />
              <p>{readStatus}</p>
            </div>
          ) : !selectedNode.canExecuteTasks ? (
            <div className="empty-state" style={{ padding: '2rem', textAlign: 'center', color: '#ef4444' }}>
              <ShieldAlert size={32} style={{ margin: '0 auto 1rem', display: 'block' }} />
              <p>No active task-capable agent available.</p>
              <small>Configuration editing is disabled because the Discovery Agent is offline or lacks capabilities.</small>
            </div>
          ) : readStatus ? (
            <div className="empty-state" style={{ padding: '2rem', textAlign: 'center', color: '#ef4444' }}>
              <p>{readStatus}</p>
              <button onClick={() => readConfigFromAgent(selectedNode.nodeId!)} className="secondary" style={{ marginTop: '1rem' }}>Retry</button>
            </div>
          ) : (
            <div className="node-config-table-wrap">
              <table className="node-config-table">
                <thead><tr><th>Property</th><th>Current Value</th><th>New Value</th></tr></thead>
                <tbody>
                  {Object.entries(draftProperties).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => {
                    const baseValue = fetchedProperties[selectedNode.nodeId!]?.[key] || selectedFile.properties?.[key] || '';
                    const isModified = value !== baseValue;
                    return (
                      <tr key={key} className={isModified ? 'modified-row' : ''}>
                        <td><code>{key}</code></td>
                        <td><span className="current-val" title={String(baseValue)}>{String(baseValue) || <em>(Not set)</em>}</span></td>
                        <td>
                          <div className="new-val-input-group" style={{ display: 'flex', gap: 'var(--space-2)' }}>
                            <input
                              type="text"
                              value={String(value)}
                              disabled={!canManage}
                              onChange={e => mutateDraft(key, e.target.value)}
                              placeholder={String(baseValue) || ''}
                            />
                            {canManage && isModified && (
                              <button className="icon-btn revert" onClick={() => mutateDraft(key, String(baseValue))} title="Revert to current">
                                <RefreshCw size={14} />
                              </button>
                            )}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
          
          {canManage && (
            <div className="node-config-footer">
               <button onClick={saveDraftToStaged} disabled={readingConfig || !selectedNode.canExecuteTasks}>
                 <Save size={14}/> Stage Changes
               </button>
            </div>
          )}
        </section>
      )}

      {stagedNodesCount > 0 && (
        <section className="node-config-section config-review">
          <div className="node-config-section-title">
            <span>2</span>
            <div><h3>Pending Changes Summary</h3><p>Review staged changes before applying them.</p></div>
          </div>
          
          <div className="config-diff-list">
            {Object.values(stagedChanges).map(change => (
              <div key={change.nodeId} className="staged-node-block">
                <h4>Node {change.nodeId} ({change.host}) - {change.configFilePath}</h4>
                {change.properties.map(p => (
                  <div className="config-diff-row modified" key={p.key}>
                    <div><span>MODIFIED</span><code>{p.key}</code></div>
                    <pre>{p.oldValue || '-'}</pre><b>-&gt;</b><pre>{p.newValue || '-'}</pre>
                  </div>
                ))}
              </div>
            ))}
          </div>

          <div className="config-review-footer">
            {canManage && <label>
              <input type="checkbox" checked={rollingRestart} onChange={e => setRollingRestart(e.target.checked)} /> 
              Perform rolling restart to apply immediately
            </label>}
            {canManage && <button className="primary" onClick={applyChanges} disabled={applying}>
              {applying ? <Loader2 size={14} className="spin" /> : <Play size={14} />} Apply Changes
            </button>}
          </div>
        </section>
      )}
    </div>
  );
}
