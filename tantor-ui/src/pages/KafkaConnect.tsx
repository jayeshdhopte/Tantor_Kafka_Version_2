import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useParams } from 'react-router-dom';
import { CheckCircle, MoreVertical, Pause, Play, Plus, RefreshCw, RotateCw, Settings, Trash2, Upload, X, FileDown, ChevronDown } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { confirmAction } from '../components/confirmUtils';
import { AnchoredMenu } from '../components/AnchoredMenu';
import { readDataServiceSession, writeDataServiceSession } from '../utils/dataServiceSessionCache';
import './DataServiceTabs.css';

interface ConnectorRow {
  name: string;
  class: string;
  state: string;
  tasks: number;
  runningTasks: number;
  config: Record<string, string>;
}

interface ConnectorPlugin {
  class: string;
  type: string;
  version: string;
}

interface ConnectSummary {
  connection: string;
  version: string;
  connectorCount: number;
  taskCount: number;
  runningTasks: number;
  runningConnectors: number;
  pausedConnectors: number;
  failedConnectors: number;
  connectors: ConnectorRow[];
  plugins: ConnectorPlugin[];
}

interface SavedConnection {
  id: string;
  connectionName: string;
  protocol: string;
  host: string;
  port: number;
  status: string;
  isDefault: boolean;
  certificateConfigured: boolean;
  truststoreConfigured: boolean;
  certificateType?: string;
}

interface DiscoveredConnection {
  detected: boolean;
  certificateRequired: boolean;
  httpsRequired: boolean;
  protocol: string | null;
  host: string | null;
  port: number | null;
  endpoint: string | null;
  message: string | null;
}

const connectorTemplate = `{
  "name": "file-source",
  "config": {
    "connector.class": "FileStreamSource",
    "tasks.max": "1",
    "file": "/tmp/input.txt",
    "topic": "file-source-topic"
  }
}`;

const errorMessage = (error: unknown, fallback: string): string =>
  error instanceof Error && error.message ? error.message : fallback;

interface CustomSelectProps {
  value: string;
  onChange: (val: string) => void;
  options: { value: string; label: string }[];
  placeholder?: string;
  disabled?: boolean;
  className?: string;
}

function CustomSelect({ value, onChange, options, placeholder, disabled, className }: CustomSelectProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [anchor, setAnchor] = useState<HTMLDivElement | null>(null);
  const selectedOption = options.find(o => o.value === value);

  return (
    <div ref={setAnchor} className={`ds-custom-select-container ${className || ''} ${disabled ? 'disabled' : ''}`}>
      <div 
        className="ds-custom-select-trigger" 
        onClick={() => !disabled && setIsOpen(!isOpen)}
      >
        <span>{selectedOption ? selectedOption.label : placeholder || 'Select...'}</span>
        <svg className={`ds-custom-select-arrow ${isOpen ? 'open' : ''}`} xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#A1A1AA" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m6 9 6 6 6-6"/></svg>
      </div>
      
      {isOpen && anchor && (
        <AnchoredMenu
          anchor={anchor}
          className="ds-custom-select-dropdown"
          onClose={() => setIsOpen(false)}
          align="start"
          matchAnchorWidth
        >
            {options.map(opt => (
              <div
                key={opt.value}
                className={`ds-custom-select-option ${opt.value === value ? 'selected' : ''}`}
                onClick={() => {
                  onChange(opt.value);
                  setIsOpen(false);
                }}
              >
                {opt.label}
              </div>
            ))}
        </AnchoredMenu>
      )}
    </div>
  );
}

export function KafkaConnect() {
  const { id } = useParams<{ id: string }>();
  const { canManage } = usePermissions();
  const [initialSession] = useState(() => readDataServiceSession<ConnectSummary>('kafka-connect', id));
  const [summary, setSummary] = useState<ConnectSummary | null>(initialSession?.summary ?? null);
  const [loading, setLoading] = useState(false);
  const [hasFetched, setHasFetched] = useState(initialSession?.hasFetched ?? false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'clusters' | 'connectors' | 'plugins'>('clusters');
  const [showCreate, setShowCreate] = useState(false);
  const [showConnection, setShowConnection] = useState(false);
  const [connectorJson, setConnectorJson] = useState(connectorTemplate);
  const [createError, setCreateError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  // Ã¢â€â‚¬Ã¢â€â‚¬ Multi-instance state Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const [savedConnections, setSavedConnections] = useState<SavedConnection[]>([]);
  const [selectedConnectionId, setSelectedConnectionId] = useState<string | null>(initialSession?.selectedConnectionId ?? null);
  const loadRequestId = useRef(0);

  // Ã¢â€â‚¬Ã¢â€â‚¬ Connection form state Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const [formConnectionName, setFormConnectionName] = useState('');
  const [customIp, setCustomIp] = useState('');
  const [customPort, setCustomPort] = useState('');
  const [protocol, setProtocol] = useState('http');
  const [certType, setCertType] = useState('PEM');
  const [certFile, setCertFile] = useState<File | null>(null);
  const [certFileName, setCertFileName] = useState('');
  const [certPassword, setCertPassword] = useState('');
  const [formIsDefault, setFormIsDefault] = useState(false);
  /** ID of the connection being edited Ã¢â‚¬â€ set when editing an existing connection. */
  const [editingConnectionId, setEditingConnectionId] = useState<string | null>(null);

  const [connectSaving, setConnectSaving] = useState(false);
  const [connectError, setConnectError] = useState<string | null>(null);

  // Ã¢â€â‚¬Ã¢â€â‚¬ Derived: currently selected connection Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const selectedConn = useMemo(
    () => savedConnections.find(c => c.id === selectedConnectionId) ?? null,
    [savedConnections, selectedConnectionId]
  );

  /**
   * Safely appends ?connectionId=... to any URL using URLSearchParams.
   * Works even when the base URL already contains query params.
   */
  const withConnId = (url: string, connId: string | null = selectedConnectionId): string => {
    if (!connId) return url;
    const [base, existing] = url.split('?');
    const params = new URLSearchParams(existing || '');
    params.set('connectionId', connId);
    return base + '?' + params.toString();
  };

  const readFileAsBase64 = (file: File): Promise<string> => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve((reader.result as string).split(',')[1]);
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });

  const buildCertData = async (): Promise<string | undefined> => {
    if (certType === 'PEM' && certFile) {
      const text = await new Promise<string>((res, rej) => {
        const r = new FileReader(); r.onload = () => res(String(r.result || '')); r.onerror = rej; r.readAsText(certFile);
      });
      return btoa(text.trim());
    } else if (certType === 'PKCS12' && certFile) {
      return await readFileAsBase64(certFile);
    }
    return undefined;
  };

  // Ã¢â€â‚¬Ã¢â€â‚¬ Load all connections (for instance switcher) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const loadConnections = useCallback(async () => {
    try {
      const res = await fetch(`/api/v1/clusters/${id}/data-services/kafka-connect/connections`);
      if (!res.ok) return;
      const data: SavedConnection[] = await res.json().catch(() => []);
      setSavedConnections(data);
      if (data.length > 0) {
        const defaultConn = data.find(c => c.isDefault) ?? data[0];
        setSelectedConnectionId(prev => prev ?? defaultConn.id);
      }
    } catch { /* non-fatal */ }
  }, [id]);

  /**
   * Open connection modal.
   * If `conn` is provided, pre-fill for editing (uses PUT /connections/{id}).
   * If omitted, blank form for a new connection.
   */
  const openConnectionModal = (conn?: SavedConnection) => {
    if (!canManage) return;
    if (conn) {
      setEditingConnectionId(conn.id);
      setFormConnectionName(conn.connectionName);
      setProtocol(conn.protocol || 'http');
      setCustomIp(conn.host || '');
      setCustomPort(conn.port ? String(conn.port) : '');
      setCertType(conn.certificateType || 'PEM');
      setFormIsDefault(conn.isDefault);
    } else {
      setEditingConnectionId(null);
      setFormConnectionName('');
      setProtocol('http');
      setCustomIp('');
      setCustomPort('');
      setCertType('PEM');
      setFormIsDefault(false);
    }
    setCertFile(null);
    setCertFileName('');
    setCertPassword('');
    setConnectError(null);
    setShowConnection(true);
  };

  const handleSaveConnection = async () => {
    if (!canManage) return;
    setConnectSaving(true);
    setConnectError(null);
    try {
      const certData = await buildCertData();
      const body = {
        connectionName: formConnectionName.trim() || 'Default connection',
        protocol,
        host: customIp.trim(),
        port: parseInt(customPort.trim()) || 8083,
        certificateType: certType,
        certificateData: certData,
        truststorePassword: certPassword || undefined,
        isDefault: formIsDefault
      };

      // Use PUT /connections/{id} when editing existing, else PUT /connection (upsert-by-name)
      const url = editingConnectionId
        ? `/api/v1/clusters/${id}/data-services/kafka-connect/connections/${editingConnectionId}`
        : `/api/v1/clusters/${id}/data-services/kafka-connect/connection`;

      const res = await fetch(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || 'Failed to save connection.');

      setCertPassword('');
      setShowConnection(false);
      await loadConnections();
      if (data.id) setSelectedConnectionId(data.id);
      await load(data.id || null);
    } catch (e: unknown) {
      setConnectError(errorMessage(e, 'Failed to save connection.'));
    } finally {
      setConnectSaving(false);
    }
  };

  const handleDeleteConnection = async () => {
    if (!canManage) return;
    if (!selectedConnectionId) return;
    if (!(await confirmAction("Are you sure you want to delete this connection?"))) return;
    
    setLoading(true);
    try {
      const res = await fetch(`/api/v1/clusters/${id}/data-services/kafka-connect/connections/${selectedConnectionId}`, {
        method: 'DELETE'
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.message || 'Failed to delete connection.');
      }
      setSelectedConnectionId(null);
      await loadConnections();
      await load();
    } catch (e: unknown) {
      setError(errorMessage(e, 'Failed to delete connection.'));
      setLoading(false);
    }
  };

  const load = async (
    connectionId: string | null = selectedConnectionId,
    discovered?: DiscoveredConnection
  ): Promise<boolean> => {
    const requestId = ++loadRequestId.current;
    setHasFetched(true);
    setLoading(true);
    setError(null);
    try {
      let url = withConnId(`/api/v1/clusters/${id}/data-services/kafka-connect/summary`, connectionId);
      if (!connectionId && discovered?.protocol && discovered.host && discovered.port) {
        const params = new URLSearchParams({
          protocol: discovered.protocol,
          ip: discovered.host,
          port: String(discovered.port)
        });
        url += `${url.includes('?') ? '&' : '?'}${params.toString()}`;
      }
      const res = await fetch(url);
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || 'Failed to load Kafka Connect.');
      if (requestId !== loadRequestId.current) return false;
      setSummary(data);

      // Persist the successful response directly as well as through the layout
      // effect below. This makes the fetched snapshot durable even if the user
      // changes routes as soon as the response is painted.
      writeDataServiceSession('kafka-connect', id, {
        selectedConnectionId: connectionId,
        summary: data,
        hasFetched: true
      });
      return true;
    } catch (e: unknown) {
      if (requestId === loadRequestId.current) setError(errorMessage(e, 'Failed to load Kafka Connect.'));
      return false;
    } finally {
      if (requestId === loadRequestId.current) setLoading(false);
    }
  };

  const handleInstanceChange = (value: string) => {
    const connectionId = value || null;
    if (!connectionId || connectionId === selectedConnectionId) return;
    loadRequestId.current += 1;
    setSelectedConnectionId(connectionId);
    setSummary(null);
    setError(null);
    setHasFetched(true);
    void load(connectionId);
  };

  const prefillDiscoveredConnection = (discovered: DiscoveredConnection, existing?: SavedConnection | null) => {
    setEditingConnectionId(existing?.id || null);
    setFormConnectionName(existing?.connectionName || 'Default connection');
    setProtocol(discovered.protocol || (discovered.httpsRequired ? 'https' : 'http'));
    setCustomIp(discovered.host || '');
    setCustomPort(discovered.port ? String(discovered.port) : '8083');
    setFormIsDefault(true);
    setCertType('PEM');
    setCertFile(null);
    setCertFileName('');
    setCertPassword('');
    setConnectError(discovered.message || null);
    setShowConnection(true);
  };

  const fetchWithDiscovery = async () => {
    const existingId = selectedConnectionId
      || savedConnections.find(connection => connection.isDefault)?.id
      || savedConnections[0]?.id
      || null;
    if (existingId) {
      const loaded = await load(existingId);
      if (loaded) return;
    }

    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`/api/v1/clusters/${id}/data-services/kafka-connect/discover`);
      const discovered: DiscoveredConnection = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(discovered.message || 'Failed to detect Kafka Connect.');

      if (!discovered.detected) {
        setHasFetched(false);
        if (canManage) prefillDiscoveredConnection(
          discovered,
          savedConnections.find(connection => connection.id === existingId)
        );
        else setError(discovered.message || 'No Kafka Connect endpoint could be detected.');
        return;
      }

      if (canManage) {
        const existing = savedConnections.find(connection => connection.id === existingId);
        const saveUrl = existingId
          ? `/api/v1/clusters/${id}/data-services/kafka-connect/connections/${existingId}`
          : `/api/v1/clusters/${id}/data-services/kafka-connect/connection`;
        const saveResponse = await fetch(saveUrl, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            connectionName: existing?.connectionName || 'Default connection',
            protocol: discovered.protocol,
            host: discovered.host,
            port: discovered.port,
            isDefault: true
          })
        });
        const saved = await saveResponse.json().catch(() => ({}));
        if (!saveResponse.ok) throw new Error(saved.message || 'Detected Kafka Connect, but could not save the connection.');
        setSelectedConnectionId(saved.id);
        await loadConnections();
        await load(saved.id);
      } else {
        await load(null, discovered);
      }
    } catch (e: unknown) {
      setHasFetched(false);
      setError(errorMessage(e, 'Failed to detect Kafka Connect.'));
    } finally {
      setLoading(false);
    }
  };

  const loadConnectionsRef = useRef(loadConnections);
  useLayoutEffect(() => {
    loadConnectionsRef.current = loadConnections;
  }, [loadConnections]);

  // Initial load
  useEffect(() => {
    if (!id) return;
    const timer = window.setTimeout(() => { void loadConnectionsRef.current(); }, 0);
    return () => window.clearTimeout(timer);
  }, [id]);

  // Commit the latest fetched snapshot before the browser can navigate away and
  // unmount this route. A normal effect can run too late when another tab is
  // selected immediately after a fetch completes.
  useLayoutEffect(() => {
    writeDataServiceSession('kafka-connect', id, {
      selectedConnectionId,
      summary,
      hasFetched
    });
  }, [hasFetched, id, selectedConnectionId, summary]);

  const clusters = useMemo(() => [{
    name: selectedConn?.connectionName || 'default-connect',
    version: summary?.version || '-',
    connectors: summary?.connectorCount ?? 0,
    runningTasks: summary?.runningTasks ?? 0
  }], [summary, selectedConn]);

  const connectorPayloads = (): Record<string, unknown>[] => {
    const parsed = JSON.parse(connectorJson);
    const payloads = Array.isArray(parsed) ? parsed : (parsed && Array.isArray(parsed.connectors) ? parsed.connectors : [parsed]);
    if (!payloads.length) throw new Error('No connector definitions found.');
    return payloads;
  };

  const handleConnectorFiles = async (files: FileList | null) => {
    if (!canManage) return;
    if (!files?.length) return;
    setCreateError(null);
    try {
      const payloads: Record<string, unknown>[] = [];
      for (const file of Array.from(files)) {
        const parsed = JSON.parse(await file.text());
        payloads.push(...(Array.isArray(parsed) ? parsed : (parsed && Array.isArray(parsed.connectors) ? parsed.connectors : [parsed])));
      }
      if (!payloads.length) throw new Error('No connector definitions found.');
      setConnectorJson(JSON.stringify(payloads.length === 1 ? payloads[0] : payloads, null, 2));
    } catch (e: unknown) { setCreateError(errorMessage(e, 'Unable to read connector JSON.')); }
  };

  const createConnector = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!canManage) return;
    setSaving(true); setError(null); setCreateError(null);
    try {
      const payloads = connectorPayloads();
      let deployed = 0;
      for (const body of payloads) {
        const res = await fetch(withConnId('/api/v1/clusters/' + id + '/data-services/kafka-connect/connectors'), {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.message || ('Failed to deploy connector. ' + deployed + ' of ' + payloads.length + ' deployed.'));
        deployed++;
      }
      setShowCreate(false); setConnectorJson(connectorTemplate); await load();
      setSuccessMessage(deployed === 1 ? 'Connector deployed successfully.' : (deployed + ' connectors deployed successfully.'));
    } catch (e: unknown) { setCreateError(errorMessage(e, 'Failed to deploy connector.')); }
    finally { setSaving(false); }
  };
  const connectorAction = async (name: string, action: 'pause' | 'resume' | 'restart' | 'delete') => {
    if (!canManage) return;
    if (action === 'delete' && !(await confirmAction(`Delete connector ${name}?`))) return;
    setSaving(true);
    setError(null);
    try {
      const baseUrl = action === 'delete'
        ? `/api/v1/clusters/${id}/data-services/kafka-connect/connectors/${encodeURIComponent(name)}`
        : `/api/v1/clusters/${id}/data-services/kafka-connect/connectors/${encodeURIComponent(name)}/${action}`;
      const res = await fetch(withConnId(baseUrl), {
        method: action === 'delete' ? 'DELETE' : 'PUT'
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || `Failed to ${action} connector.`);
      await load();
    } catch (e: unknown) {
      setError(errorMessage(e, `Failed to ${action} connector.`));
    } finally {
      setSaving(false);
    }
  };

  const statusClass = (state: string) => {
    if (state === 'RUNNING') return 'ds-status';
    if (state === 'PAUSED') return 'ds-status warn';
    return 'ds-status error';
  };

  const connStatusColor = (s: string) =>
    s === 'ONLINE' ? '#80e8a2' : (s === 'OFFLINE' || s === 'ERROR') ? '#e88080' : '#a8c5c0';

  return (
    <div className="data-services-page animate-fade-in" style={{ width: '100%' }}>
      <div className="ds-header ds-sr-header" style={{ width: '100%' }}>
        <div className="ds-actions" style={{ width: '100%', display: 'flex', justifyContent: hasFetched ? 'space-between' : 'flex-end', alignItems: 'flex-end', marginBottom: hasFetched ? '0' : '24px' }}>
          
          {/* Ã¢â€â‚¬Ã¢â€â‚¬ Instance Selector Ã¢â€â‚¬Ã¢â€â‚¬ */}
          {hasFetched && <div className="ds-compat-control" style={{ display: 'flex', flexDirection: 'column', gap: '6px', alignItems: 'flex-start' }}>
            <span style={{ fontSize: 'var(--text-sm)', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Instance</span>
            <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
              <CustomSelect
                className="ds-instance-select"
                value={selectedConnectionId ?? ''}
                onChange={handleInstanceChange}
                disabled={savedConnections.length === 0}
                options={
                  savedConnections.length > 0
                    ? savedConnections.map(c => ({
                        value: c.id,
                        label: `${c.connectionName}${c.isDefault ? ' (default)' : ''}`
                      }))
                    : [{ value: '', label: 'Default connection' }]
                }
              />
              {selectedConn && (
                <span
                  style={{
                    display: 'inline-block',
                    width: 8,
                    height: 8,
                    borderRadius: '50%',
                    background: connStatusColor(selectedConn.status),
                    marginLeft: 4
                  }}
                  title={selectedConn.status}
                />
              )}
            </div>
          </div>}

          <div className="ds-buttons-group" style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
            {/* Ã¢â€â‚¬Ã¢â€â‚¬ Buttons Ã¢â€â‚¬Ã¢â€â‚¬ */}
            {canManage && (
              <button 
                className="ds-button ds-kafka-connect-action-button"
                onClick={() => openConnectionModal()} 
              >
                <Settings size={16} style={{ color: 'var(--button-primary)' }} /> Add Connection
              </button>
            )}

            {canManage && (
              <button 
                className="ds-button primary" 
                onClick={() => setShowCreate(true)}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: 'var(--space-2)',
                  height: '35px',
                  padding: '8px 16px',
                  background: 'var(--button-primary)',
                  border: 'none',
                  borderRadius: 'var(--radius-md)',
                  color: "var(--text-light)",
                  fontFamily: 'Satoshi, sans-serif',
                  fontWeight: 'var(--font-medium)',
                  fontSize: 'var(--text-base)',
                  cursor: 'pointer',
                  transition: 'all 0.2s'
                }}
              >
                <Plus size={16} color="#FFFFFF" /> Create Connector
              </button>
            )}

            {canManage && (
              <button
                className="ds-icon-button icon-gray"
                onClick={handleDeleteConnection}
                disabled={!selectedConn}
                style={{
                  width: '35px',
                  height: '35px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  background: "var(--bg-surface)",
                  border: '1px solid var(--border-mid)',
                  borderRadius: 'var(--radius-md)',
                  cursor: 'pointer',
                  opacity: selectedConn ? 1 : 0.5
                }}
                title="Delete connection"
              >
                <Trash2 size={16} style={{ color: 'var(--text-neutral)' }} />
              </button>
            )}

            <button 
              className="ds-icon-button icon-gray" 
              onClick={() => void fetchWithDiscovery()}
              disabled={loading}
              style={{
                width: '35px',
                height: '35px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                background: "var(--bg-surface)",
                border: '1px solid var(--border-mid)',
                borderRadius: 'var(--radius-md)',
                cursor: 'pointer'
              }}
              title="Refresh"
            >
              <RefreshCw size={16} className={loading ? 'spin' : ''} style={{ color: 'var(--text-neutral)' }} />
            </button>

            {canManage && (
              <button
                className="ds-icon-button icon-gray"
                onClick={() => openConnectionModal(selectedConn ?? undefined)}
                disabled={!selectedConn}
                style={{
                  width: '35px',
                  height: '35px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  background: "var(--bg-surface)",
                  border: '1px solid var(--border-mid)',
                  borderRadius: 'var(--radius-md)',
                  cursor: 'pointer',
                  opacity: selectedConn ? 1 : 0.5
                }}
                title="Edit connection"
              >
                <MoreVertical size={16} style={{ color: 'var(--text-neutral)' }} />
              </button>
            )}
          </div>
        </div>
      </div>

      {error && <div className="ds-alert">{error}</div>}

      {!hasFetched ? (
        <div className="ds-fetch-prompt ds-kafka-connect-fetch-prompt" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '40px 0', gap: 'var(--space-4)' }}>
          <p style={{ margin: 0, fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-md)', color: 'var(--text-tertiary)' }}>Kafka Connect data is not loaded automatically.</p>
          <button 
            type="button" 
            onClick={() => void fetchWithDiscovery()}
            disabled={loading}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: 'var(--space-2)',
              height: '36px',
              padding: '0 16px',
              borderRadius: 'var(--radius-md)',
              background: 'var(--button-primary)',
              color: "var(--text-light)",
              fontWeight: 'var(--font-medium)',
              fontSize: 'var(--text-base)',
              border: 'none',
              cursor: 'pointer',
              fontFamily: 'Satoshi, sans-serif'
            }}
          >
            <RefreshCw size={14} className={loading ? 'spin' : ''} style={{ display: 'inline-block' }} />
            {loading ? 'Fetching Kafka Connect...' : 'Fetch Kafka Connect for this cluster'}
          </button>
        </div>
      ) : <>
      <div className="ds-metrics ds-kc-metrics" style={{
        display: 'flex',
        flexDirection: 'row',
        alignItems: 'center',
        padding: '0 0 24px 0',
        gap: 'var(--space-4)',
        background: 'transparent',
        borderRadius: '0',
        marginBottom: '24px',
        boxSizing: 'border-box',
        width: '100%'
      }}>
        {/* Total Connectors */}
        <div className="ds-kc-metric-card" style={{
          boxSizing: 'border-box',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'flex-start',
          padding: 'var(--space-4)',
          gap: 'var(--space-2)',
          background: "var(--bg-surface)",
          border: '1px solid #E4E4E7',
          borderRadius: 'var(--radius-md)',
          flex: '1 1 0px'
        }}>
          <span style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', color: 'var(--text-neutral)' }}>Total Connectors</span>
          <strong style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-bold)', fontSize: '22px', color: 'var(--button-primary-active)' }}>{summary?.connectorCount ?? 0}</strong>
        </div>

        {/* Running Connectors */}
        <div className="ds-kc-metric-card" style={{
          boxSizing: 'border-box',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'flex-start',
          padding: 'var(--space-4)',
          gap: 'var(--space-2)',
          background: "var(--bg-surface)",
          border: '1px solid #E4E4E7',
          borderRadius: 'var(--radius-md)',
          flex: '1 1 0px'
        }}>
          <span style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', color: 'var(--text-neutral)' }}>Running Connectors</span>
          <strong style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-bold)', fontSize: '22px', color: 'var(--button-primary-active)' }}>{summary?.runningConnectors ?? 0}</strong>
        </div>

        {/* Paused Connectors */}
        <div className="ds-kc-metric-card" style={{
          boxSizing: 'border-box',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'flex-start',
          padding: 'var(--space-4)',
          gap: 'var(--space-2)',
          background: "var(--bg-surface)",
          border: '1px solid #E4E4E7',
          borderRadius: 'var(--radius-md)',
          flex: '1 1 0px'
        }}>
          <span style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', color: 'var(--text-neutral)' }}>Paused Connectors</span>
          <strong style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-bold)', fontSize: '22px', color: 'var(--button-primary-active)' }}>{summary?.pausedConnectors ?? 0}</strong>
        </div>

        {/* Failed Connectors */}
        <div className="ds-kc-metric-card" style={{
          boxSizing: 'border-box',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'flex-start',
          padding: 'var(--space-4)',
          gap: 'var(--space-2)',
          background: "var(--bg-surface)",
          border: '1px solid #E4E4E7',
          borderRadius: 'var(--radius-md)',
          flex: '1 1 0px'
        }}>
          <span style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', color: 'var(--text-neutral)' }}>Failed Connectors</span>
          <strong style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-bold)', fontSize: '22px', color: 'var(--button-primary-active)' }}>{summary?.failedConnectors ?? 0}</strong>
        </div>
      </div>

      <div className="ds-tabs ds-kc-tabs" style={{ display: 'flex', gap: 'var(--space-6)', borderBottom: '1px solid var(--border-default)', marginBottom: '20px' }}>
        <button 
          className={activeTab === 'clusters' ? 'active' : ''}
          onClick={() => setActiveTab('clusters')}
          style={{
            background: 'none',
            border: 'none',
            borderBottom: activeTab === 'clusters' ? '2px solid var(--button-primary)' : '2px solid transparent',
            color: activeTab === 'clusters' ? 'var(--button-primary)' : 'var(--text-tertiary)',
            fontFamily: 'Satoshi, sans-serif',
            fontWeight: activeTab === 'clusters' ? 500 : 400,
            fontSize: 'var(--text-base)',
            padding: '8px 12px 12px 12px',
            cursor: 'pointer',
            marginBottom: '-1px'
          }}
        >
          Clusters
        </button>
        <button 
          className={activeTab === 'connectors' ? 'active' : ''}
          onClick={() => setActiveTab('connectors')}
          style={{
            background: 'none',
            border: 'none',
            borderBottom: activeTab === 'connectors' ? '2px solid var(--button-primary)' : '2px solid transparent',
            color: activeTab === 'connectors' ? 'var(--button-primary)' : 'var(--text-tertiary)',
            fontFamily: 'Satoshi, sans-serif',
            fontWeight: activeTab === 'connectors' ? 500 : 400,
            fontSize: 'var(--text-base)',
            padding: '8px 12px 12px 12px',
            cursor: 'pointer',
            marginBottom: '-1px'
          }}
        >
          Connectors
        </button>
        <button 
          className={activeTab === 'plugins' ? 'active' : ''}
          onClick={() => setActiveTab('plugins')}
          style={{
            background: 'none',
            border: 'none',
            borderBottom: activeTab === 'plugins' ? '2px solid var(--button-primary)' : '2px solid transparent',
            color: activeTab === 'plugins' ? 'var(--button-primary)' : 'var(--text-tertiary)',
            fontFamily: 'Satoshi, sans-serif',
            fontWeight: activeTab === 'plugins' ? 500 : 400,
            fontSize: 'var(--text-base)',
            padding: '8px 12px 12px 12px',
            cursor: 'pointer',
            marginBottom: '-1px'
          }}
        >
          Plugins
        </button>
      </div>

      <div className="ds-panel ds-kc-panel">
        {activeTab === 'clusters' && (
          <table className="ds-table ds-kc-table">
            <thead>
              <tr><th>Name</th><th>Version</th><th>Connectors</th><th>Running Tasks</th><th>REST Endpoint</th></tr>
            </thead>
            <tbody>
              {clusters.map(cluster => (
                <tr key={cluster.name}>
                  <td>{cluster.name}</td>
                  <td>{cluster.version}</td>
                  <td>{cluster.connectors}</td>
                  <td>{cluster.runningTasks}</td>
                  <td>
                    {summary?.connection ? (
                      <a 
                        href={summary.connection} 
                        target="_blank" 
                        rel="noopener noreferrer"
                        style={{ color: 'var(--button-primary)', textDecoration: 'underline' }}
                      >
                        {summary.connection}
                      </a>
                    ) : (
                      '-'
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        {activeTab === 'connectors' && (
          <table className="ds-table ds-kc-table ds-kc-connectors-table">
            <thead>
              <tr><th>Name</th><th>Class</th><th>Status</th><th>Tasks</th>{canManage && <th>Actions</th>}</tr>
            </thead>
            <tbody>
              {loading && !summary ? (
                <tr><td colSpan={canManage ? 5 : 4} className="ds-empty">Loading connectors...</td></tr>
              ) : summary && summary.connectors.length > 0 ? (
                summary.connectors.map(connector => (
                  <tr key={connector.name}>
                    <td>{connector.name}</td>
                    <td>{connector.class || '-'}</td>
                    <td><span className={statusClass(connector.state)}>{connector.state.charAt(0) + connector.state.slice(1).toLowerCase()}</span></td>
                    <td>{connector.runningTasks} / {connector.tasks}</td>
                    {canManage && (
                      <td>
                        <div className="ds-inline-actions">
                          <button className="ds-button" onClick={() => connectorAction(connector.name, 'pause')} disabled={saving} title="Pause">
                            <Pause size={15} />
                          </button>
                          <button className="ds-button" onClick={() => connectorAction(connector.name, 'resume')} disabled={saving} title="Resume">
                            <Play size={15} />
                          </button>
                          <button className="ds-button" onClick={() => connectorAction(connector.name, 'restart')} disabled={saving} title="Restart">
                            <RotateCw size={15} />
                          </button>
                          <button className="ds-button danger" onClick={() => connectorAction(connector.name, 'delete')} disabled={saving} title="Delete">
                            <Trash2 size={15} />
                          </button>
                        </div>
                      </td>
                    )}
                  </tr>
                ))
              ) : (
                <tr><td colSpan={canManage ? 5 : 4} className="ds-empty">No connectors found.</td></tr>
              )}
            </tbody>
          </table>
        )}

        {activeTab === 'plugins' && (
          <table className="ds-table ds-kc-table">
            <thead>
              <tr><th>Class</th><th>Type</th><th>Version</th></tr>
            </thead>
            <tbody>
              {summary && summary.plugins.length > 0 ? (
                summary.plugins.map(plugin => (
                  <tr key={`${plugin.class}-${plugin.type}`}>
                    <td>{plugin.class}</td>
                    <td>{plugin.type}</td>
                    <td>{plugin.version}</td>
                  </tr>
                ))
              ) : (
                <tr><td colSpan={3} className="ds-empty">No connector plugins found.</td></tr>
              )}
            </tbody>
          </table>
        )}
      </div>
      </>}

      {/* Ã¢â€â‚¬Ã¢â€â‚¬ Connection modal Ã¢â€â‚¬Ã¢â€â‚¬ */}
      {canManage && showConnection && createPortal(
        <div className="ds-modal-backdrop" role="dialog" aria-modal="true">
          <div className="ds-modal ds-connection-modal" style={{ width: '680px', borderRadius: 'var(--radius-lg)', background: "var(--bg-surface)", padding: 'var(--space-6)', boxShadow: '0px 22px 60px rgba(0, 0, 0, 0.24)' }}>
            <div className="ds-modal-header" style={{ border: 'none', padding: '0 0 20px 0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                <h3 style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: '18px', color: 'var(--button-primary-active)', margin: 0 }}>Add Kafka Connect Connection</h3>
                <span className="ds-muted-line" style={{ fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', marginTop: '4px', display: 'block' }}>New connection</span>
              </div>
              <button type="button" className="ds-icon-button" onClick={() => setShowConnection(false)} title="Close" style={{ border: 'none', background: 'none', cursor: 'pointer', color: 'var(--text-tertiary)' }}>
                <X size={20} />
              </button>
            </div>
            
            <div className="ds-form ds-compact-form" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-4)', background: '#F8F9FA', border: '1px solid #ECECF1', borderRadius: 'var(--radius-md)', padding: 'var(--space-6)', marginBottom: '24px' }}>
              {connectError && <div className="ds-alert" style={{ marginBottom: 12 }}>{connectError}</div>}
              {selectedConn?.status && editingConnectionId && (
                <div style={{ padding: '8px 12px', background: 'rgba(255,255,255,0.05)', borderRadius: 4, marginBottom: 12, fontSize: 13 }}>
                  Status: <strong style={{ color: connStatusColor(selectedConn.status) }}>{selectedConn.status}</strong>
                  {selectedConn.certificateConfigured && <span style={{ marginLeft: 16 }}>Cert Configured</span>}
                  {selectedConn.truststoreConfigured && <span style={{ marginLeft: 16 }}>Truststore Password Configured</span>}
                </div>
              )}
              
              <div className="ds-field" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
                <label style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-sm)', color: 'var(--button-primary-active)' }}>Connection Name</label>
                <input
                  value={formConnectionName}
                  onChange={e => setFormConnectionName(e.target.value)}
                  placeholder="e.g. ETL Kafka Connect"
                  required
                  style={{ width: '100%', height: '40px', background: "var(--bg-surface)", border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)', padding: '0 12px', fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-base)', outline: 'none' }}
                />
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 'var(--space-4)' }}>
                <div className="ds-field" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
                  <label style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-sm)', color: 'var(--button-primary-active)' }}>Protocol</label>
                  <div style={{ position: 'relative' }}>
                    <select 
                      value={protocol} 
                      onChange={e => setProtocol(e.target.value)}
                      style={{ width: '100%', height: '40px', background: "var(--bg-surface)", border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)', padding: '0 12px', fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-base)', outline: 'none', appearance: 'none', cursor: 'pointer' }}
                    >
                      <option value="http">http://</option>
                      <option value="https">https://</option>
                    </select>
                    <span style={{ position: 'absolute', right: '12px', top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', display: 'flex', alignItems: 'center' }}>
                      <ChevronDown size={16} style={{ color: 'var(--text-tertiary)' }} />
                    </span>
                  </div>
                </div>
                <div className="ds-field" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
                  <label style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-sm)', color: 'var(--button-primary-active)' }}>Host / IP</label>
                  <input 
                    value={customIp} 
                    onChange={e => setCustomIp(e.target.value)} 
                    placeholder="Host or IP address"
                    required 
                    style={{ width: '100%', height: '40px', background: "var(--bg-surface)", border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)', padding: '0 12px', fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-base)', outline: 'none' }}
                  />
                </div>
                <div className="ds-field" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
                  <label style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-sm)', color: 'var(--button-primary-active)' }}>Port</label>
                  <input 
                    type="number" 
                    value={customPort} 
                    onChange={e => setCustomPort(e.target.value)} 
                    placeholder="8083" 
                    required 
                    style={{ width: '100%', height: '40px', background: "var(--bg-surface)", border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)', padding: '0 12px', fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-base)', outline: 'none' }}
                  />
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'var(--space-4)' }}>
                <div className="ds-field" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
                  <label style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-sm)', color: 'var(--button-primary-active)' }}>Certificate Type</label>
                  <div style={{ position: 'relative' }}>
                    <select 
                      value={certType} 
                      onChange={e => { setCertType(e.target.value); setCertFile(null); setCertFileName(''); }}
                      style={{ width: '100%', height: '40px', background: "var(--bg-surface)", border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)', padding: '0 12px', fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-base)', outline: 'none', appearance: 'none', cursor: 'pointer' }}
                    >
                      <option value="PEM">PEM</option>
                      <option value="PKCS12">PKCS12</option>
                    </select>
                    <span style={{ position: 'absolute', right: '12px', top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', display: 'flex', alignItems: 'center' }}>
                      <ChevronDown size={16} style={{ color: 'var(--text-tertiary)' }} />
                    </span>
                  </div>
                </div>
                <div className="ds-field" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
                  <label style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-sm)', color: 'var(--button-primary-active)' }}>Certificate / Truststore</label>
                  <label 
                    className="ds-upload-control"
                    style={{
                      boxSizing: 'border-box',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      gap: 'var(--space-2)',
                      width: '100%',
                      height: '40px',
                      background: "var(--bg-surface)",
                      border: '1px solid #7F56D9',
                      borderRadius: 'var(--radius-md)',
                      cursor: 'pointer',
                      fontFamily: 'Satoshi, sans-serif',
                      fontSize: 'var(--text-base)',
                      color: '#7F56D9',
                      fontWeight: 'var(--font-medium)'
                    }}
                  >
                    <FileDown size={16} style={{ color: '#7F56D9' }} /> {certFileName || 'Choose file'}
                    <input
                      type="file"
                      accept={certType === 'PEM' ? '.pem,.crt,.cer' : '.p12,.pfx'}
                      onChange={e => { const f = e.target.files?.[0] || null; setCertFile(f); setCertFileName(f ? f.name : ''); }}
                      style={{ display: 'none' }}
                    />
                  </label>
                  {certFileName && <span className="ds-secret-note" style={{ fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-xs)', color: 'var(--color-success)', display: 'flex', alignItems: 'center', gap: '4px', marginTop: '4px' }}><CheckCircle size={14} /> {certFileName}</span>}
                </div>
              </div>

              {certType === 'PKCS12' && (
                <div className="ds-field" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
                  <label style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-sm)', color: 'var(--button-primary-active)' }}>Truststore Password {selectedConn?.truststoreConfigured && editingConnectionId ? '(Leave blank to keep existing)' : ''}</label>
                  <input 
                    type="password" 
                    value={certPassword} 
                    onChange={e => setCertPassword(e.target.value)} 
                    placeholder="Password" 
                    style={{ width: '100%', height: '40px', background: "var(--bg-surface)", border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)', padding: '0 12px', fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-base)', outline: 'none' }}
                  />
                </div>
              )}

              <div className="ds-default-toggle-row" style={{ display: 'flex', alignItems: 'center', gap: '12px', marginTop: '8px' }}>
                <label className="ds-toggle-switch" htmlFor="kc-is-default" style={{ position: 'relative', display: 'inline-block', width: '33px', height: '18px', cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    id="kc-is-default"
                    checked={formIsDefault}
                    onChange={e => setFormIsDefault(e.target.checked)}
                    style={{ opacity: 0, width: 0, height: 0 }}
                  />
                  <span className="ds-toggle-track" style={{
                    boxSizing: 'border-box',
                    position: 'absolute',
                    inset: 0,
                    display: 'flex',
                    flexDirection: 'row',
                    alignItems: 'center',
                    padding: '1.5px',
                    width: '33px',
                    height: '18px',
                    background: formIsDefault ? 'var(--button-primary)' : '#ADADAD',
                    border: formIsDefault ? '0.75px solid var(--button-primary)' : '0.75px solid #ADADAD',
                    borderRadius: '9px',
                    transition: 'background-color 0.2s, border-color 0.2s'
                  }}>
                    <span className="ds-toggle-thumb" style={{
                      width: '13.5px',
                      height: '13.5px',
                      background: "var(--bg-surface)",
                      borderRadius: '50%',
                      position: 'absolute',
                      left: formIsDefault ? '16.5px' : '1.5px',
                      transition: 'left 0.2s',
                      transform: 'none'
                    }} />
                  </span>
                </label>
                <label htmlFor="kc-is-default" className="ds-toggle-label" style={{ fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', cursor: 'pointer' }}>
                  Set as default connection for this cluster
                </label>
              </div>
            </div>

            <div className="ds-modal-footer" style={{ border: 'none', padding: '0', display: 'flex', justifyContent: 'flex-end', gap: '12px' }}>
              <button 
                className="ds-button" 
                onClick={() => setShowConnection(false)} 
                disabled={connectSaving}
                style={{
                  height: '38px',
                  padding: '0 20px',
                  background: "var(--bg-surface)",
                  border: '1px solid var(--border-default)',
                  borderRadius: 'var(--radius-md)',
                  fontFamily: 'Satoshi, sans-serif',
                  fontSize: 'var(--text-base)',
                  fontWeight: 'var(--font-medium)',
                  color: 'var(--button-primary-active)',
                  cursor: 'pointer'
                }}
              >
                Cancel
              </button>
              <button
                className="ds-button primary"
                onClick={handleSaveConnection}
                disabled={connectSaving || !customIp.trim() || !customPort.trim()}
                style={{
                  height: '38px',
                  padding: '0 20px',
                  background: 'var(--button-primary)',
                  border: 'none',
                  borderRadius: 'var(--radius-md)',
                  fontFamily: 'Satoshi, sans-serif',
                  fontSize: 'var(--text-base)',
                  fontWeight: 'var(--font-medium)',
                  color: "var(--text-light)",
                  cursor: 'pointer',
                  opacity: (connectSaving || !customIp.trim() || !customPort.trim()) ? 0.6 : 1
                }}
              >
                {connectSaving ? <RefreshCw size={16} className="spin" style={{ marginRight: '8px' }} /> : null} Save & Connect
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}

      {canManage && showCreate && createPortal(
        <div className="ds-modal-backdrop" role="dialog" aria-modal="true">
          <form className="ds-modal ds-upload-style-modal ds-connector-create-modal" onSubmit={createConnector}>
            <div className="ds-modal-header">
              <div>
                <h3>Create Connector</h3>
                <span className="ds-muted-line">Deploy connector configuration</span>
              </div>
              <button type="button" className="ds-close-btn" onClick={() => setShowCreate(false)} title="Close"><X size={20} /></button>
            </div>
            <div className="ds-form" style={{ padding: 0 }}>
              <div className="ds-modal-inner-card">
                {createError && <div className="ds-alert">{createError}</div>}
                <div className="ds-upload-row">
                  <label className="ds-button" htmlFor="connector-json-upload"><Upload size={16} /> Upload JSON files</label>
                  <input id="connector-json-upload" type="file" accept="application/json,.json" multiple hidden onChange={e => { void handleConnectorFiles(e.target.files); e.target.value = ''; }} />
                  <span>Choose multiple files, or paste a JSON array for bulk deployment.</span>
                </div>
                <div className="ds-field">
                  <label>Connector JSON</label>
                  <textarea value={connectorJson} onChange={e => setConnectorJson(e.target.value)} required />
                </div>
              </div>
            </div>
            <div className="ds-modal-footer">
              <button type="button" className="ds-button" onClick={() => setShowCreate(false)}>Cancel</button>
              <button type="submit" className="ds-button primary" disabled={saving}>
                {saving ? <RefreshCw size={16} className="spin" /> : <Plus size={16} />} Deploy
              </button>
            </div>
          </form>
        </div>,
        document.body
      )}
      {successMessage && createPortal(
        <div className="ds-modal-backdrop" role="dialog" aria-modal="true">
          <div className="ds-modal ds-success-modal">
            <CheckCircle size={48} /><h3>Deployment successful</h3><p>{successMessage}</p>
            <button className="ds-button primary" onClick={() => setSuccessMessage(null)}>Done</button>
          </div>
        </div>,
        document.body
      )}

    </div>
  );
}
