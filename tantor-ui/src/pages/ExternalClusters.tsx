import { useEffect, useMemo, useRef, useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  AlertTriangle,
  ChevronLeft,
  CheckCircle2,
  Copy,
  RefreshCw,
  Server,
  Network,
  FileText,
  X,
  Wifi,
} from 'lucide-react';
import { getValidToken } from '../services/KeycloakService';
import './ExternalClusters.css';

interface BrokerNode {
  node_id?: string | number;
  broker_id?: string | number;
  id?: string | number;
  host?: string;
  port?: number;
  isBroker?: boolean;
  isController?: boolean;
  hasActiveAgent?: boolean;
  agentDiscoveryKey?: string;
  [key: string]: unknown;
}

interface BootstrapResult {
  name?: string;
  success?: boolean;
  connected?: boolean;
  status?: string;
  bootstrapServers?: string;
  bootstrap_servers?: string;
  security_protocol?: string;
  mode?: string;
  kafkaMode?: string;
  clusterId?: string;
  cluster_id?: string;
  kafka_cluster_id?: string;
  brokerCount?: number;
  brokers?: BrokerNode[];
  topicCount?: number;
  topic_count?: number;
  topics?: unknown[];
  controllerId?: string | number;
  controller_id?: string | number;
  kafka_version?: string;
  kafkaVersion?: string;
  hostname?: string;
  security?: string;
  environment?: string;
  socket_results?: unknown[];
  message?: string;
  discoveryKey?: string;
}

const registrationKafkaMode = (result: BootstrapResult | null): string => {
  const raw = String(result?.kafkaMode || result?.mode || '').trim();
  if (/^zookeeper$/i.test(raw) || /^zk$/i.test(raw)) return 'ZooKeeper';
  if (/^kraft$/i.test(raw)) return 'KRaft';
  return 'Unknown';
};

interface DiscoveryAgentStatus {
  id: string;
  agentName?: string;
  hostname?: string;
  ipAddresses?: string;
  status?: string;
  health?: 'green' | 'orange' | 'red' | string;
  stateLabel?: string;
  lastHeartbeat?: string;
  canExecuteTasks?: boolean;
  clusterId?: string | null;
}

const TRUSTSTORE_FILE_RULES: Record<string, { accept: string; extensions: string[]; label: string }> = {
  PKCS12: { accept: '.p12,.pfx', extensions: ['.p12', '.pfx'], label: 'PKCS12 truststore' },
  PEM: { accept: '.pem,.crt,.cer', extensions: ['.pem', '.crt', '.cer'], label: 'PEM certificate' },
};

export function ExternalClusters() {
  const navigate = useNavigate();
  const [banner, setBanner] = useState('');
  const [error, setError] = useState('');
  const [testing, setTesting] = useState(false);
  const [registering, setRegistering] = useState(false);
  const [openPanel, setOpenPanel] = useState<'bootstrap' | 'agent'>('bootstrap');
  const [agents, setAgents] = useState<DiscoveryAgentStatus[]>([]);
  const [agentsLoading, setAgentsLoading] = useState(false);
  const [clusterNameError, setClusterNameError] = useState('');
  const [checkingClusterName, setCheckingClusterName] = useState(false);
  const clusterNameValidationSequence = useRef(0);

  const [form, setForm] = useState({
    name: '',
    environment: 'prod',
    bootstrapServers: '',
    kafkaVersion: '',
    securityProtocol: 'PLAINTEXT',
    saslMechanism: 'PLAIN',
    saslUsername: '',
    saslPassword: '',
    truststoreType: 'PKCS12',
    truststorePassword: '',
    truststoreBase64: '',
    truststoreFilename: '',
    keystoreType: 'PKCS12',
    keystorePassword: '',
    keyPassword: '',
    keystoreBase64: '',
    keystoreFilename: '',
    disableHostnameVerification: false,
  });
  const [bootstrapResult, setBootstrapResult] = useState<BootstrapResult | null>(null);
  const [selectedAgents, setSelectedAgents] = useState<Record<string, string>>({});
  const bootstrapIsZooKeeper = registrationKafkaMode(bootstrapResult) === 'ZooKeeper';
  const truststoreFileRule = TRUSTSTORE_FILE_RULES[form.truststoreType] || TRUSTSTORE_FILE_RULES.PKCS12;

  const serverHint = useMemo(() => {
    const host = window.location.hostname || '<tantor-server-ip>';
    if (host === 'localhost' || host === '127.0.0.1') {
      return 'https://<tantor-server-ip-or-dns>';
    }
    return `https://${host}`;
  }, []);

  const agentConfig = useMemo(() => (
    `discovery:
  server_url: "${serverHint}"
  host_id: "discovery-<vm-hostname-or-ip>"
  agent_name: "tantor-discovery-<vm-hostname-or-ip>"
  scan_paths:
    - "/opt"
    - "/srv"
    - "/data"
    - "/usr/local"
    - "/var/lib"
  interval: "15s"
  task_poll_interval: "5s"
  node_name: ""
  restart_command: "systemctl restart kafka.service"
  systemd_use_sudo: false
  metrics_url: "http://localhost:7071/metrics"
  disable_metrics: false
  skip_precheck: false
  tls_ca_cert: "/etc/tantor-discovery-agent/certs/control-plane-ca.crt"
  tls_client_cert: "/etc/tantor-discovery-agent/certs/discovery-agent.crt"
  tls_client_key: "/etc/tantor-discovery-agent/certs/discovery-agent.key"`
  ), [serverHint]);

  const loadAgents = useCallback(async () => {
    setAgentsLoading(true);
    try {
      const res = await fetch('/api/v1/ui/external-clusters/agents');
      if (res.ok) setAgents(await res.json());
    } catch (e) {
      console.error(e);
    } finally {
      setAgentsLoading(false);
    }
  }, []);

  useEffect(() => {
    void (async () => { await loadAgents(); })();
    const timer = window.setInterval(() => { void (async () => { await loadAgents(); })(); }, 10000);
    return () => window.clearInterval(timer);
  }, [loadAgents]);

  const formatHeartbeat = (value?: string) => {
    if (!value) return 'Never';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return 'Unknown';
    return date.toLocaleString([], {
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  const displayIp = (value?: string) => {
    if (!value) return '-';
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed.join(', ') || '-';
    } catch {
      // fall through to raw value
    }
    return value;
  };

  const validateClusterName = async (value = form.name) => {
    const name = value.trim();
    const validationSequence = ++clusterNameValidationSequence.current;
    if (!name) {
      setClusterNameError('');
      setCheckingClusterName(false);
      return true;
    }

    setCheckingClusterName(true);
    try {
      const res = await fetch(`/api/v1/ui/external-clusters/name-availability?name=${encodeURIComponent(name)}`);
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || 'Unable to validate the cluster name.');
      if (validationSequence !== clusterNameValidationSequence.current) return false;
      const available = data.available === true;
      setClusterNameError(available ? '' : (data.message || 'A cluster with this name already exists. Choose a different name.'));
      return available;
    } catch (validationError) {
      if (validationSequence !== clusterNameValidationSequence.current) return false;
      setClusterNameError(validationError instanceof Error
        ? validationError.message
        : 'Unable to validate the cluster name.');
      return false;
    } finally {
      if (validationSequence === clusterNameValidationSequence.current) {
        setCheckingClusterName(false);
      }
    }
  };

  const testBootstrap = async () => {
    if (!form.bootstrapServers.trim()) return;
    setTesting(true);
    setError('');
    setBanner('');
    setBootstrapResult(null);
    try {
      const res = await fetch('/api/v1/ui/external-clusters/bootstrap/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(form),
      });
      const data: BootstrapResult = await res.json();
      setBootstrapResult(data);

      if (data.brokers) {
        const initialSelection: Record<string, string> = {};
        data.brokers.forEach((b) => {
          if (b.hasActiveAgent && b.agentDiscoveryKey) {
            initialSelection[b.host ?? ''] = b.agentDiscoveryKey;
          }
        });
        setSelectedAgents(initialSelection);
      }

      if (!res.ok || data.connected !== true) {
        throw new Error(data.message || 'Bootstrap connection failed');
      }
      setBanner('Kafka details fetched successfully.');
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to inspect the Kafka cluster');
    } finally {
      setTesting(false);
    }
  };

  const handleFileUpload = (
    e: React.ChangeEvent<HTMLInputElement>,
    fieldBase64: string,
    fieldFilename: string,
    rule?: { extensions: string[]; label: string },
  ) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (rule) {
      const fileName = file.name.toLowerCase();
      const isAllowed = rule.extensions.some(ext => fileName.endsWith(ext));
      if (!isAllowed) {
        e.target.value = '';
        setForm(prev => ({
          ...prev,
          [fieldBase64]: '',
          [fieldFilename]: ''
        }));
        setBootstrapResult(null);
        setBanner('');
        setError(`${rule.label} must use ${rule.extensions.join(' or ')} file format.`);
        return;
      }
    }
    setError('');
    const reader = new FileReader();
    reader.onload = (evt) => {
      const result = evt.target?.result;
      if (typeof result === 'string') {
        const base64Content = result.split(',')[1] || result;
        setForm(prev => ({
          ...prev,
          [fieldBase64]: base64Content,
          [fieldFilename]: file.name
        }));
      }
    };
    reader.readAsDataURL(file);
  };

  const registerBootstrap = async () => {
    if (!form.bootstrapServers.trim() || bootstrapResult?.connected !== true) return;
    if (!(await validateClusterName())) return;
    setRegistering(true);
    setError('');
    setBanner('');
    try {
      const kafkaMode = registrationKafkaMode(bootstrapResult);
      const normalizedBrokers = (bootstrapResult?.brokers || []).map((broker) =>
        kafkaMode === 'ZooKeeper'
          ? { ...broker, isBroker: true, isController: false }
          : broker
      );
      const payload = {
        ...form,
        clusterId: bootstrapResult?.cluster_id || bootstrapResult?.kafka_cluster_id || bootstrapResult?.clusterId,
        brokerCount: bootstrapResult?.brokerCount
          ?? bootstrapResult?.brokers?.filter((node) => node.isBroker !== false).length
          ?? 0,
        agentFound: !!bootstrapResult?.brokers?.some((b) => b.hasActiveAgent),
        security: form.securityProtocol,
        brokers: normalizedBrokers,
        controllerId: bootstrapResult?.controllerId || bootstrapResult?.controller_id || null,
        kafkaVersion: bootstrapResult?.kafkaVersion || bootstrapResult?.kafka_version || 'Unknown',
        kafkaMode,
        discoveryKey: bootstrapResult?.discoveryKey,
        selectedAgents: selectedAgents
      };

      // Registration mutates managed-cluster state, so obtain a fresh Keycloak
      // token explicitly instead of relying solely on the global fetch wrapper.
      const token = await getValidToken();
      if (!token) {
        throw new Error('Your sign-in session is unavailable. Please sign in again and retry.');
      }

      const res = await fetch('/api/v1/ui/external-clusters/bootstrap/register', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const message = data.message || data.error || 'Failed to register external cluster';
        if (res.status === 409) {
          setClusterNameError(message);
          return;
        }
        throw new Error(message);
      }
      setBanner(`External cluster ${data.name || form.name || 'connected'} connected.`);
      setClusterNameError('');
      setForm(prev => ({ ...prev, name: '', bootstrapServers: '', kafkaVersion: '' }));
      setBootstrapResult(null);
      navigate(`/clusters`);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to connect external cluster');
    } finally {
      setRegistering(false);
    }
  };

  const copyAgentConfig = async () => {
    await navigator.clipboard.writeText(agentConfig);
    setBanner('Discovery agent config copied.');
  };

  return (
    <div className="external-page animate-fade-in">
      <div className="external-wrapper">
        <header className="external-header">
          <div className="external-header-left">
            <div className="back-clickable-title" onClick={() => navigate('/clusters')}>
              <ChevronLeft size={24} className="back-arrow-icon" />
              <h1>External Kafka Clusters</h1>
            </div>
            <p>Connect an external Kafka cluster using its bootstrap URL.</p>
          </div>

          <div className="external-tab-switcher">
            <button
              className={`tab-btn ${openPanel === 'bootstrap' ? 'active' : ''}`}
              onClick={() => setOpenPanel('bootstrap')}
            >
              Connect external cluster
            </button>
            <button
              className={`tab-btn ${openPanel === 'agent' ? 'active' : ''}`}
              onClick={() => setOpenPanel('agent')}
            >
              Discovery Agent setup
            </button>
          </div>
        </header>

        {(banner || error) && (
          <div className={`external-banner ${error ? 'error' : 'success'}`}>
            {error ? <AlertTriangle size={16} /> : <CheckCircle2 size={16} />}
            <span>{error || banner}</span>
          </div>
        )}

        <section className="external-connect-grid">
          {openPanel === 'bootstrap' && (
            <div className="external-bootstrap-container">
              <div className="external-card bootstrap-server-card">
                <div className="card-header-title">
                  <h3>Bootstrap server</h3>
                </div>
                <div className="form-row-three">
                  <div className="form-field-group">
                    <label>Cluster Name (Optional)</label>
                    <input
                      type="text"
                      placeholder="prod-external-01"
                      value={form.name}
                      className={clusterNameError ? 'field-invalid' : ''}
                      aria-invalid={clusterNameError ? 'true' : 'false'}
                      aria-describedby="external-cluster-name-message"
                      onBlur={() => void validateClusterName()}
                      onChange={e => {
                        clusterNameValidationSequence.current += 1;
                        setCheckingClusterName(false);
                        setClusterNameError('');
                        setForm(prev => ({ ...prev, name: e.target.value }));
                      }}
                    />
                    {(checkingClusterName || clusterNameError) && (
                      <span
                        id="external-cluster-name-message"
                        className={clusterNameError ? 'field-error-message' : 'field-validation-message'}
                      >
                        {clusterNameError || 'Checking name availability...'}
                      </span>
                    )}
                  </div>
                  <div className="form-field-group">
                    <label>Environment</label>
                    <select
                      value={form.environment}
                      onChange={e => setForm(prev => ({ ...prev, environment: e.target.value }))}
                    >
                      <option value="dev">Development</option>
                      <option value="test">Test</option>
                      <option value="staging">Staging</option>
                      <option value="prod">Production</option>
                    </select>
                  </div>
                  <div className="form-field-group bootstrap-url-field">
                    <label>Bootstrap URL</label>
                    <input
                      type="text"
                      placeholder="Broker host and port"
                      value={form.bootstrapServers}
                      onChange={e => {
                        setForm(prev => ({ ...prev, bootstrapServers: e.target.value }));
                        setBootstrapResult(null);
                      }}
                      onKeyDown={e => {
                        if (e.key === 'Enter') testBootstrap();
                      }}
                    />
                  </div>
                </div>
              </div>

              <div className="external-card security-config-card">
                <div className="card-header-title">
                  <h3>Security Configuration</h3>
                </div>
                <div className="form-grid-columns">
                  <div className="form-field-group security-protocol-field">
                    <label>Security Protocol</label>
                    <select
                      value={form.securityProtocol}
                      onChange={e => {
                        setForm(prev => ({ ...prev, securityProtocol: e.target.value }));
                        setBootstrapResult(null);
                      }}
                    >
                      <option value="PLAINTEXT">PLAINTEXT</option>
                      <option value="SSL">SSL</option>
                      <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
                      <option value="SASL_SSL">SASL_SSL</option>
                    </select>
                  </div>

                  {(form.securityProtocol === 'SASL_PLAINTEXT' || form.securityProtocol === 'SASL_SSL') && (
                    <>
                      <div className="form-field-group">
                        <label>SASL Mechanism</label>
                        <select
                          value={form.saslMechanism}
                          onChange={e => {
                            setForm(prev => ({ ...prev, saslMechanism: e.target.value }));
                            setBootstrapResult(null);
                          }}
                        >
                          <option value="PLAIN">PLAIN</option>
                          <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                          <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                        </select>
                      </div>
                      <div className="form-field-group">
                        <label>SASL Username</label>
                        <input
                          type="text"
                          placeholder="Username"
                          value={form.saslUsername}
                          onChange={e => {
                            setForm(prev => ({ ...prev, saslUsername: e.target.value }));
                            setBootstrapResult(null);
                          }}
                        />
                      </div>
                      <div className="form-field-group">
                        <label>SASL Password</label>
                        <input
                          type="password"
                          placeholder="Password"
                          value={form.saslPassword}
                          onChange={e => {
                            setForm(prev => ({ ...prev, saslPassword: e.target.value }));
                            setBootstrapResult(null);
                          }}
                        />
                      </div>
                    </>
                  )}

                  {(form.securityProtocol === 'SSL' || form.securityProtocol === 'SASL_SSL') && (
                    <>
                      <div className="form-field-group">
                        <label>Truststore Type</label>
                        <select
                          value={form.truststoreType}
                          onChange={e => {
                            setForm(prev => ({
                              ...prev,
                              truststoreType: e.target.value,
                              truststoreBase64: '',
                              truststoreFilename: '',
                            }));
                            setBootstrapResult(null);
                          }}
                        >
                          <option value="PKCS12">PKCS12</option>
                          <option value="PEM">PEM / X.509</option>
                        </select>
                      </div>
                      <div className="form-field-group">
                        <label>Truststore File (CA Certificate)</label>
                        <div className="file-upload-custom-wrapper">
                          <label className="file-upload-custom-btn">
                            <FileText size={15} />
                            <span>Choose file</span>
                            <input
                              key={form.truststoreType}
                              type="file"
                              accept={truststoreFileRule.accept}
                              onChange={e => {
                                handleFileUpload(e, 'truststoreBase64', 'truststoreFilename', truststoreFileRule);
                                setBootstrapResult(null);
                              }}
                              style={{ display: 'none' }}
                            />
                          </label>
                          {form.truststoreFilename && (
                            <div className="file-attached-badge">
                              <span className="file-attached-label">{form.truststoreFilename} attached</span>
                              <button
                                type="button"
                                className="clear-file-btn"
                                onClick={() => {
                                  setForm(prev => ({
                                    ...prev,
                                    truststoreBase64: '',
                                    truststoreFilename: '',
                                  }));
                                  setBootstrapResult(null);
                                }}
                                aria-label="Remove file"
                              >
                                <X size={12} />
                              </button>
                            </div>
                          )}
                        </div>
                      </div>
                      <div className="form-field-group ssl-hostname-field">
                        <label>Disable Hostname Verification</label>
                        <div className="checkbox-wrapper">
                          <input
                            type="checkbox"
                            checked={form.disableHostnameVerification}
                            onChange={e => {
                              setForm(prev => ({ ...prev, disableHostnameVerification: e.target.checked }));
                              setBootstrapResult(null);
                            }}
                          />
                          <span>Skip checking hostname in certificate</span>
                        </div>
                      </div>
                      {form.truststoreType !== 'PEM' && (
                        <div className="form-field-group ssl-password-field">
                          <label>Truststore Password</label>
                          <input
                            type="password"
                            placeholder="Password"
                            value={form.truststorePassword}
                            onChange={e => {
                              setForm(prev => ({ ...prev, truststorePassword: e.target.value }));
                              setBootstrapResult(null);
                            }}
                          />
                        </div>
                      )}
                    </>
                  )}
                </div>
              </div>

              {testing && (
                <div className="inspection-loading">
                  <RefreshCw size={15} className="spin" />
                  Testing direct connection via Kafka Admin API...
                </div>
              )}

              {bootstrapResult && (
                <div className={`inspection-result ${(bootstrapResult.success ?? bootstrapResult.connected) ? 'ok' : 'error'}`}>
                  <div className="inspection-result-header">
                    <div className="status-icon-wrapper">
                      {(bootstrapResult.success ?? bootstrapResult.connected) ? (
                        <span className="success-check-dot"><CheckCircle2 size={16} aria-hidden="true" /></span>
                      ) : (
                        <span className="error-warn-dot"><AlertTriangle size={16} aria-hidden="true" /></span>
                      )}
                    </div>
                    <div className="status-info-col">
                      <strong className="status-title">{form.name || bootstrapResult.name || 'External Kafka Cluster'}</strong>
                      <span className="status-desc">{(bootstrapResult.success ?? bootstrapResult.connected) ? 'Bootstrap connection verified' : 'Bootstrap connection failed'}</span>
                    </div>
                  </div>

                  <div className="bootstrap-summary">
                    <div className="summary-item">
                      <span>{bootstrapResult.brokerCount
                        ?? bootstrapResult.brokers?.filter((node) => node.isBroker !== false).length
                        ?? 0} broker(s) detected</span>
                    </div>
                    <div className="summary-item">
                      <span>
                        Version: {(() => {
                          const v = bootstrapResult.kafkaVersion || bootstrapResult.kafka_version || 'Auto-detected';
                          return v === 'auto-detected by Kafka client' ? 'Auto-detected' : v;
                        })()}
                      </span>
                    </div>
                    <div className="summary-item">
                      <span>
                        Mode: {(() => {
                          const m = bootstrapResult.mode || bootstrapResult.kafkaMode || 'Auto-detected';
                          return m === 'auto-detected by Kafka client' ? 'Auto-detected' : m;
                        })()}
                      </span>
                    </div>
                    <div className="summary-item">
                      <span>
                        Security: {form.securityProtocol || bootstrapResult.security_protocol || 'PLAINTEXT'}
                      </span>
                    </div>
                  </div>

                  {bootstrapResult.brokers && bootstrapResult.brokers.length > 0 && (
                    <div className="inspection-brokers" style={{ marginTop: '16px', background: '#f8fafc', padding: 'var(--space-3)', borderRadius: '6px', border: '1px solid var(--border-subtle)' }}>
                      <h4 style={{ marginBottom: '8px', fontSize: 'var(--text-xs)', fontWeight: '600', color: 'var(--text-muted)', textTransform: 'uppercase' }}>Discovered Nodes</h4>
                      <table style={{ width: '100%', fontSize: 'var(--text-sm)', borderCollapse: 'collapse' }}>
                        <thead>
                          <tr style={{ borderBottom: '1px solid #cbd5e1', textAlign: 'left', color: 'var(--text-muted)' }}>
                            <th style={{ padding: '6px', width: '40px' }}></th>
                            <th style={{ padding: '6px' }}>Node ID</th>
                            <th style={{ padding: '6px' }}>Host</th>
                            <th style={{ padding: '6px' }}>Port</th>
                            <th style={{ padding: '6px' }}>Role</th>
                          </tr>
                        </thead>
                        <tbody>
                          {bootstrapResult.brokers.map((broker) => {
                            const isSelected = !!selectedAgents[broker.host ?? ''];
                            const hasAgent = !!broker.hasActiveAgent;
                            return (
                              <tr key={broker.node_id ?? broker.broker_id ?? broker.id} style={{ borderBottom: '1px solid var(--border-subtle)', background: isSelected ? '#f0fdf4' : 'transparent' }}>
                                <td style={{ padding: '6px', textAlign: 'center' }}>
                                  <input
                                    type="checkbox"
                                    checked={isSelected}
                                    disabled={false}
                                    onChange={(e) => {
                                      setSelectedAgents(prev => {
                                        const next = { ...prev };
                                        if (e.target.checked) {
                                          next[broker.host ?? ''] = broker.agentDiscoveryKey || broker.host || '';
                                        } else {
                                          delete next[broker.host ?? ''];
                                        }
                                        return next;
                                      });
                                    }}
                                    style={{ cursor: 'pointer' }}
                                  />
                                </td>
                                <td style={{ padding: '6px' }}><strong>{broker.node_id ?? broker.broker_id ?? broker.id}</strong></td>
                                <td style={{ padding: '6px' }}>
                                  {broker.host}
                                  {!hasAgent && <span style={{ display: 'block', fontSize: '11px', color: '#94a3b8' }}>No telemetry / unmanaged</span>}
                                </td>
                                <td style={{ padding: '6px' }}>{broker.port}</td>
                                <td style={{ padding: '6px' }}>
                                  {bootstrapIsZooKeeper ? (
                                    <span style={{ color: '#3b82f6', fontWeight: 'var(--font-medium)' }}>Broker</span>
                                  ) : broker.isController && broker.isBroker ? (
                                    <span style={{ color: '#059669', fontWeight: 'var(--font-medium)' }}>Controller + Broker</span>
                                  ) : broker.isController ? (
                                    <span style={{ color: '#7c3aed', fontWeight: 'var(--font-medium)' }}>Controller</span>
                                  ) : (
                                    <span style={{ color: '#3b82f6', fontWeight: 'var(--font-medium)' }}>Broker</span>
                                  )}
                                </td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}

                  {bootstrapResult.message && !(bootstrapResult.success ?? bootstrapResult.connected) && (
                    <p className="inspection-error-message">{bootstrapResult.message}</p>
                  )}
                </div>
              )}

              <div className="external-actions-row">
                <button className="action-btn cancel-btn" onClick={() => navigate('/clusters')}>
                  Cancel
                </button>
                <button
                  className="action-btn test-btn"
                  onClick={testBootstrap}
                  title="Test Connection"
                  disabled={!form.bootstrapServers.trim() || testing}
                >
                  <RefreshCw size={14} className={testing ? 'spin' : ''} />
                  Test Connection
                </button>
                <button
                  className={`action-btn connect-btn ${bootstrapResult?.connected === true ? 'active' : ''}`}
                  onClick={registerBootstrap}
                  disabled={registering || checkingClusterName || !!clusterNameError || bootstrapResult?.connected !== true}
                >
                  {registering ? <RefreshCw size={14} className="spin" /> : <Network size={14} />}
                  Connect Cluster
                </button>
              </div>
            </div>
          )}

          {openPanel === 'agent' && (
            <div className="external-card agent-card">
              <div className="card-header-title">
                <h3>Discovery Agent</h3>
                <p className="card-header-subtitle">Full management path for restart, host metrics, and config persistence.</p>
              </div>
              <hr className="agent-divider" />

              <div className="agent-connectivity-header">
                <div>
                  <h3>Agent connectivity</h3>
                  <p>Shows discovery agents that are polling this Tantor server, even before Kafka is detected.</p>
                </div>
                <button className="btn" onClick={loadAgents} disabled={agentsLoading} aria-label="Refresh agents" title="Refresh">
                  <RefreshCw size={14} className={agentsLoading ? 'spin' : ''} />
                </button>
              </div>

              {agents.length === 0 ? (
                <div className="agent-empty-state">
                  <Wifi size={18} />
                  <div>
                    <strong>No discovery agent polling yet</strong>
                    <span>Start the agent on a client VM and this area will show the connection heartbeat.</span>
                  </div>
                </div>
              ) : (
                <div className="agent-status-grid">
                  {agents.map(agent => (
                    <div className={`agent-status-card ${agent.health || 'orange'}`} key={agent.id}>
                      <div className="agent-status-top">
                        <span className="agent-status-dot" />
                        <strong>{agent.agentName || agent.id}</strong>
                      </div>
                      <div className="agent-status-meta">
                        <span>{agent.stateLabel || agent.status || 'Unknown'}</span>
                        <span>Host: {agent.hostname || '-'}</span>
                        <span>IP: {displayIp(agent.ipAddresses)}</span>
                        <span>Last poll: {formatHeartbeat(agent.lastHeartbeat)}</span>
                        <span>{agent.canExecuteTasks ? 'Task control enabled' : 'Read-only heartbeat'}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              <hr className="agent-divider" />

              <div className="agent-flow">
                <div className="flow-step">
                  <span className="step-number">1</span>
                  <span className="step-text">Build or copy tantor-discovery-agent-linux to the Kafka VM.</span>
                </div>
                <div className="flow-step">
                  <span className="step-number">2</span>
                  <span className="step-text">Set server_url to this Tantor backend.</span>
                </div>
                <div className="flow-step">
                  <span className="step-number">3</span>
                  <span className="step-text">Run it with nohup; discovered clusters will report metrics and handle restart tasks.</span>
                </div>
              </div>

              <div className="code-block-container">
                <div className="code-block-header">
                  <button className="copy-btn" onClick={copyAgentConfig} title="Copy config">
                    <Copy size={16} />
                  </button>
                </div>
                <div className="code-block-body">
                  <pre>{agentConfig}</pre>
                </div>
              </div>

              <div className="agent-note">
                <Server size={18} className="note-icon" />
                <span className="note-text">The agent auto-detects KRaft vs ZooKeeper from Kafka properties. Users do not select the mode manually.</span>
              </div>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
