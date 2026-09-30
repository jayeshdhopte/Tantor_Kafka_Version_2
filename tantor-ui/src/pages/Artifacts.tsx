import { useState, useEffect, useRef, useCallback } from 'react';
import { createPortal } from 'react-dom';
import {
  Upload, XCircle, ChevronDown, ChevronUp,
  Loader2, X, RefreshCw, Server, DownloadCloud,
  Power, PowerOff, Trash2, AlertTriangle, MoreVertical, FileText
} from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { AnchoredMenu } from '../components/AnchoredMenu';
import './Artifacts.css';
import orangeBanner from '../assets/orange.png';

interface ArtifactVersionRaw {
  id: string;
  serviceType?: string;
  version: string;
  status?: string;
  createdAt?: string;
  fileSizeBytes?: number;
  fileName?: string;
  filename?: string;
  sha256: string;
  downloadUrl?: string;
  download_url?: string;
}

interface ArtifactVersion {
  id: string;
  service_type: string;
  version: string;
  available: boolean;
  release_date: string;
  size_mb: string;
  filename: string;
  sha256: string;
  download_url: string;
}

interface Host {
  id: string;
  hostname: string;
  status: string;
  agentStatus?: string;
  available?: boolean;
  ipAddress?: string;
  ipAddresses?: string;
}

interface HostParcel {
  id: string;
  hostId: string;
  hostIp?: string;
  parcelDir?: string;
  artifactId: string;
  serviceType: string;
  version: string;
  status: string;
  active: boolean;
  lastTaskId?: string;
  errorMsg?: string;
  updatedAt?: string;
}

type ParcelAction = 'distribute' | 'activate' | 'deactivate' | 'remove';

interface ArtifactAuditEvent {
  id: string;
  userName?: string;
  category?: string;
  action: string;
  resourceType?: string;
  artifactId?: string;
  status: string;
  details?: unknown;
  createdAt?: string;
}

const SCHEMA_REGISTRY_ARTIFACT_UPLOAD_ENABLED = false;

const artifactServiceOptions = [
  {
    value: 'KAFKA',
    label: 'Apache Kafka',
    versionPlaceholder: 'e.g. 3.9.2',
    directoryPlaceholder: 'custom/kafka (under configured repository root)',
    fileLabel: 'Kafka parcel binary (.tgz or .tar.gz)',
    fileAccept: '.tgz,.tar.gz',
    helper: 'Kafka binaries appear in the parcel list and can be distributed to hosts.',
  },
  {
    value: 'KAFKA_EXPORTER',
    label: 'Kafka Exporter',
    versionPlaceholder: 'e.g. 1.9.0',
    directoryPlaceholder: 'custom/kafka-exporter (under configured repository root)',
    fileLabel: 'Kafka exporter binary (.tar.gz)',
    fileAccept: '.tgz,.tar.gz',
    helper: 'Kafka exporter binaries for metrics collection.',
  },
  {
    value: 'JMX_EXPORTER',
    label: 'JMX Exporter',
    versionPlaceholder: 'e.g. 0.20.0',
    directoryPlaceholder: 'custom/jmx-exporter (under configured repository root)',
    fileLabel: 'JMX exporter jar (.jar)',
    fileAccept: '.jar',
    helper: 'JMX exporter jars are stored for Kafka monitoring deployments.',
  },
  {
    value: 'KAFKA_CONNECT',
    label: 'Kafka Connect',
    versionPlaceholder: 'e.g. 8.1.1',
    directoryPlaceholder: 'custom/kafka-connect (under configured repository root)',
    fileLabel: 'Kafka Connect distribution (.tgz or .tar.gz)',
    fileAccept: '.tgz,.tar.gz',
    helper: 'Upload a Connect distribution for managed cluster deployment.',
  },
  {
    value: 'SCHEMA_REGISTRY',
    label: 'Schema Registry',
    versionPlaceholder: 'e.g. 7.6.0',
    directoryPlaceholder: 'custom/schema-registry (under configured repository root)',
    fileLabel: 'Confluent Schema Registry binary (.tgz or .tar.gz)',
    fileAccept: '.tgz,.tar.gz',
    helper: 'Schema Registry binaries can be selected during Kafka cluster deployment.',
  },
].filter(option => SCHEMA_REGISTRY_ARTIFACT_UPLOAD_ENABLED || option.value !== 'SCHEMA_REGISTRY');

const artifactServiceLabels: Record<string, string> = {
  KAFKA: 'Kafka',
  KAFKA_EXPORTER: 'Kafka Exporter',
  JMX_EXPORTER: 'JMX Exporter',
  KAFKA_CONNECT: 'Kafka Connect',
  SCHEMA_REGISTRY: 'Schema Registry',
};

const artifactServiceLabel = (serviceType: string) =>
  artifactServiceLabels[serviceType] || serviceType
    .toLowerCase()
    .split('_')
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');

const summarizeError = (message: string, limit = 170) => {
  const clean = message
    .replace(/\s*Logs:\s*==>.*/is, '')
    .replace(/\s+/g, ' ')
    .trim();

  return clean.length <= limit ? clean : `${clean.slice(0, limit).trimEnd()}...`;
};

export function Artifacts() {
  const { canManage } = usePermissions();
  const [versions, setVersions] = useState<ArtifactVersion[]>([]);
  const [hosts, setHosts] = useState<Host[]>([]);
  const [hostParcels, setHostParcels] = useState<HostParcel[]>([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [actingKey, setActingKey] = useState<string | null>(null);
  const [uploadMsg, setUploadMsg] = useState<{ text: string; ok: boolean } | null>(null);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [serviceType, setServiceType] = useState('KAFKA');
  const [versionInput, setVersionInput] = useState('');
  const [uploadDirectory, setUploadDirectory] = useState('');
  const [universalDistributionDir, setUniversalDistributionDir] = useState(
    () => window.localStorage.getItem('tantor.universalDistributionDir') || '/srv/apps/tantor/parcels',
  );
  const [distributionDirs, setDistributionDirs] = useState<Record<string, string>>({});
  const [hostDistributionDirs, setHostDistributionDirs] = useState<Record<string, string>>({});
  const [selectedHosts, setSelectedHosts] = useState<Record<string, string[]>>({});
  const [openArtifactMenuId, setOpenArtifactMenuId] = useState<string | null>(null);
  const [artifactMenuAnchor, setArtifactMenuAnchor] = useState<HTMLElement | null>(null);
  const [auditModalArtifact, setAuditModalArtifact] = useState<ArtifactVersion | null>(null);
  const [artifactAuditEvents, setArtifactAuditEvents] = useState<ArtifactAuditEvent[]>([]);
  const [artifactAuditLoading, setArtifactAuditLoading] = useState(false);
  const [deleteConfirmVer, setDeleteConfirmVer] = useState<ArtifactVersion | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const selectedServiceOption = artifactServiceOptions.find(option => option.value === serviceType) || artifactServiceOptions[0];
  const fileMatchesServiceType = !file || selectedServiceOption.fileAccept
    .split(',')
    .some(extension => file.name.toLowerCase().endsWith(extension.trim().toLowerCase()));

  const fetchVersions = useCallback(async () => {
    const res = await fetch('/api/v1/artifacts?status=AVAILABLE&size=100');
    if (!res.ok) return;
    const data = await res.json();
    setVersions((data.content || []).map((a: ArtifactVersionRaw) => ({
      id: a.id,
      service_type: (a.serviceType || 'KAFKA').toUpperCase(),
      version: a.version,
      available: a.status === 'AVAILABLE',
      release_date: a.createdAt ? new Date(a.createdAt).toLocaleDateString() : '',
      size_mb: ((a.fileSizeBytes ?? 0) / 1024 / 1024).toFixed(1),
      filename: a.fileName ?? a.filename ?? '',
      sha256: a.sha256,
      download_url: a.downloadUrl || a.download_url || `/api/v1/artifacts/${a.id}/download`,
    })));
  }, []);

  const fetchHosts = useCallback(async () => {
    const res = await fetch('/api/v1/ui/hosts');
    if (!res.ok) return;
    setHosts(await res.json());
  }, []);

  const fetchParcelState = useCallback(async () => {
    const res = await fetch('/api/v1/ui/parcels');
    if (!res.ok) return;
    setHostParcels(await res.json());
  }, []);

  const refreshAll = useCallback(async () => {
    setLoading(true);
    try {
      await Promise.all([fetchVersions(), fetchHosts(), fetchParcelState()]);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, [fetchVersions, fetchHosts, fetchParcelState]);

  useEffect(() => {
    void (async () => { await refreshAll(); })();
    const timer = window.setInterval(() => { void (async () => { await fetchParcelState(); })(); }, 5000);
    return () => window.clearInterval(timer);
  }, [refreshAll, fetchParcelState]);

  useEffect(() => {
    window.localStorage.setItem('tantor.universalDistributionDir', universalDistributionDir);
  }, [universalDistributionDir]);

  const handleUploadSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!canManage) return;
    if (!file || !versionInput || !fileMatchesServiceType) return;
    setUploading(true);
    setUploadMsg(null);

    const form = new FormData();
    form.append('file', file);
    form.append('serviceType', serviceType);
    form.append('version', versionInput);
    if (uploadDirectory.trim()) form.append('storageDirectory', uploadDirectory.trim());
    form.append('overwrite', 'false');

    try {
      const res = await fetch('/api/v1/artifacts', { method: 'POST', body: form });
      if (res.ok) {
        setUploadMsg({ text: `Uploaded ${file.name} (${(file.size / 1024 / 1024).toFixed(1)} MB)`, ok: true });
        setShowUploadModal(false);
        setFile(null);
        setVersionInput('');
        setUploadDirectory('');
        await refreshAll();
      } else {
        const err = await res.json().catch(() => ({}));
        setUploadMsg({ text: err.detail || err.message || 'Upload failed.', ok: false });
      }
    } catch {
      setUploadMsg({ text: 'Upload failed due to a network error.', ok: false });
    } finally {
      setUploading(false);
      if (fileRef.current) fileRef.current.value = '';
    }
  };

  const getHostParcel = (artifactId: string, hostId: string) =>
    hostParcels.find(p => p.artifactId === artifactId && p.hostId === hostId);

  const isHostOnline = (host: Host) => {
    const status = (host.status || '').toUpperCase();
    const agentStatus = (host.agentStatus || '').toUpperCase();
    return agentStatus === 'ONLINE' || status === 'ONLINE' || status === 'AVAILABLE';
  };

  const runParcelAction = async (action: ParcelAction, ver: ArtifactVersion, host: Host) => {
    if (!canManage) return;
    const key = `${action}-${ver.id}-${host.id}`;
    setActingKey(key);
    setUploadMsg(null);
    try {
      const res = await fetch(`/api/v1/ui/parcels/${ver.id}/${action}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          hostIds: [host.id],
          checksum: ver.sha256,
          serviceType: ver.service_type,
          version: ver.version,
          fileName: ver.filename,
          parcelDir: hostDistributionDirs[`${ver.id}:${host.id}`] || distributionDirs[ver.id] || universalDistributionDir,
        }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.message || err.error || `${action} failed`);
      }
      await fetchParcelState();
      setUploadMsg({ text: `${actionLabel(action)} scheduled on ${host.hostname || host.id}`, ok: true });
    } catch (e) {
      setUploadMsg({ text: e instanceof Error ? e.message : `${actionLabel(action)} failed`, ok: false });
    } finally {
      setActingKey(null);
    }
  };

  const distributeAll = async (ver: ArtifactVersion) => {
    if (!canManage) return;
    const eligible = hosts.filter(host =>
      isHostOnline(host)
      && (() => {
        const state = getHostParcel(ver.id, host.id);
        return !state || ['FAILED', 'REMOVED'].includes(state.status);
      })()
    );
    if (eligible.length === 0) {
      setUploadMsg({ text: 'No online hosts are waiting for this parcel.', ok: false });
      return;
    }
    const key = `distribute-all-${ver.id}`;
    setActingKey(key);
    try {
      const res = await fetch(`/api/v1/ui/parcels/${ver.id}/distribute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          hostIds: eligible.map(host => host.id),
          checksum: ver.sha256,
          serviceType: ver.service_type,
          version: ver.version,
          fileName: ver.filename,
          parcelDir: distributionDirs[ver.id] || universalDistributionDir,
          parcelDirs: Object.fromEntries(eligible.map(host => [
            host.id,
            hostDistributionDirs[`${ver.id}:${host.id}`] || distributionDirs[ver.id] || universalDistributionDir,
          ])),
        }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.message || body.error || 'Distribute all failed.');
      }
      setUploadMsg({ text: `Distribution scheduled on ${eligible.length} hosts.`, ok: true });
      await fetchParcelState();
    } catch (e) {
      setUploadMsg({ text: e instanceof Error ? e.message : 'Distribute all failed.', ok: false });
    } finally {
      setActingKey(null);
    }
  };

  const distributeSelected = async (ver: ArtifactVersion) => {
    if (!canManage) return;
    const ids = selectedHosts[ver.id] || [];
    const targets = hosts.filter(host => ids.includes(host.id));
    if (!targets.length) {
      setUploadMsg({ text: 'Select at least one host.', ok: false });
      return;
    }
    const key = `distribute-selected-${ver.id}`;
    setActingKey(key);
    try {
      const res = await fetch(`/api/v1/ui/parcels/${ver.id}/distribute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          hostIds: targets.map(host => host.id),
          checksum: ver.sha256,
          serviceType: ver.service_type,
          version: ver.version,
          fileName: ver.filename,
          parcelDir: distributionDirs[ver.id] || universalDistributionDir,
          parcelDirs: Object.fromEntries(targets.map(host => [
            host.id,
            hostDistributionDirs[`${ver.id}:${host.id}`] || distributionDirs[ver.id] || universalDistributionDir,
          ])),
        }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.message || body.error || 'Selected-host distribution failed.');
      }
      setUploadMsg({ text: `Distribution scheduled on ${targets.length} selected host${targets.length === 1 ? '' : 's'}.`, ok: true });
      setSelectedHosts(current => ({ ...current, [ver.id]: [] }));
      await fetchParcelState();
    } catch (e) {
      setUploadMsg({ text: e instanceof Error ? e.message : 'Selected-host distribution failed.', ok: false });
    } finally {
      setActingKey(null);
    }
  };

  const toggleHostSelection = (artifactId: string, hostId: string) => {
    if (!canManage) return;
    setSelectedHosts(current => {
      const selected = current[artifactId] || [];
      return {
        ...current, [artifactId]: selected.includes(hostId)
          ? selected.filter(id => id !== hostId)
          : [...selected, hostId]
      };
    });
  };

  const deleteArtifactBinary = (ver: ArtifactVersion) => {
    if (!canManage) return;
    setOpenArtifactMenuId(null);
    const inUse = hostParcels.some(p => p.artifactId === ver.id && p.status !== 'REMOVED');
    if (inUse) {
      setUploadMsg({
        text: `Remove ${artifactServiceLabel(ver.service_type)} ${ver.version} from all hosts before deleting the binary.`,
        ok: false,
      });
      return;
    }
    setDeleteConfirmVer(ver);
  };

  const confirmDeleteArtifactBinary = async (ver: ArtifactVersion) => {
    const key = `delete-artifact-${ver.id}`;
    setActingKey(key);
    setUploadMsg(null);
    setDeleteConfirmVer(null);
    try {
      const res = await fetch(`/api/v1/artifacts/${ver.id}`, {
        method: 'DELETE',
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || err.message || err.error || 'Delete failed.');
      }
      if (expanded === ver.id) setExpanded(null);
      setUploadMsg({ text: `Deleted ${artifactServiceLabel(ver.service_type)} ${ver.version} binary.`, ok: true });
      await refreshAll();
    } catch (e) {
      setUploadMsg({ text: e instanceof Error ? e.message : 'Delete failed.', ok: false });
    } finally {
      setActingKey(null);
    }
  };

  const openArtifactLogs = async (ver: ArtifactVersion) => {
    setOpenArtifactMenuId(null);
    setAuditModalArtifact(ver);
    setArtifactAuditEvents([]);
    setArtifactAuditLoading(true);
    try {
      const res = await fetch(`/api/v1/artifacts/audit/${ver.id}`);
      if (!res.ok) throw new Error('Unable to load artifact logs.');
      const body = await res.json();
      setArtifactAuditEvents(body.events || []);
    } catch (e) {
      setUploadMsg({ text: e instanceof Error ? e.message : 'Unable to load artifact logs.', ok: false });
    } finally {
      setArtifactAuditLoading(false);
    }
  };

  const actionButton = (action: ParcelAction, ver: ArtifactVersion, host: Host, disabled = false) => {
    const key = `${action}-${ver.id}-${host.id}`;
    const Icon = actionIcon(action);
    return (
      <button
        key={action}
        className={`parcel-action ${action}`}
        disabled={disabled || actingKey !== null || !canManage}
        onClick={() => runParcelAction(action, ver, host)}
        title={actionLabel(action)}
      >
        {actingKey === key ? <Loader2 size={13} className="spin" /> : <Icon size={13} />}
        {actionLabel(action)}
      </button>
    );
  };

  const formatParcelStatus = (status?: string) => {
    if (!status || status === 'AVAILABLE') return 'Available';
    return status
      .toLowerCase()
      .split('_')
      .map(part => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
  };

  const renderActions = (ver: ArtifactVersion, host: Host, state?: HostParcel) => {
    if (!canManage) {
      return <span className="parcel-blocked">View only</span>;
    }
    const hostOnline = isHostOnline(host);
    if (!hostOnline) {
      return (
        <span className="parcel-blocked">
          <AlertTriangle size={13} />
          Host offline
        </span>
      );
    }
    if (!ver.available) {
      return (
        <span className="parcel-blocked">
          <XCircle size={13} />
          Artifact unavailable
        </span>
      );
    }
    const status = state?.status || 'AVAILABLE';
    if (['DISTRIBUTING', 'ACTIVATING', 'DEACTIVATING', 'REMOVING'].includes(status)) {
      const displayLabel = status === 'DISTRIBUTING' ? 'Distributing' :
        status === 'ACTIVATING' ? 'Activating' :
          status === 'DEACTIVATING' ? 'Deactivating' : 'Removing';
      return (
        <button className={`parcel-action progress ${status.toLowerCase()}`} disabled>
          <Loader2 size={13} className="spin" />
          {displayLabel}
        </button>
      );
    }
    if (!state || status === 'REMOVED') {
      return actionButton('distribute', ver, host);
    }
    if (status === 'FAILED') {
      return (
        <>
          {actionButton('distribute', ver, host)}
          {actionButton('remove', ver, host)}
        </>
      );
    }
    if (status === 'ACTIVE') {
      return actionButton('deactivate', ver, host);
    }
    return (
      <>
        {actionButton('activate', ver, host)}
        {actionButton('remove', ver, host)}
      </>
    );
  };

  return (
    <div className="artifacts-page animate-fade-in" onClick={() => { setOpenArtifactMenuId(null); setArtifactMenuAnchor(null); }}>
      <header className="page-header flex-between">
        <div>
          <h1>Artifacts</h1>
          <p>Manage your Linux Distribute, activate, deactivate, and remove Kafka parcels on managed hostservers for Kafka deployment</p>
        </div>
        <div className="header-actions">
          {uploadMsg && (
            <span className={`upload-msg ${uploadMsg.ok ? 'ok' : 'err'}`}>
              {uploadMsg.text}
            </span>
          )}
          <button className="btn btn-sync" onClick={refreshAll} disabled={loading || actingKey !== null}>
            <RefreshCw size={14} className={loading ? 'spin' : ''} />
            Sync
          </button>
          {canManage && (
            <button className="btn btn-primary-action" onClick={() => setShowUploadModal(true)} disabled={uploading}>
              {uploading ? <Loader2 size={14} className="spin" /> : <Upload size={14} />}
              Upload binary
            </button>
          )}
        </div>
      </header>

      <section className="universal-distribution-directory">
        <label>Universal distribution directory</label>
        <input
          className="form-control"
          value={universalDistributionDir}
          onChange={event => setUniversalDistributionDir(event.target.value)}
          placeholder="/srv/apps/tantor/parcels"
          disabled={!canManage}
        />
      </section>

      {loading ? (
        <div className="state-center">
          <Loader2 size={28} className="spin" />
          <p>Loading parcels...</p>
        </div>
      ) : versions.length === 0 ? (
        <div className="state-center no-artifacts">
          <div className="no-artifacts-illustration-container">
            <svg className="no-artifacts-illustration" width="130" height="100" viewBox="0 0 130 100" fill="none" xmlns="http://www.w3.org/2000/svg">
              {/* Card 1 */}
              <rect x="15" y="10" width="100" height="20" rx="4" fill="white" stroke="var(--border-subtle)" strokeWidth="1" />
              <rect x="25" y="18" width="20" height="4" rx="1" fill="#A78BFA" opacity="0.6" />
              <rect x="55" y="18" width="20" height="4" rx="1" fill="#A78BFA" opacity="0.6" />
              {/* Card 2 */}
              <rect x="15" y="36" width="100" height="20" rx="4" fill="white" stroke="#D1D5DB" strokeWidth="1.2" />
              <rect x="25" y="44" width="24" height="4" rx="1" fill="#8B5CF6" opacity="0.8" />
              <rect x="57" y="44" width="24" height="4" rx="1" fill="#8B5CF6" opacity="0.8" />
              {/* Card 3 */}
              <rect x="15" y="62" width="100" height="20" rx="4" fill="white" stroke="var(--border-subtle)" strokeWidth="1" />
              <rect x="25" y="70" width="20" height="4" rx="1" fill="#A78BFA" opacity="0.6" />
              <rect x="55" y="70" width="20" height="4" rx="1" fill="#A78BFA" opacity="0.6" />
              {/* Reflection lines under cards */}
              <line x1="25" y1="90" x2="45" y2="90" stroke="var(--bg-neutral)" strokeWidth="1.5" strokeLinecap="round" />
              <line x1="55" y1="90" x2="75" y2="90" stroke="var(--bg-neutral)" strokeWidth="1.5" strokeLinecap="round" />
              <line x1="85" y1="90" x2="105" y2="90" stroke="var(--bg-neutral)" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
          </div>
          <h3>No Artifacts</h3>
          <p>Upload your first artifact to distribute and manage resources across hosts.</p>
        </div>
      ) : (
        <div className="artifacts-list-container">
          <h3 className="section-title">Artifact List</h3>
          <div className="versions-list">
            {versions.map(ver => {
              const isKafkaArtifact = ver.service_type === 'KAFKA';
              const isOpen = isKafkaArtifact && expanded === ver.id;
              const distributed = hostParcels.filter(p => p.artifactId === ver.id && p.status !== 'REMOVED').length;
              const active = hostParcels.filter(p => p.artifactId === ver.id && p.active).length;
              const deleteKey = `delete-artifact-${ver.id}`;
              const canDeleteBinary = distributed === 0 && active === 0;
              return (
                <div key={ver.id} className="version-card">
                  <div className="version-card-top">
                    <button
                      className={`version-card-header ${isKafkaArtifact ? '' : 'non-expandable'}`}
                      onClick={() => {
                        if (isKafkaArtifact) setExpanded(isOpen ? null : ver.id);
                      }}
                    >
                      <div className="status-dot-container">
                        <span className={`status-dot ${ver.available ? 'available' : 'unavailable'}`}></span>
                      </div>
                      <div className="version-info">
                        <div className="version-title-row">
                          <span className="version-name">{artifactServiceLabel(ver.service_type)} {ver.version}</span>
                          {ver.available ? (
                            <span className="status-badge available">Available</span>
                          ) : (
                            <span className="status-badge unavailable">Not downloaded</span>
                          )}
                          {active > 0 && <span className="status-badge active">Active on {active}</span>}
                        </div>
                        <div className="version-meta">
                          <span>Uploaded: {ver.release_date || 'N/A'}</span>
                          <span className="meta-separator">|</span>
                          <span>Size: {ver.size_mb} MB</span>
                          <span className="meta-separator">|</span>
                          <span className="meta-filename">File: {ver.filename}</span>
                        </div>
                      </div>
                    </button>
                    <div className="version-card-tools">
                      <div className="artifact-menu-anchor" onClick={event => event.stopPropagation()}>
                        <button
                          className="artifact-menu-button"
                          onClick={event => {
                            const opening = openArtifactMenuId !== ver.id;
                            setOpenArtifactMenuId(opening ? ver.id : null);
                            setArtifactMenuAnchor(opening ? event.currentTarget : null);
                          }}
                          title="Artifact actions"
                        >
                          <MoreVertical size={15} />
                        </button>
                        {openArtifactMenuId === ver.id && artifactMenuAnchor && (
                          <AnchoredMenu anchor={artifactMenuAnchor} className="artifact-action-menu" onClose={() => { setOpenArtifactMenuId(null); setArtifactMenuAnchor(null); }}>
                            <button onClick={() => openArtifactLogs(ver)}>
                              <FileText size={14} />
                              View Log
                            </button>
                            {canManage && (
                              <button
                                className="menu-item-delete"
                                disabled={!canDeleteBinary || actingKey !== null}
                                onClick={() => deleteArtifactBinary(ver)}
                                title={canDeleteBinary ? 'Delete uploaded binary' : 'Remove from all hosts before deleting binary'}
                              >
                                {actingKey === deleteKey ? <Loader2 size={14} className="spin" /> : <Trash2 size={14} />}
                                Delete
                              </button>
                            )}
                          </AnchoredMenu>
                        )}
                      </div>
                      {isKafkaArtifact && (
                        <div className="chevron-box" onClick={() => setExpanded(isOpen ? null : ver.id)}>
                          <span className="chevron">{isOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}</span>
                        </div>
                      )}
                    </div>
                  </div>

                  {isKafkaArtifact && isOpen && (
                    <div className="version-card-body">
                      {canManage && (
                        <div className="parcel-distribution-controls">
                          <div className="override-input-group">
                            <label>Artifact Directory Override (Optional)</label>
                            <input
                              className="form-control"
                              value={distributionDirs[ver.id] || ''}
                              onChange={event => setDistributionDirs(current => ({ ...current, [ver.id]: event.target.value }))}
                              placeholder={universalDistributionDir}
                            />
                          </div>
                          <div className="override-actions">
                            <button className="btn btn-distribute-selected" onClick={() => distributeSelected(ver)} disabled={actingKey !== null || !(selectedHosts[ver.id]?.length)}>
                              {actingKey === `distribute-selected-${ver.id}` ? <Loader2 size={14} className="spin" /> : <DownloadCloud size={14} />}
                              Distribute Selected
                            </button>
                            <button className="btn btn-distribute-all" onClick={() => distributeAll(ver)} disabled={actingKey !== null}>
                              {actingKey === `distribute-all-${ver.id}` ? <Loader2 size={14} className="spin" /> : <DownloadCloud size={14} />}
                              Distribute All
                            </button>
                          </div>
                        </div>
                      )}
                      {hosts.length === 0 ? (
                        <div className="parcel-empty-hosts">
                          <Server size={18} />
                          No hosts are registered yet.
                        </div>
                      ) : (
                        <div className="parcel-host-table">
                          <div className="parcel-host-row header">
                            <span className="col-host-select">
                              {canManage && (
                                <input
                                  type="checkbox"
                                  checked={hosts.length > 0 && (selectedHosts[ver.id]?.length || 0) === hosts.length}
                                  onChange={event => setSelectedHosts(current => ({
                                    ...current,
                                    [ver.id]: event.target.checked ? hosts.map(host => host.id) : [],
                                  }))}
                                />
                              )}
                              <span style={{ marginLeft: '8px' }}>Host</span>
                            </span>
                            <span>State</span>
                            <span>Destination Path</span>
                            <span>Action</span>
                          </div>
                          {hosts.map(host => {
                            const state = getHostParcel(ver.id, host.id);
                            const status = state?.status || 'AVAILABLE';
                            return (
                              <div key={host.id} className="parcel-host-row">
                                <div className="parcel-host">
                                  {canManage && (
                                    <input
                                      type="checkbox"
                                      checked={(selectedHosts[ver.id] || []).includes(host.id)}
                                      onChange={() => toggleHostSelection(ver.id, host.id)}
                                    />
                                  )}
                                  <div className="host-details">
                                    <strong>{host.hostname || host.id}</strong>
                                    <span className="host-uuid">{host.id}</span>
                                  </div>
                                </div>
                                  <div>
                                    <span className={`parcel-status ${status.toLowerCase()}`}>
                                      {formatParcelStatus(status)}
                                    </span>
                                  </div>
                                  <div className="parcel-host-destination">
                                  <input
                                    className="form-control"
                                    value={hostDistributionDirs[`${ver.id}:${host.id}`] || ''}
                                    onChange={event => setHostDistributionDirs(current => ({
                                      ...current,
                                      [`${ver.id}:${host.id}`]: event.target.value,
                                    }))}
                                    placeholder={distributionDirs[ver.id] || universalDistributionDir}
                                    disabled={!canManage}
                                  />
                                </div>
                                  <div className="parcel-actions">
                                    {renderActions(ver, host, state)}
                                  </div>
                                  {state?.errorMsg && (
                                    <details className="parcel-error-details">
                                      <summary>
                                        <AlertTriangle size={15} aria-hidden="true" />
                                        <span>{summarizeError(state.errorMsg)}</span>
                                        <b>View details</b>
                                      </summary>
                                      <pre>{state.errorMsg}</pre>
                                    </details>
                                  )}
                                </div>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {canManage && showUploadModal ? createPortal(
        <div className="modal-overlay" onClick={() => setShowUploadModal(false)}>
          <div className="modal upload-parcel-modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header upload-parcel-header">
              <div className="upload-parcel-heading">
                <h2>Upload Parcel Binary</h2>
                <p className="modal-subtitle">Upload a Kafka, Kafka Exporter, or JMX Exporter binary to the internal artifact repository.</p>
              </div>
              <button className="modal-close" onClick={() => setShowUploadModal(false)}>
                <X size={14} />
              </button>
            </div>

            <form onSubmit={handleUploadSubmit} style={{ display: 'flex', flexDirection: 'column', flex: 1 }}>
              <div className="modal-body">
                {uploadMsg && !uploadMsg.ok && (
                  <div role="alert" className="upload-msg err" style={{ marginBottom: '1rem', whiteSpace: 'normal', overflowWrap: 'anywhere' }}>
                    {uploadMsg.text}
                  </div>
                )}
                <div className="form-group">
                  <label>Service Type</label>
                  <select
                    className="form-control"
                    value={serviceType}
                    onChange={e => {
                      setServiceType(e.target.value);
                      setFile(null);
                      if (fileRef.current) fileRef.current.value = '';
                    }}
                  >
                    {artifactServiceOptions.map(option => (
                      <option key={option.value} value={option.value}>{option.label}</option>
                    ))}
                  </select>
                  <small className="form-hint">{selectedServiceOption.helper}</small>
                </div>

                <div className="form-group">
                  <label>Version Number</label>
                  <input
                    type="text"
                    className="form-control"
                    value={versionInput}
                    onChange={e => setVersionInput(e.target.value)}
                    placeholder={selectedServiceOption.versionPlaceholder}
                    required
                  />
                </div>

                <div className="form-group">
                  <label>Repository Subdirectory (Optional)</label>
                  <input
                    type="text"
                    className="form-control"
                    value={uploadDirectory}
                    onChange={e => setUploadDirectory(e.target.value)}
                    placeholder={selectedServiceOption.directoryPlaceholder}
                  />
                </div>

                <div className="form-group">
                  <label>{selectedServiceOption.fileLabel}</label>
                  <div className="upload-dropzone" onClick={() => fileRef.current?.click()}>
                    <span className="upload-dropzone-icon-shell">
                      <Upload size={22} className="upload-dropzone-icon" />
                    </span>
                    {file ? (
                      <>
                        <span className="dropzone-filename">{file.name}</span>
                        <span className="dropzone-size">{(file.size / 1024 / 1024).toFixed(2)} MB</span>
                      </>
                    ) : (
                      <span className="dropzone-hint">Click to select a binary file</span>
                    )}
                    <input
                      type="file"
                      ref={fileRef}
                      style={{ display: 'none' }}
                      onChange={e => setFile(e.target.files?.[0] ?? null)}
                      accept={selectedServiceOption.fileAccept}
                    />
                  </div>
                  {!fileMatchesServiceType && (
                    <small className="form-error">Selected file does not match {selectedServiceOption.label}.</small>
                  )}
                </div>
              </div>

              <div className="modal-footer">
                <button type="button" className="btn btn-cancel-purple-outline" onClick={() => setShowUploadModal(false)}>
                  Cancel
                </button>
                <button type="submit" className="btn btn-primary-action" disabled={uploading || !file || !versionInput || !fileMatchesServiceType}>
                  {uploading ? 'Uploading...' : 'Upload'}
                </button>
              </div>
            </form>
          </div>
        </div>
        , document.body) : null}

      {auditModalArtifact ? createPortal(
        <div className="modal-overlay" onClick={() => setAuditModalArtifact(null)}>
          <div className="modal artifact-log-modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Artifacts Logs</h2>
              <button className="modal-close" onClick={() => setAuditModalArtifact(null)}>
                <X size={18} />
              </button>
            </div>

            <div className="modal-body" style={{ padding: '0 32px 24px 32px' }}>
              {artifactAuditLoading ? (
                <div className="artifact-log-empty"><Loader2 size={18} className="spin" /> Loading audit log...</div>
              ) : artifactAuditEvents.length === 0 ? (
                <div className="artifact-log-empty">No audit log entries found for this artifact.</div>
              ) : (
                <div className="artifact-log-box">
                  {artifactAuditEvents.map(event => {
                    const created = event.createdAt ? new Date(event.createdAt) : new Date();
                    const dateStr = created.toLocaleDateString('en-US') + ', ' + created.toLocaleTimeString('en-US');
                    const category = (event.category || 'PACKAGE').toUpperCase();
                    const action = (event.action || 'PACKAGE_UPLOADED').toUpperCase();
                    const status = (event.status || 'SUCCESS').toUpperCase();
                    const logLine = `${dateStr} ${category} ${action} ${status}`;
                    return (
                      <div key={event.id} className="log-line">
                        {logLine}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>

            <div className="modal-footer">
              <button type="button" className="btn btn-cancel-purple-outline" onClick={() => setAuditModalArtifact(null)}>
                Cancel
              </button>
            </div>
          </div>
        </div>
        , document.body) : null}

      {deleteConfirmVer ? createPortal(
        <div className="modal-overlay" onClick={() => setDeleteConfirmVer(null)}>
          <div className="modal delete-confirm-modal" onClick={e => e.stopPropagation()}>
            <div className="delete-modal-banner">
              <img src={orangeBanner} alt="Banner" className="delete-banner-image" />
              <button className="modal-close" onClick={() => setDeleteConfirmVer(null)}>
                <X size={14} />
              </button>
            </div>
            <div className="delete-confirm-content">
              <div className="delete-confirm-title-row">
                <AlertTriangle className="delete-alert-icon" size={20} />
                <h2>Delete Artifact</h2>
              </div>
              <p className="delete-confirm-subtitle">
                Are you sure you want to delete this artifact?
              </p>
              <p className="delete-confirm-desc">
                This action will permanently remove the artifact from the repository and cannot be undone.
              </p>
            </div>
            <div className="modal-footer delete-modal-footer">
              <button type="button" className="btn btn-cancel-pink-outline" onClick={() => setDeleteConfirmVer(null)}>
                Cancel
              </button>
              <button type="button" className="btn btn-primary-action" onClick={() => confirmDeleteArtifactBinary(deleteConfirmVer)}>
                Delete
              </button>
            </div>
          </div>
        </div>
        , document.body) : null}
    </div>
  );
}

function actionLabel(action: ParcelAction): string {
  return {
    distribute: 'Distribute',
    activate: 'Activate',
    deactivate: 'Deactivate',
    remove: 'Remove',
  }[action];
}

function actionIcon(action: ParcelAction) {
  return {
    distribute: DownloadCloud,
    activate: Power,
    deactivate: PowerOff,
    remove: Trash2,
  }[action];
}
