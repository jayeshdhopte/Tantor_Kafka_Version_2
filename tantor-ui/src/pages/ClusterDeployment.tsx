import { SchemaRegistryFields, KafkaConnectFields } from '../components/ServiceConfigurationFields';
import { validateServicePath as validatePath, defaultSchemaRegistryConfig, defaultKafkaConnectConfig, type SchemaRegistryConfig, type KafkaConnectConfig } from '../components/serviceConfiguration';
import { useState, useEffect, useMemo, useRef } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { confirmAction, notifyAction } from '../components/confirmUtils';
import { AnchoredMenu } from '../components/AnchoredMenu';
import {
  AlertTriangle,
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronLeft,
  FileText,
  Upload,
  Download,
  Loader2,
  MoreVertical,
  Play,
  RefreshCw,
  Search,
  Settings2,
  Trash2,
  X,
  XCircle,
} from 'lucide-react';
import { AgentConnectivityModal } from '../components/AgentConnectivityModal';
import { runtimeConfig } from '../config/runtimeConfig';
import './ClusterDeployment.css';

type Host = {
  id: string;
  hostname: string;
  status: string;
  available?: boolean;
  availabilityReason?: string;
  clusterId?: string;
  clusterName?: string;
  ipAddresses?: string;
  ipAddress?: string;
  ip_address?: string;
  agentType?: 'HOST' | 'KAFKA_DISCOVERY' | string;
  deployable?: boolean;
};

type ClusterHost = {
  hostId?: string;
  role?: string;
  nodeId?: number;
};

type ExistingCluster = {
  id: string;
  name: string;
  kafkaVersion: string;
  mode: string;
  environment?: string;
  kafkaClusterId?: string;
  config?: Record<string, unknown>;
  hosts?: ClusterHost[];
};

type KafkaVersionRaw = {
  id?: string;
  version: string;
  status?: string;
  attributes?: { scala_version?: string; release_date?: string };
  createdAt?: string;
  fileSizeBytes?: number;
  serviceType?: string;
  fileName?: string;
  filename?: string;
};

type KafkaVersionInfo = {
  version: string;
  available: boolean;
  scala_version: string;
  release_date: string;
  size_mb: number;
  filename: string;
  id?: string;
};

type SchemaArtifact = {
  id: string;
  version: string;
  serviceType?: string;
  fileName?: string;
  filename?: string;
  status?: string;
  sha256?: string;
  checksum?: string;
};

type DeploymentMode = 'kraft' | 'zookeeper';
type RoleChoice = 'broker_controller' | 'broker' | 'controller' | 'separate' | 'broker_zookeeper' | 'zookeeper';
type FlowStage = 'details' | 'preview';
type ConfigMode = 'default' | 'custom';
type ConfigKind = 'server' | 'broker' | 'controller' | 'zookeeper';
type AddonRole = 'schema_registry' | 'kafka_connect';
type PrereqStatus = 'IDLE' | 'QUEUED' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'REBOOT_REQUIRED';

type ServiceAssignment = {
  host_id: string;
  role: RoleChoice;
  node_id: number;
  configuration_mode: ConfigMode;
  properties_template: string;
  heap_size: string;
  listener_port?: number;
  controller_port?: number;
  jmx_port?: number;
  zookeeper_peer_port?: number;
  zookeeper_election_port?: number;
};

type HostPorts = {
  listenerPort: number;
  controllerPort: number;
  brokerJmxPort: number;
  controllerJmxPort: number;
  zookeeperPeerPort: number;
  zookeeperElectionPort: number;
};

type HostPortKey = keyof HostPorts;

const DEFAULT_BROKER_JMX_PORT = 7071;
const DEFAULT_CONTROLLER_JMX_PORT = 7072;
const SCHEMA_REGISTRY_DEPLOYMENT_ENABLED = true;
const KAFKA_CONNECT_DEPLOYMENT_ENABLED = true;

const isUnifiedKafkaArtifact = (artifact: SchemaArtifact) =>
  (artifact.fileName || artifact.filename || '').toLowerCase().startsWith('tantor-unified-kafka-');

type PropertyRow = {
  key: string;
  value: string;
  required?: boolean;
  locked?: boolean;
};

type NodeConfigState = {
  mode: ConfigMode;
  rows: PropertyRow[];
  heapSize: string;
};

type PrereqResult = {
  status: PrereqStatus;
  taskId?: string;
  logOutput: string;
  errorMsg: string;
};

function PrerequisiteLog({ result }: { result: PrereqResult }) {
  const lines = [result.errorMsg, result.logOutput].filter(Boolean).join('\n\n').split('\n');
  return <div className="cd-prereq-log">
    {lines.map((line, index) => {
      const tone = line.startsWith('[PASS]') ? 'pass'
        : line.startsWith('[FAIL]') || line.toLowerCase().includes('gate failed') ? 'fail'
          : line.startsWith('[WARN]') ? 'warn' : 'neutral';
      return <span className={tone} key={`${index}-${line}`}>{line || '\u00a0'}</span>;
    })}
  </div>;
}

type KraftValidationNode = {
  hostId: string;
  address: string;
  nodeId: number;
  role: string;
};

type KraftValidationReport = {
  valid: boolean;
  errors: string[];
  warnings: string[];
  acknowledgementRequired: boolean;
  clusterId: string;
  quorumMode: 'static' | 'dynamic';
  controllerCount: number;
  brokerCount: number;
  failureTolerance: number;
  controllerQuorum: string;
  nodes: KraftValidationNode[];
  generatedConfig: Record<string, string>;
};

const UI_ONLY_PROPERTY_KEYS = new Set(['node.host', 'advertised.host', 'controller.host', 'zookeeper.host']);

const KRAFT_ROLE_OPTIONS: Array<{ id: RoleChoice; label: string; detail: string }> = [
  {
    id: 'broker_controller',
    label: 'Broker + Controller',
    detail: 'One combined Kafka process using server.properties.',
  },
  {
    id: 'broker',
    label: 'Broker',
    detail: 'Broker process only using broker.properties.',
  },
  {
    id: 'controller',
    label: 'Controller',
    detail: 'Controller process only using controller.properties.',
  },
  {
    id: 'separate',
    label: 'Broker and Controller',
    detail: 'Two JVMs on the same VM using broker.properties and controller.properties.',
  },
];

const ZOOKEEPER_ROLE_OPTIONS: Array<{ id: RoleChoice; label: string; detail: string }> = [
  {
    id: 'broker_zookeeper',
    label: 'Broker + ZooKeeper',
    detail: 'Two JVM services on one VM using server.properties and zookeeper.properties.',
  },
  {
    id: 'broker',
    label: 'Broker',
    detail: 'Broker process only using server.properties.',
  },
  {
    id: 'zookeeper',
    label: 'ZooKeeper',
    detail: 'ZooKeeper process only using zookeeper.properties.',
  },
];

const KRAFT_COMMON_CONFIG_KINDS: ConfigKind[] = ['server', 'broker', 'controller'];
const ZOOKEEPER_COMMON_CONFIG_KINDS: ConfigKind[] = ['server', 'zookeeper'];
const SYNCED_BROKER_PROPERTY_KEYS = new Set([
  'num.partitions',
  'default.replication.factor',
  'min.insync.replicas',
  'offsets.topic.replication.factor',
  'transaction.state.log.replication.factor',
  'transaction.state.log.min.isr',
]);

function defaultCommonRows(kind: ConfigKind, mode: DeploymentMode): PropertyRow[] {
  if (mode === 'zookeeper' && kind === 'zookeeper') {
    return [
      { key: 'tickTime', value: '2000' },
      { key: 'initLimit', value: '5' },
      { key: 'syncLimit', value: '2' },
      { key: 'maxClientCnxns', value: '0' },
      { key: 'admin.enableServer', value: 'false' },
      { key: 'autopurge.purgeInterval', value: '1' },
      { key: 'autopurge.snapRetainCount', value: '10' },
      { key: '4lw.commands.whitelist', value: '*' },
    ];
  }

  if (mode === 'zookeeper' && kind !== 'server') return [];

  if (kind === 'controller') {
    return [
      { key: 'controller.quorum.election.timeout.ms', value: '5000' },
      { key: 'controller.quorum.fetch.timeout.ms', value: '5000' },
      { key: 'controller.quorum.election.backoff.max.ms', value: '5000' },
      { key: 'controller.quorum.request.timeout.ms', value: '10000' },
      { key: 'metadata.log.segment.bytes', value: '1073741824' },
      { key: 'metadata.log.segment.ms', value: '604800000' },
      { key: 'metadata.max.retention.bytes', value: '-1' },
      { key: 'metadata.max.retention.ms', value: '604800000' },
      { key: 'num.network.threads', value: '8' },
      { key: 'num.io.threads', value: '16' },
      { key: 'socket.send.buffer.bytes', value: '102400' },
      { key: 'socket.receive.buffer.bytes', value: '102400' },
      { key: 'socket.request.max.bytes', value: '104857600' },
    ];
  }

  const brokerRows: PropertyRow[] = [
    { key: 'num.partitions', value: '1' },
    { key: 'auto.create.topics.enable', value: 'false' },
    { key: 'default.replication.factor', value: '', required: true },
    { key: 'min.insync.replicas', value: '', required: true },
    { key: 'offsets.topic.replication.factor', value: '3' },
    { key: 'offsets.topic.num.partitions', value: '50' },
    { key: 'transaction.state.log.replication.factor', value: '3' },
    { key: 'transaction.state.log.min.isr', value: '2' },
    { key: 'message.max.bytes', value: '15728640' },
    { key: 'replica.fetch.max.bytes', value: '15728640' },
    { key: 'fetch.message.max.bytes', value: '15728640' },
    { key: 'socket.request.max.bytes', value: '104857600' },
    { key: 'log.segment.bytes', value: '1073741824' },
    { key: 'log.retention.hours', value: '72' },
    { key: 'log.retention.check.interval.ms', value: '300000' },
    { key: 'num.replica.fetchers', value: '4' },
    { key: 'replica.lag.time.max.ms', value: '30000' },
    { key: 'num.network.threads', value: '8' },
    { key: 'num.io.threads', value: '8' },
    { key: 'socket.send.buffer.bytes', value: '102400' },
    { key: 'socket.receive.buffer.bytes', value: '102400' },
    { key: 'group.initial.rebalance.delay.ms', value: '0' },
    { key: 'broker.rack', value: 'rack1' },
  ];

  if (kind === 'broker') return brokerRows;

  if (mode === 'zookeeper') {
    return [
      ...brokerRows,
      { key: 'zookeeper.connection.timeout.ms', value: '40000' },
    ];
  }

  return [
    ...brokerRows,
    { key: 'controller.quorum.election.timeout.ms', value: '5000' },
    { key: 'controller.quorum.fetch.timeout.ms', value: '5000' },
    { key: 'controller.quorum.election.backoff.max.ms', value: '5000' },
    { key: 'controller.quorum.request.timeout.ms', value: '10000' },
    { key: 'metadata.log.segment.bytes', value: '1073741824' },
    { key: 'metadata.log.segment.ms', value: '604800000' },
    { key: 'metadata.max.retention.bytes', value: '-1' },
    { key: 'metadata.max.retention.ms', value: '604800000' },
  ];
}

function commonConfigKindsForMode(mode: DeploymentMode): ConfigKind[] {
  return mode === 'zookeeper' ? ZOOKEEPER_COMMON_CONFIG_KINDS : KRAFT_COMMON_CONFIG_KINDS;
}

function createCommonConfigs(mode: DeploymentMode): Record<ConfigKind, PropertyRow[]> {
  return {
    server: defaultCommonRows('server', mode),
    broker: defaultCommonRows('broker', mode),
    controller: defaultCommonRows('controller', mode),
    zookeeper: defaultCommonRows('zookeeper', mode),
  };
}

function kafkaMajorVersion(version: string): number {
  return Number.parseInt(String(version).split('.')[0] || '0', 10);
}

function setRowsValue(rows: PropertyRow[], key: string, value: string): PropertyRow[] {
  return rows.map(row => row.key === key ? { ...row, value } : row);
}

function syncCommonRows(rows: PropertyRow[], config: Record<string, unknown>): PropertyRow[] {
  return rows.map(row => {
    if (row.key === 'default.replication.factor' && config.replication_factor) return { ...row, value: String(config.replication_factor) };
    if (row.key === 'min.insync.replicas' && config.min_insync_replicas) return { ...row, value: String(config.min_insync_replicas) };
    if (row.key === 'num.partitions' && config.num_partitions) return { ...row, value: String(config.num_partitions) };
    return row;
  });
}

function parseIpList(raw: unknown): string[] {
  if (Array.isArray(raw)) return raw.map(String).map(ip => ip.trim()).filter(Boolean);
  if (typeof raw === 'string' && raw.startsWith('[')) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return parsed.map(String).map(ip => ip.trim()).filter(Boolean);
    } catch { /* ignore */ }
  }
  if (typeof raw === 'string') return raw.split(',').map(ip => ip.trim()).filter(Boolean);
  return [];
}

function displayIp(host: Host): string {
  const ips = parseIpList(host.ip_address || host.ipAddress || host.ipAddresses);
  return ips.find(ip => ip.startsWith('192.168.'))
    || ips.find(ip => !ip.startsWith('127.') && !ip.startsWith('172.'))
    || ips[0]
    || 'Unknown';
}

function nodeAvailabilityMessage(host: Host): string {
  if (host.availabilityReason) return host.availabilityReason;
  const status = String(host.status || '').toUpperCase();
  if (status === 'PENDING') return 'Pending connection - use + Add Node';
  if (status === 'OCCUPIED_INTERNAL' || status === 'OCCUPIED_EXTERNAL' || status === 'OCCUPIED') return 'Kafka Already Deployed';
  return status || 'Unavailable';
}

function serializeProperties(rows: PropertyRow[]): string {
  return rows
    .filter(row => row.key.trim() && !UI_ONLY_PROPERTY_KEYS.has(row.key.trim()) && String(row.value).trim())
    .map(row => `${row.key.trim()}=${row.value}`)
    .join('\n');
}

function activeStatus(status: string): boolean {
  return ['PENDING', 'IN_PROGRESS', 'RUNNING', 'QUEUED'].includes(String(status || '').toUpperCase());
}

const CustomRefreshIcon = ({ size = 20, color = '#818181', className = '' }: { size?: number, color?: string, className?: string }) => (
  <svg width={size} height={size} viewBox='0 0 24 24' fill='none' stroke={color} strokeWidth='1.25' strokeLinecap='round' strokeLinejoin='round' className={className}>
    <path d='M 12 5 A 7 7 0 0 1 17 17' />
    <path d='M 18 13 L 17 17 L 21 16' />
    <path d='M 12 19 A 7 7 0 0 1 7 7' />
    <path d='M 6 11 L 7 7 L 3 8' />
  </svg>
);

export function ClusterDeployment({ onClose }: { onClose?: () => void }) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const addClusterId = searchParams.get('mode') === 'add' ? searchParams.get('clusterId') : null;
  const isAddNodeMode = Boolean(addClusterId);
  const [stage, setStage] = useState<FlowStage>('details');
  const [hosts, setHosts] = useState<Host[]>([]);
  const [versions, setVersions] = useState<KafkaVersionInfo[]>([]);
  const [existingCluster, setExistingCluster] = useState<ExistingCluster | null>(null);
  const [loadingHosts, setLoadingHosts] = useState(true);
  const [loadingVersions, setLoadingVersions] = useState(true);
  const [schemaArtifacts, setSchemaArtifacts] = useState<SchemaArtifact[]>([]);
  const [loadingCluster, setLoadingCluster] = useState(false);

  const [clusterName, setClusterName] = useState('');
  const [kafkaVersion, setKafkaVersion] = useState('');
  const [environment, setEnvironment] = useState('DEV');
  const [clusterConfigMode, setClusterConfigMode] = useState<ConfigMode>('default');
  const [customImportSummary, setCustomImportSummary] = useState('');
  const [deploymentMode, setDeploymentMode] = useState<DeploymentMode>('kraft');
  const [installDir, setInstallDir] = useState('/opt');
  const [dataDir, setDataDir] = useState('/data/kafka');
  const [logDir, setLogDir] = useState('/var/log/kafka');
  const [artifactLoadDir, setArtifactLoadDir] = useState('/srv/tantor-agent/artifacts');
  const [listenerPort, setListenerPort] = useState(9092);
  const [controllerPort, setControllerPort] = useState(9093);
  const [zookeeperPeerPort, setZookeeperPeerPort] = useState(2888);
  const [zookeeperElectionPort, setZookeeperElectionPort] = useState(3888);
  const [hostPorts, setHostPorts] = useState<Record<string, HostPorts>>({});
  const [portCheckResults, setPortCheckResults] = useState<Record<string, PrereqResult>>({});
  const [hoveredPortCheckHostId, setHoveredPortCheckHostId] = useState<string | null>(null);
  const [numPartitions, setNumPartitions] = useState(1);
  const [connectArtifacts, setConnectArtifacts] = useState<SchemaArtifact[]>([]);

  const [nodeSearch, setNodeSearch] = useState('');
  const [nodeDropdownOpen, setNodeDropdownOpen] = useState(false);

  const [selectedNodeIds, setSelectedNodeIds] = useState<string[]>([]);
  const [rolesByHost, setRolesByHost] = useState<Record<string, RoleChoice>>({});
  const [addonRolesByHost, setAddonRolesByHost] = useState<Record<string, AddonRole[]>>({});
  const [schemaConfigsByHost, setSchemaConfigsByHost] = useState<Record<string, SchemaRegistryConfig>>({});
  const [connectConfigsByHost, setConnectConfigsByHost] = useState<Record<string, KafkaConnectConfig>>({});
  const [schemaChecksByHost, setSchemaChecksByHost] = useState<Record<string, PrereqResult>>({});
  const [connectChecksByHost, setConnectChecksByHost] = useState<Record<string, PrereqResult>>({});
  const addonCheckRevision = useRef(0);
  const addonCheckInputs = JSON.stringify({ selectedNodeIds, rolesByHost, addonRolesByHost,
    schemaConfigsByHost, connectConfigsByHost, hostPorts, listenerPort, controllerPort,
    kafkaVersion, installDir, dataDir, logDir, deploymentMode });
  useEffect(() => {
    addonCheckRevision.current += 1;
    setSchemaChecksByHost({});
    setConnectChecksByHost({});
  }, [addonCheckInputs]);
  const [configsByService, setConfigsByService] = useState<Record<string, NodeConfigState>>({});
  const [commonConfigs, setCommonConfigs] = useState<Record<ConfigKind, PropertyRow[]>>(() => createCommonConfigs('kraft'));
  const [commonConfigKind, setCommonConfigKind] = useState<ConfigKind>('server');
  const [configModalHostId, setConfigModalHostId] = useState<string | null>(null);
  const [commonConfigOpen, setCommonConfigOpen] = useState(false);
  const [prereqResults, setPrereqResults] = useState<Record<string, PrereqResult>>({});
  const [checkingPrereqs, setCheckingPrereqs] = useState(false);
  const [deploying, setDeploying] = useState(false);
  const [validatingKraft, setValidatingKraft] = useState(false);
  const [kraftValidation, setKraftValidation] = useState<KraftValidationReport | null>(null);
  const [kraftGeneratedConfig, setKraftGeneratedConfig] = useState<Record<string, string>>({});

  const dropdownRef = useRef<HTMLDivElement>(null);

  const [kraftRiskAcknowledged, setKraftRiskAcknowledged] = useState(false);
  const [showEnrollModal, setShowEnrollModal] = useState(false);
  const [openRoleMenuHostId, setOpenRoleMenuHostId] = useState<string | null>(null);
  const [roleMenuAnchor, setRoleMenuAnchor] = useState<HTMLElement | null>(null);

  useEffect(() => {
    loadHosts();
    loadVersions();
    if (SCHEMA_REGISTRY_DEPLOYMENT_ENABLED) loadSchemaArtifacts();
    if (KAFKA_CONNECT_DEPLOYMENT_ENABLED) loadConnectArtifacts();
  }, []);

  useEffect(() => {
    if (!addClusterId) return;
    setStage('details');
    setLoadingCluster(true);
    fetch(`/api/v1/ui/clusters/${addClusterId}`)
      .then(res => {
        if (!res.ok) throw new Error('Cluster not found');
        return res.json();
      })
      .then((cluster: ExistingCluster) => {
        setExistingCluster(cluster);
        setClusterName(cluster.name || '');
        setKafkaVersion(cluster.kafkaVersion || '');
        setEnvironment(cluster.environment || '');
        const loadedMode: DeploymentMode = cluster.mode === 'zookeeper' ? 'zookeeper' : 'kraft';
        setDeploymentMode(loadedMode);
        const cfg = cluster.config || {};
        setClusterConfigMode(String(cfg.configuration_mode || 'default') === 'custom' ? 'custom' : 'default');
        setInstallDir('');
        setDataDir('');
        setLogDir('');
        setArtifactLoadDir('');
        setListenerPort(Number(cfg.listener_port || 9092));
        setControllerPort(Number(cfg.controller_port || 9093));
        setZookeeperPeerPort(Number(cfg.zookeeper_peer_port || 2888));
        setZookeeperElectionPort(Number(cfg.zookeeper_election_port || 3888));
        setNumPartitions(Number(cfg.num_partitions || 1));
        const loadedConfigs = createCommonConfigs(loadedMode);
        setCommonConfigs({
          server: syncCommonRows(loadedConfigs.server, cfg),
          broker: syncCommonRows(loadedConfigs.broker, cfg),
          controller: syncCommonRows(loadedConfigs.controller, cfg),
          zookeeper: loadedConfigs.zookeeper,
        });
      })
      .catch(error => {
        console.error(error);
        notifyAction('Failed to load cluster details for add-node mode.');
        navigate('/clusters');
      })
      .finally(() => setLoadingCluster(false));
  }, [addClusterId, navigate]);

  const loadHosts = async () => {
    setLoadingHosts(true);
    try {
      const res = await fetch('/api/v1/ui/hosts');
      if (res.ok) {
        const inventory: Host[] = await res.json();
        setHosts(inventory.filter(host => host.deployable !== false && host.agentType !== 'KAFKA_DISCOVERY'));
      }
    } catch (e) {
      console.error(e);
      setHosts([]);
    } finally {
      setLoadingHosts(false);
    }
  };

  const loadVersions = async () => {
    setLoadingVersions(true);
    try {
      const res = await fetch('/api/v1/artifacts?serviceType=KAFKA');
      const data = await res.json();
      const mapped = (data.content || []).map((a: KafkaVersionRaw) => ({
        version: a.version,
        available: a.status === 'AVAILABLE',
        scala_version: a.attributes?.scala_version || '2.13',
        release_date: a.attributes?.release_date || (a.createdAt ? new Date(a.createdAt).toLocaleDateString() : ''),
        size_mb: parseFloat(((a.fileSizeBytes ?? 0) / 1024 / 1024).toFixed(1)),
        filename: a.fileName ?? a.filename ?? '',
        id: a.id,
      }));
      setVersions(mapped);
      const firstAvailable = mapped.find((v: KafkaVersionInfo) => v.available) || mapped[0];
      if (firstAvailable) setKafkaVersion(current => current || firstAvailable.version);
    } catch (e) {
      console.error(e);
      setVersions([]);
    } finally {
      setLoadingVersions(false);
    }
  };

  const loadSchemaArtifacts = async () => {
    try {
      const res = await fetch('/api/v1/artifacts?status=AVAILABLE&size=100');
      if (!res.ok) return;
      const data = await res.json();
      const available = (data.content || []).filter((artifact: SchemaArtifact) =>
        artifact.status === 'AVAILABLE' && (artifact.serviceType === 'SCHEMA_REGISTRY' || isUnifiedKafkaArtifact(artifact)));
      setSchemaArtifacts(available);
    } catch (error) {
      console.error(error);
      setSchemaArtifacts([]);
    }
  };

  const loadConnectArtifacts = async () => {
    try {
      const res = await fetch('/api/v1/artifacts?status=AVAILABLE&size=100');
      if (!res.ok) return;
      const data = await res.json();
      setConnectArtifacts((data.content || []).filter((artifact: SchemaArtifact) =>
        artifact.status === 'AVAILABLE' && (artifact.serviceType === 'KAFKA_CONNECT' || isUnifiedKafkaArtifact(artifact))));
    } catch (error) {
      console.error(error);
      setConnectArtifacts([]);
    }
  };

  const availableVersions = versions.filter(version => version.available);
  const zookeeperSupported = kafkaMajorVersion(kafkaVersion) > 0 && kafkaMajorVersion(kafkaVersion) < 4;
  const commonConfigKinds = commonConfigKindsForMode(deploymentMode);
  const selectedHosts = selectedNodeIds
    .map(id => hosts.find(host => host.id === id))
    .filter(Boolean) as Host[];

  const filteredHosts = hosts.filter(host => {
    const needle = `${host.hostname} ${displayIp(host)} ${host.id}`.toLowerCase();
    return needle.includes(nodeSearch.toLowerCase());
  });

  const selectableFilteredHosts = filteredHosts.filter(host => host.status === 'AVAILABLE' && host.available !== false);
  const allFilteredSelected = selectableFilteredHosts.length > 0
    && selectableFilteredHosts.every(host => selectedNodeIds.includes(host.id));

  const roleOptions = isAddNodeMode
    ? (deploymentMode === 'zookeeper' ? ZOOKEEPER_ROLE_OPTIONS : KRAFT_ROLE_OPTIONS).filter(role => role.id === 'broker')
    : deploymentMode === 'zookeeper' ? ZOOKEEPER_ROLE_OPTIONS : KRAFT_ROLE_OPTIONS;
  const allRoleOptions = [...KRAFT_ROLE_OPTIONS, ...ZOOKEEPER_ROLE_OPTIONS];
  const defaultRoleForMode: RoleChoice = isAddNodeMode ? 'broker' : deploymentMode === 'zookeeper' ? 'broker_zookeeper' : 'broker_controller';
  const hostHasAddonRole = (hostId: string, role: AddonRole) => (addonRolesByHost[hostId] || []).includes(role);
  const schemaHosts = selectedHosts.filter(host => hostHasAddonRole(host.id, 'schema_registry'));
  const connectHosts = selectedHosts.filter(host => hostHasAddonRole(host.id, 'kafka_connect'));
  const schemaConfigFor = (hostId: string) => schemaConfigsByHost[hostId] || defaultSchemaRegistryConfig();
  const connectConfigFor = (hostId: string) => connectConfigsByHost[hostId] || defaultKafkaConnectConfig();

  const brokerCount = selectedHosts.filter(host => {
    const role = rolesByHost[host.id] || defaultRoleForMode;
    return role === 'broker_controller' || role === 'broker' || role === 'separate' || role === 'broker_zookeeper';
  }).length;

  const controllerCount = selectedHosts.filter(host => {
    const role = rolesByHost[host.id] || defaultRoleForMode;
    return role === 'broker_controller' || role === 'controller' || role === 'separate';
  }).length;

  const zookeeperCount = selectedHosts.filter(host => {
    const role = rolesByHost[host.id] || defaultRoleForMode;
    return role === 'broker_zookeeper' || role === 'zookeeper';
  }).length;

  const existingBrokerCount = isAddNodeMode
    ? (existingCluster?.hosts || []).filter(host => ['broker', 'broker_controller', 'broker_zookeeper'].includes(String(host.role || ''))).length
    : 0;
  const effectiveBrokerCount = brokerCount + existingBrokerCount;

  const replication = useMemo(() => {
    if (brokerCount <= 1) return { factor: 1, minIsr: 1 };
    if (brokerCount === 2) return { factor: 2, minIsr: 1 };
    return { factor: 3, minIsr: 2 };
  }, [brokerCount]);

  useEffect(() => {
    if (isAddNodeMode) return;
    setCommonConfigs(prev => {
      const syncDefaults = (rows: PropertyRow[]) => rows.map(row => {
        if (row.key === 'default.replication.factor' && (clusterConfigMode === 'default' || !row.value.trim())) {
          return { ...row, value: String(replication.factor) };
        }
        if (row.key === 'min.insync.replicas' && (clusterConfigMode === 'default' || !row.value.trim())) {
          return { ...row, value: String(replication.minIsr) };
        }
        if (row.key === 'offsets.topic.replication.factor' && (clusterConfigMode === 'default' || (row.value === '3' && replication.factor < 3))) {
          return { ...row, value: String(replication.factor) };
        }
        if (row.key === 'transaction.state.log.replication.factor' && (clusterConfigMode === 'default' || (row.value === '3' && replication.factor < 3))) {
          return { ...row, value: String(replication.factor) };
        }
        if (row.key === 'transaction.state.log.min.isr' && (clusterConfigMode === 'default' || (row.value === '2' && replication.minIsr < 2))) {
          return { ...row, value: String(replication.minIsr) };
        }
        return row;
      });
      return {
        ...prev,
        server: syncDefaults(prev.server),
        broker: syncDefaults(prev.broker),
      };
    });
  }, [clusterConfigMode, deploymentMode, isAddNodeMode, replication.factor, replication.minIsr]);

  const warnings = useMemo(() => {
    const items: string[] = [];
    if (effectiveBrokerCount === 1) items.push('Only one broker will be present. Kafka will run without data replication.');
    if (deploymentMode === 'zookeeper' && zookeeperCount === 1) items.push('Only one ZooKeeper selected. ZooKeeper failover will not be available.');
    if (deploymentMode === 'zookeeper' && zookeeperCount > 1 && zookeeperCount % 2 === 0) items.push('Even ZooKeeper count selected. Odd ZooKeeper count is recommended for quorum voting.');
    if (isAddNodeMode && selectedHosts.some(host => {
      const role = rolesByHost[host.id] || defaultRoleForMode;
      return role === 'controller' || role === 'broker_controller' || role === 'separate' || role === 'broker_zookeeper' || role === 'zookeeper';
    })) {
      items.push('Adding quorum nodes changes cluster membership. Existing nodes may need updated configs and restart sequencing.');
    }
    return items;
  }, [defaultRoleForMode, deploymentMode, effectiveBrokerCount, isAddNodeMode, rolesByHost, selectedHosts, zookeeperCount]);

  const pathErrors = [
    validatePath(installDir, 'Install directory'),
    validatePath(dataDir, 'Data directory'),
    validatePath(logDir, 'Log directory'),
    validatePath(artifactLoadDir, 'Artifacts/Load Directory'),
    ...schemaHosts.flatMap(host => {
      const cfg = schemaConfigFor(host.id);
      return [validatePath(cfg.installDir, `${host.hostname}: Schema Registry install directory`), validatePath(cfg.configDir, `${host.hostname}: Schema Registry config directory`), validatePath(cfg.logDir, `${host.hostname}: Schema Registry log directory`), validatePath(cfg.workingDir, `${host.hostname}: Schema Registry working directory`)];
    }),
    ...connectHosts.flatMap(host => {
      const cfg = connectConfigFor(host.id);
      return [validatePath(cfg.installDir, `${host.hostname}: Kafka Connect install directory`), validatePath(cfg.configDir, `${host.hostname}: Kafka Connect config directory`), validatePath(cfg.logDir, `${host.hostname}: Kafka Connect log directory`), validatePath(cfg.workingDir, `${host.hostname}: Kafka Connect working directory`), validatePath(cfg.pluginDir, `${host.hostname}: Kafka Connect plugin directory`)];
    }),
  ].filter(Boolean);

  const configModalHost = configModalHostId
    ? selectedHosts.find(host => host.id === configModalHostId) || null
    : null;

  const prerequisiteComplete = selectedHosts.length > 0
    && selectedHosts.every(host => prereqResults[host.id]?.status === 'SUCCESS');
  const schemaPrecheckComplete = schemaHosts.every(host => schemaChecksByHost[host.id]?.status === 'SUCCESS');
  const connectPrecheckComplete = connectHosts.every(host => connectChecksByHost[host.id]?.status === 'SUCCESS');
  const kraftDeploymentBlocked = deploymentMode === 'kraft'
    && !isAddNodeMode
    && (!kraftValidation
      || kraftValidation.errors.length > 0
      || (kraftValidation.acknowledgementRequired && !kraftRiskAcknowledged));

  const configKey = (hostId: string, kind: ConfigKind) => `${hostId}:${kind}`;

  const ipRowKeyForKind = (kind: ConfigKind) => {
    if (kind === 'broker') return 'advertised.host';
    if (kind === 'controller') return 'controller.host';
    if (kind === 'zookeeper') return 'zookeeper.host';
    return 'node.host';
  };

  const defaultRowsForKind = (kind: ConfigKind): PropertyRow[] => {
    return [
      { key: ipRowKeyForKind(kind), value: '', required: true, locked: true },
    ];
  };

  const defaultHeapForKind = (kind: ConfigKind) => {
    if (kind === 'controller' || kind === 'zookeeper') return '512M';
    return '1G';
  };

  const configFileName = (kind: ConfigKind) => {
    if (kind === 'server') return 'server.properties';
    if (kind === 'broker') return 'broker.properties';
    if (kind === 'zookeeper') return 'zookeeper.properties';
    return 'controller.properties';
  };

  const configKindsForRole = (role: RoleChoice): ConfigKind[] => {
    if (deploymentMode === 'zookeeper') {
      if (role === 'broker_zookeeper') return ['server', 'zookeeper'];
      if (role === 'broker') return ['server'];
      return ['zookeeper'];
    }
    if (role === 'broker_controller') return ['server'];
    if (role === 'broker') return ['broker'];
    if (role === 'controller') return ['controller'];
    if (role === 'separate') return ['broker', 'controller'];
    return ['zookeeper'];
  };

  const serviceConfigFor = (hostId: string, kind: ConfigKind): NodeConfigState => {
    const existing = configsByService[configKey(hostId, kind)];
    return existing || { mode: 'default', rows: defaultRowsForKind(kind), heapSize: defaultHeapForKind(kind) };
  };

  const updateServiceConfig = (hostId: string, kind: ConfigKind, patch: Partial<NodeConfigState>) => {
    setConfigsByService(prev => {
      const current = prev[configKey(hostId, kind)] || { mode: 'default', rows: defaultRowsForKind(kind), heapSize: defaultHeapForKind(kind) };
      return {
        ...prev,
        [configKey(hostId, kind)]: { ...current, ...patch },
      };
    });
  };

  const updatePropertyValue = (hostId: string, kind: ConfigKind, key: string, value: string) => {
    const cfg = serviceConfigFor(hostId, kind);
    updateServiceConfig(hostId, kind, {
      mode: 'custom',
      rows: cfg.rows.map(row => row.key === key ? { ...row, value } : row),
    });
  };

  const commonConfigValue = (key: string) => {
    const row = commonConfigKinds
      .flatMap(kind => commonConfigs[kind])
      .find(item => item.key === key);
    return row?.value.trim() || '';
  };

  const updateCommonConfigValue = (kind: ConfigKind, key: string, value: string) => {
    setCommonConfigs(prev => {
      if (!SYNCED_BROKER_PROPERTY_KEYS.has(key)) {
        return { ...prev, [kind]: setRowsValue(prev[kind], key, value) };
      }
      return {
        ...prev,
        server: setRowsValue(prev.server, key, value),
        broker: setRowsValue(prev.broker, key, value),
      };
    });
    if (key === 'num.partitions') {
      const numeric = Number.parseInt(value || '0', 10);
      setNumPartitions(Number.isFinite(numeric) ? numeric : 0);
    }
    setClusterConfigMode('custom');
  };

  const selectClusterConfigMode = (mode: ConfigMode) => {
    setClusterConfigMode(mode);
    if (mode === 'default') {
      setCommonConfigs(createCommonConfigs(deploymentMode));
      setCommonConfigKind('server');
    }
  };

  const parseCsvLine = (line: string) => {
    const values: string[] = [];
    let value = '';
    let quoted = false;
    for (let index = 0; index < line.length; index++) {
      const character = line[index];
      if (character === '"' && line[index + 1] === '"' && quoted) { value += '"'; index++; }
      else if (character === '"') quoted = !quoted;
      else if (character === ',' && !quoted) { values.push(value.trim()); value = ''; }
      else value += character;
    }
    values.push(value.trim());
    return values;
  };

  const importCustomCsv = async (file: File) => {
    if (!file.name.toLowerCase().endsWith('.csv')) {
      setCustomImportSummary('Only the documented CSV template is supported.');
      return;
    }
    const rows = (await file.text()).split(/\r?\n/).filter(line => line.trim()).map(parseCsvLine);
    const header = rows.shift()?.map(value => value.toLowerCase()) || [];
    const scopeIndex = header.indexOf('scope');
    const keyIndex = header.indexOf('key');
    const valueIndex = header.indexOf('value');
    if (scopeIndex < 0 || keyIndex < 0 || valueIndex < 0) {
      setCustomImportSummary('CSV must contain scope,key,value columns.');
      return;
    }
    const imported: Partial<Record<ConfigKind, PropertyRow[]>> = {};
    let propertyCount = 0;
    rows.forEach(row => {
      const scope = String(row[scopeIndex] || '').toLowerCase();
      const key = String(row[keyIndex] || '').trim();
      const value = String(row[valueIndex] || '').trim();
      if (scope === 'cluster') {
        if (key === 'cluster_name') setClusterName(value);
        else if (key === 'environment') setEnvironment(value.toUpperCase());
        else if (key === 'install_directory') setInstallDir(value);
        else if (key === 'data_directory') setDataDir(value);
        else if (key === 'log_directory') setLogDir(value);
        else if (key === 'artifact_directory') setArtifactLoadDir(value);
        return;
      }
      if (!['server', 'broker', 'controller'].includes(scope) || !key) return;
      const kind = scope as ConfigKind;
      imported[kind] = [...(imported[kind] || []), { key, value }];
      propertyCount++;
    });
    setCommonConfigs(current => {
      const next = { ...current };
      (['server', 'broker', 'controller'] as ConfigKind[]).forEach(kind => {
        const additions = imported[kind] || [];
        if (!additions.length) return;
        const keys = new Set(additions.map(row => row.key));
        next[kind] = [...current[kind].filter(row => !keys.has(row.key)), ...additions];
      });
      return next;
    });
    setClusterConfigMode('custom');
    setCustomImportSummary(`Imported ${propertyCount} properties and cluster-level deployment details from ${file.name}.`);
  };

  const downloadCustomTemplate = () => {
    const csv = [
      'scope,key,value',
      'cluster,cluster_name,production-kafka',
      'cluster,environment,DEV',
      'cluster,install_directory,/opt',
      'cluster,data_directory,/data/kafka',
      'cluster,log_directory,/var/log/kafka',
      'server,num.partitions,3',
      'broker,default.replication.factor,3',
      'controller,controller.listener.names,CONTROLLER',
    ].join('\n');
    const link = document.createElement('a');
    link.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    link.download = 'tantor-cluster-config-template.csv';
    link.click();
    URL.revokeObjectURL(link.href);
  };

  const configuredReplicationFactor = Number.parseInt(commonConfigValue('default.replication.factor') || String(replication.factor), 10);
  const configuredMinIsr = Number.parseInt(commonConfigValue('min.insync.replicas') || String(replication.minIsr), 10);
  const replicationWarnings = configuredReplicationFactor > 1 && configuredMinIsr === configuredReplicationFactor
    ? [`Minimum ISR equals replication factor (${configuredReplicationFactor}). Writes will stop if any replica becomes unavailable.`]
    : [];

  const missingRequiredConfigs = selectedHosts.flatMap(host => {
    const role = rolesByHost[host.id] || defaultRoleForMode;
    return configKindsForRole(role).flatMap(kind => {
      const cfg = serviceConfigFor(host.id, kind);
      return cfg.rows
        .filter(row => row.required && !row.value.trim())
        .map(row => `${host.hostname}: ${configFileName(kind)} requires ${row.key}`);
    });
  });

  const configValidationErrors = [
    ...commonConfigKinds.flatMap(kind => commonConfigs[kind]
      .filter(row => row.required && !String(row.value).trim())
      .map(row => `${configFileName(kind)} requires ${row.key}.`)),
    commonConfigKinds.flatMap(kind => commonConfigs[kind]).some(row => row.required && String(row.value).trim() && (!/^\d+$/.test(String(row.value).trim()) || Number(row.value) < 1))
      ? 'Common numeric properties must be positive numbers.'
      : '',
    deploymentMode === 'zookeeper' && kafkaMajorVersion(kafkaVersion) >= 4 ? `Kafka ${kafkaVersion} cannot use ZooKeeper mode. Select a Kafka 3.x artifact or KRaft.` : '',
    commonConfigValue('default.replication.factor') && Number(commonConfigValue('default.replication.factor')) > effectiveBrokerCount ? `default.replication.factor=${commonConfigValue('default.replication.factor')} is invalid because the cluster will have ${effectiveBrokerCount} broker${effectiveBrokerCount === 1 ? '' : 's'}.` : '',
    commonConfigValue('min.insync.replicas') && commonConfigValue('default.replication.factor') && Number(commonConfigValue('min.insync.replicas')) > Number(commonConfigValue('default.replication.factor')) ? 'min.insync.replicas cannot be greater than default.replication.factor.' : '',
    commonConfigValue('offsets.topic.replication.factor') && Number(commonConfigValue('offsets.topic.replication.factor')) > effectiveBrokerCount ? `offsets.topic.replication.factor=${commonConfigValue('offsets.topic.replication.factor')} is invalid because the cluster will have ${effectiveBrokerCount} brokers.` : '',
    commonConfigValue('transaction.state.log.replication.factor') && Number(commonConfigValue('transaction.state.log.replication.factor')) > effectiveBrokerCount ? `transaction.state.log.replication.factor=${commonConfigValue('transaction.state.log.replication.factor')} is invalid because the cluster will have ${effectiveBrokerCount} brokers.` : '',
  ].filter(Boolean);

  const addonValidationErrors = [
    ...schemaHosts.flatMap(host => /^[1-9]\d*[mMgG]$/.test(schemaConfigFor(host.id).heapSize)
      ? [] : [`${host.hostname}: Schema Registry heap size must be a positive size such as 512M or 1G.`]),
    ...connectHosts.flatMap(host => {
      const cfg = connectConfigFor(host.id);
      return [
        /^[1-9]\d*[mMgG]$/.test(cfg.heapSize) ? '' : `${host.hostname}: Kafka Connect heap size must be a positive size such as 512M or 1G.`,
        /^[A-Za-z0-9._-]{1,200}$/.test(cfg.groupId) ? '' : `${host.hostname}: Kafka Connect worker group ID must use letters, numbers, dots, underscores or hyphens.`,
      ].filter(Boolean);
    }),
  ];
  const configBlockingIssues = [...missingRequiredConfigs, ...configValidationErrors, ...addonValidationErrors];

  const selectedPortValidationErrors = selectedHosts.flatMap(host => {
    const role = rolesByHost[host.id] || defaultRoleForMode;
    const ports = hostPorts[host.id] || {
      listenerPort,
      controllerPort,
      brokerJmxPort: DEFAULT_BROKER_JMX_PORT,
      controllerJmxPort: DEFAULT_CONTROLLER_JMX_PORT,
      zookeeperPeerPort,
      zookeeperElectionPort,
    };
    const selected: Array<{ label: string; value: number }> = [];
    if (['broker', 'broker_controller', 'separate', 'broker_zookeeper'].includes(role)) {
      selected.push({ label: 'Broker port', value: ports.listenerPort });
      selected.push({ label: 'Broker JMX port', value: ports.brokerJmxPort });
    }
    if (deploymentMode === 'kraft' && ['controller', 'broker_controller', 'separate'].includes(role)) {
      selected.push({ label: 'Controller port', value: ports.controllerPort });
      if (role === 'controller' || role === 'separate') {
        selected.push({ label: 'Controller JMX port', value: ports.controllerJmxPort });
      }
    }
    if (deploymentMode === 'zookeeper' && ['zookeeper', 'broker_zookeeper'].includes(role)) {
      selected.push(
        { label: 'ZooKeeper client port', value: ports.controllerPort },
        { label: 'ZooKeeper peer port', value: ports.zookeeperPeerPort },
        { label: 'ZooKeeper election port', value: ports.zookeeperElectionPort },
      );
    }

    if (hostHasAddonRole(host.id, 'schema_registry')) selected.push({ label: 'Schema Registry REST port', value: schemaConfigFor(host.id).port });
    if (hostHasAddonRole(host.id, 'kafka_connect')) selected.push({ label: 'Kafka Connect REST port', value: connectConfigFor(host.id).port });

    const invalid = selected
      .filter(port => !Number.isInteger(port.value) || port.value < 1 || port.value > 65535)
      .map(port => `${host.hostname}: ${port.label} must be between 1 and 65535.`);
    const duplicates = selected
      .filter((port, index) => selected.findIndex(candidate => candidate.value === port.value) !== index)
      .map(port => `${host.hostname}: port ${port.value} is assigned more than once.`);
    return [...invalid, ...new Set(duplicates)];
  });

  const canPreview = clusterName.trim()
    && kafkaVersion
    && selectedHosts.length > 0
    && brokerCount > 0
    && (isAddNodeMode || (deploymentMode === 'kraft' ? controllerCount > 0 : zookeeperCount > 0))
    && selectedPortValidationErrors.length === 0
    && schemaHosts.every(host => {
      const cfg = schemaConfigFor(host.id);
      return Boolean(cfg.artifactId) && Number.isInteger(cfg.port) && cfg.port > 0 && cfg.port <= 65535;
    })
    && connectHosts.every(host => {
      const cfg = connectConfigFor(host.id);
      return Boolean(cfg.artifactId) && Number.isInteger(cfg.port) && cfg.port > 0 && cfg.port <= 65535;
    });

  const serviceTemplate = (kind: ConfigKind, cfg: NodeConfigState) => {
    const commonRows = commonConfigs[kind] || [];
    return serializeProperties([...commonRows, ...cfg.rows]);
  };

  const buildServices = (): ServiceAssignment[] => {
    const usedNodeIds = new Set((existingCluster?.hosts || [])
      .map(host => Number(host.nodeId || 0))
      .filter(id => id > 0));
    const allocateNodeId = (start: number) => {
      let next = start;
      while (usedNodeIds.has(next)) next++;
      usedNodeIds.add(next);
      return next;
    };
    const services: ServiceAssignment[] = [];

    const getHp = (hostId: string): HostPorts => hostPorts[hostId] || {
      listenerPort,
      controllerPort,
      brokerJmxPort: DEFAULT_BROKER_JMX_PORT,
      controllerJmxPort: DEFAULT_CONTROLLER_JMX_PORT,
      zookeeperPeerPort,
      zookeeperElectionPort,
    };

    selectedHosts.forEach(host => {
      const role = rolesByHost[host.id] || defaultRoleForMode;
      const hp = getHp(host.id);
      const configFor = (kind: ConfigKind) => serviceConfigFor(host.id, kind);
      if (role === 'broker_controller') {
        const cfg = configFor('server');
        services.push({ host_id: host.id, role: 'broker_controller', node_id: allocateNodeId(1), configuration_mode: cfg.mode, properties_template: serviceTemplate('server', cfg), heap_size: cfg.heapSize, listener_port: hp.listenerPort, controller_port: hp.controllerPort, jmx_port: hp.brokerJmxPort });
      } else if (role === 'broker_zookeeper') {
        const brokerCfg = configFor('server');
        const zookeeperCfg = configFor('zookeeper');
        services.push({ host_id: host.id, role: 'broker', node_id: allocateNodeId(1), configuration_mode: brokerCfg.mode, properties_template: serviceTemplate('server', brokerCfg), heap_size: brokerCfg.heapSize, listener_port: hp.listenerPort, jmx_port: hp.brokerJmxPort });
        services.push({ host_id: host.id, role: 'zookeeper', node_id: allocateNodeId(1001), configuration_mode: zookeeperCfg.mode, properties_template: serviceTemplate('zookeeper', zookeeperCfg), heap_size: zookeeperCfg.heapSize, controller_port: hp.controllerPort, zookeeper_peer_port: hp.zookeeperPeerPort, zookeeper_election_port: hp.zookeeperElectionPort });
      } else if (role === 'separate') {
        const brokerCfg = configFor('broker');
        const controllerCfg = configFor('controller');
        services.push({ host_id: host.id, role: 'broker', node_id: allocateNodeId(1), configuration_mode: brokerCfg.mode, properties_template: serviceTemplate('broker', brokerCfg), heap_size: brokerCfg.heapSize, listener_port: hp.listenerPort, jmx_port: hp.brokerJmxPort });
        services.push({ host_id: host.id, role: 'controller', node_id: allocateNodeId(101), configuration_mode: controllerCfg.mode, properties_template: serviceTemplate('controller', controllerCfg), heap_size: controllerCfg.heapSize, controller_port: hp.controllerPort, jmx_port: hp.controllerJmxPort });
      } else if (role === 'controller') {
        const cfg = configFor('controller');
        services.push({ host_id: host.id, role: 'controller', node_id: allocateNodeId(101), configuration_mode: cfg.mode, properties_template: serviceTemplate('controller', cfg), heap_size: cfg.heapSize, controller_port: hp.controllerPort, jmx_port: hp.controllerJmxPort });
      } else if (role === 'zookeeper') {
        const cfg = configFor('zookeeper');
        services.push({ host_id: host.id, role: 'zookeeper', node_id: allocateNodeId(1001), configuration_mode: cfg.mode, properties_template: serviceTemplate('zookeeper', cfg), heap_size: cfg.heapSize, controller_port: hp.controllerPort, zookeeper_peer_port: hp.zookeeperPeerPort, zookeeper_election_port: hp.zookeeperElectionPort });
      } else {
        const kind: ConfigKind = deploymentMode === 'zookeeper' ? 'server' : 'broker';
        const cfg = configFor(kind);
        services.push({ host_id: host.id, role: 'broker', node_id: allocateNodeId(1), configuration_mode: cfg.mode, properties_template: serviceTemplate(kind, cfg), heap_size: cfg.heapSize, listener_port: hp.listenerPort, jmx_port: hp.brokerJmxPort });
      }
    });

    return services;
  };

  const buildDeploymentPayload = (includeGeneratedKraftConfig = true) => {
    const selectedArtifact = versions.find(version => version.version === kafkaVersion);
    const schemaHost = schemaHosts[0];
    const connectHost = connectHosts[0];
    const schemaConfig = schemaHost ? schemaConfigFor(schemaHost.id) : null;
    const connectConfig = connectHost ? connectConfigFor(connectHost.id) : null;
    const selectedSchemaArtifact = schemaConfig ? schemaArtifacts.find(artifact => artifact.id === schemaConfig.artifactId) : undefined;
    const selectedConnectArtifact = connectConfig ? connectArtifacts.find(artifact => artifact.id === connectConfig.artifactId) : undefined;
    const artifactRepoBaseUrl = runtimeConfig.artifactApiBasePath;
    return {
      name: clusterName.trim(),
      kafka_version: kafkaVersion,
      mode: deploymentMode,
      services: buildServices(),
      environment: environment.trim(),
      acknowledge_kraft_risk: kraftRiskAcknowledged,
      artifactUrl: selectedArtifact ? `${artifactRepoBaseUrl}/${selectedArtifact.id}/download` : '',
      addons: !isAddNodeMode && (schemaHost || connectHost) ? {
        schema_registry: schemaHost && schemaConfig ? {
          enabled: true,
          host_id: schemaHost.id, artifact_id: schemaConfig.artifactId,
          artifact_url: `${artifactRepoBaseUrl}/${schemaConfig.artifactId}/download`,
          checksum: selectedSchemaArtifact?.sha256 || selectedSchemaArtifact?.checksum || '',
          version: selectedSchemaArtifact?.version || '',
          port: schemaConfig.port, install_dir: schemaConfig.installDir.trim(), config_dir: schemaConfig.configDir.trim(),
          log_dir: schemaConfig.logDir.trim(), working_dir: schemaConfig.workingDir.trim(), heap_size: schemaConfig.heapSize, compatibility_level: schemaConfig.compatibility,
        } : undefined,
        kafka_connect: connectHost && connectConfig ? {
          enabled: true, host_id: connectHost.id, artifact_id: connectConfig.artifactId,
          artifact_url: `${artifactRepoBaseUrl}/${connectConfig.artifactId}/download`, checksum: selectedConnectArtifact?.sha256 || selectedConnectArtifact?.checksum || '', version: selectedConnectArtifact?.version || '',
          port: connectConfig.port, install_dir: connectConfig.installDir.trim(), config_dir: connectConfig.configDir.trim(), log_dir: connectConfig.logDir.trim(), working_dir: connectConfig.workingDir.trim(), plugin_dir: connectConfig.pluginDir.trim(), heap_size: connectConfig.heapSize, group_id: connectConfig.groupId,
        } : undefined,
      } : undefined,
      config: {
        configuration_mode: clusterConfigMode,
        kafka_install_dir: installDir.trim(),
        kafka_install_base_dir: installDir.trim(),
        kafka_data_dir: dataDir.trim(),
        kafka_app_log_dir: logDir.trim(),
        artifact_load_dir: artifactLoadDir.trim(),
        scala_version: selectedArtifact?.scala_version || '2.13',
        listener_port: listenerPort,
        controller_port: controllerPort,
        broker_jmx_port: DEFAULT_BROKER_JMX_PORT,
        controller_jmx_port: DEFAULT_CONTROLLER_JMX_PORT,
        zookeeper_port: deploymentMode === 'zookeeper' ? controllerPort : undefined,
        zookeeper_peer_port: zookeeperPeerPort,
        zookeeper_election_port: zookeeperElectionPort,
        num_partitions: Number(commonConfigValue('num.partitions') || numPartitions),
        replication_factor: configuredReplicationFactor,
        min_insync_replicas: configuredMinIsr,
        ...(includeGeneratedKraftConfig && deploymentMode === 'kraft' ? kraftGeneratedConfig : {}),
      },
    };
  };

  const openPreview = async () => {
    setSchemaChecksByHost({});
    setConnectChecksByHost({});
    setPrereqResults({});
    setKraftRiskAcknowledged(false);
    if (deploymentMode !== 'kraft' || isAddNodeMode) {
      setKraftValidation(null);
      setKraftGeneratedConfig({});
      setStage('preview');
      return;
    }

    setValidatingKraft(true);
    try {
      const res = await fetch('/api/v1/ui/clusters/validate-kraft', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildDeploymentPayload(false)),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        notifyAction(body.error || body.message || 'KRaft topology validation failed.');
        return;
      }
      const report = body as KraftValidationReport;
      setKraftValidation(report);
      setKraftGeneratedConfig(report.generatedConfig || {});
      setStage('preview');
    } catch (error) {
      console.error(error);
      notifyAction('Network error while validating the KRaft topology.');
    } finally {
      setValidatingKraft(false);
    }
  };


  const toggleNodeSelection = (hostId: string) => {
    setSelectedNodeIds(prev => {
      const isSelected = prev.includes(hostId);
      let nextSelected;
      if (isSelected) {
        nextSelected = prev.filter(id => id !== hostId);
      } else {
        nextSelected = [...prev, hostId];
      }

      setRolesByHost(rolesPrev => {
        const nextRoles = { ...rolesPrev };
        if (!isSelected) {
          nextRoles[hostId] = defaultRoleForMode;
        } else {
          delete nextRoles[hostId];
        }
        return nextRoles;
      });

      return nextSelected;
    });
    setPrereqResults({});
  };

  const toggleSelectAllNodes = () => {
    const selectableHostIds = selectableFilteredHosts.map(host => host.id);
    if (!selectableHostIds.length) return;

    setSelectedNodeIds(prev => {
      const shouldSelectAll = !selectableHostIds.every(id => prev.includes(id));
      const nextSelected = shouldSelectAll
        ? Array.from(new Set([...prev, ...selectableHostIds]))
        : prev.filter(id => !selectableHostIds.includes(id));

      setRolesByHost(rolesPrev => {
        const nextRoles = { ...rolesPrev };
        if (shouldSelectAll) {
          selectableHostIds.forEach(id => {
            nextRoles[id] = nextRoles[id] || defaultRoleForMode;
          });
        } else {
          selectableHostIds.forEach(id => {
            delete nextRoles[id];
          });
        }
        return nextRoles;
      });

      return nextSelected;
    });
    setPrereqResults({});
  };

  const removeNode = (hostId: string) => {
    setSelectedNodeIds(prev => prev.filter(id => id !== hostId));
    // // setDraftNodeIds(prev => prev.filter(id => id !== hostId));
    setRolesByHost(prev => {
      const next = { ...prev };
      delete next[hostId];
      return next;
    });
    setAddonRolesByHost(prev => { const next = { ...prev }; delete next[hostId]; return next; });
    setSchemaConfigsByHost(prev => { const next = { ...prev }; delete next[hostId]; return next; });
    setConnectConfigsByHost(prev => { const next = { ...prev }; delete next[hostId]; return next; });
    setSchemaChecksByHost(prev => { const next = { ...prev }; delete next[hostId]; return next; });
    setConnectChecksByHost(prev => { const next = { ...prev }; delete next[hostId]; return next; });
    setPrereqResults(prev => {
      const next = { ...prev };
      delete next[hostId];
      return next;
    });
  };

  const toggleAddonRole = (hostId: string, role: AddonRole) => {
    setPrereqResults({});
    setPortCheckResults({});
    const assignedHost = selectedHosts.find(host => host.id !== hostId && hostHasAddonRole(host.id, role));
    if (assignedHost && !hostHasAddonRole(hostId, role)) {
      notifyAction(`${role === 'schema_registry' ? 'Schema Registry' : 'Kafka Connect'} is currently deployed once per cluster. Remove the role from ${assignedHost.hostname} before assigning it to another host.`);
      return;
    }
    setAddonRolesByHost(prev => {
      const active = (prev[hostId] || []).includes(role);
      const nextRoles = active ? (prev[hostId] || []).filter(item => item !== role) : [...(prev[hostId] || []), role];
      return { ...prev, [hostId]: nextRoles };
    });
    if (role === 'schema_registry') {
      setSchemaConfigsByHost(prev => prev[hostId] ? prev : { ...prev, [hostId]: defaultSchemaRegistryConfig() });
      setSchemaChecksByHost(prev => { const next = { ...prev }; delete next[hostId]; return next; });
    } else {
      setConnectConfigsByHost(prev => prev[hostId] ? prev : { ...prev, [hostId]: defaultKafkaConnectConfig() });
      setConnectChecksByHost(prev => { const next = { ...prev }; delete next[hostId]; return next; });
    }
  };

  const updateSchemaConfig = (hostId: string, patch: Partial<SchemaRegistryConfig>) => {
    setPrereqResults({});
    setPortCheckResults({});
    setSchemaConfigsByHost(prev => ({ ...prev, [hostId]: { ...schemaConfigFor(hostId), ...patch } }));
    setSchemaChecksByHost(prev => { const next = { ...prev }; delete next[hostId]; return next; });
  };

  const updateConnectConfig = (hostId: string, patch: Partial<KafkaConnectConfig>) => {
    setPrereqResults({});
    setPortCheckResults({});
    setConnectConfigsByHost(prev => ({ ...prev, [hostId]: { ...connectConfigFor(hostId), ...patch } }));
    setConnectChecksByHost(prev => { const next = { ...prev }; delete next[hostId]; return next; });
  };

  const changeDeploymentMode = (mode: DeploymentMode) => {
    setDeploymentMode(mode);
    const nextDefaultRole: RoleChoice = isAddNodeMode ? 'broker' : mode === 'zookeeper' ? 'broker_zookeeper' : 'broker_controller';
    setRolesByHost(() => {
      const next: Record<string, RoleChoice> = {};
      selectedNodeIds.forEach(id => { next[id] = nextDefaultRole; });
      return next;
    });
    setConfigsByService({});
    setCommonConfigs(createCommonConfigs(mode));
    setCommonConfigKind('server');
    setPrereqResults({});
    if (mode === 'kraft') {
      setControllerPort(9093);
    } else {
      setControllerPort(2181);
    }
  };

  const changeKafkaVersion = (version: string) => {
    setKafkaVersion(version);
    if (kafkaMajorVersion(version) >= 4 && deploymentMode === 'zookeeper') {
      changeDeploymentMode('kraft');
    }
  };

  const getHostPorts = (hostId: string): HostPorts => hostPorts[hostId] || {
    listenerPort,
    controllerPort,
    brokerJmxPort: DEFAULT_BROKER_JMX_PORT,
    controllerJmxPort: DEFAULT_CONTROLLER_JMX_PORT,
    zookeeperPeerPort,
    zookeeperElectionPort,
  };

  // During preview Kafka has not been installed yet, but Schema Registry and
  // Connect still need the endpoint that they will use after deployment.
  const plannedBootstrapServers = () => selectedHosts
    .filter(host => ['broker', 'broker_controller', 'separate', 'broker_zookeeper'].includes(rolesByHost[host.id] || defaultRoleForMode))
    .map(host => `${displayIp(host)}:${getHostPorts(host.id).listenerPort}`)
    .filter(endpoint => !endpoint.startsWith('Unknown:'))
    .join(',');

  const updateHostPort = (hostId: string, key: HostPortKey, value: number) => {
    setHostPorts(prev => ({
      ...prev,
      [hostId]: { ...getHostPorts(hostId), [key]: value }
    }));
    setPortCheckResults(prev => {
      const next = { ...prev };
      delete next[hostId];
      return next;
    });
  };

  const portFieldsForHost = (hostId: string) => {
    const role = rolesByHost[hostId] || defaultRoleForMode;
    const fields: Array<{ key: HostPortKey; label: string }> = [];
    if (['broker', 'broker_controller', 'separate', 'broker_zookeeper'].includes(role)) {
      fields.push({ key: 'listenerPort', label: 'Broker Port' });
      fields.push({ key: 'brokerJmxPort', label: 'Broker JMX Port' });
    }
    if (deploymentMode === 'kraft' && ['controller', 'broker_controller', 'separate'].includes(role)) {
      fields.push({ key: 'controllerPort', label: 'Controller Port' });
      if (role === 'controller' || role === 'separate') {
        fields.push({ key: 'controllerJmxPort', label: 'Controller JMX Port' });
      }
    }
    if (deploymentMode === 'zookeeper' && ['zookeeper', 'broker_zookeeper'].includes(role)) {
      fields.push(
        { key: 'controllerPort', label: 'ZooKeeper Client Port' },
        { key: 'zookeeperPeerPort', label: 'ZooKeeper Peer Port' },
        { key: 'zookeeperElectionPort', label: 'ZooKeeper Election Port' },
      );
    }
    return fields;
  };

  const prerequisitePortsForHost = (hostId: string): number[] => {
    const role = rolesByHost[hostId] || defaultRoleForMode;
    const ports = new Set<number>();
    const hp = getHostPorts(hostId);
    const hasBroker = ['broker', 'broker_controller', 'separate', 'broker_zookeeper'].includes(role);
    if (hasBroker) {
      ports.add(hp.listenerPort);
      ports.add(hp.brokerJmxPort);
    }
    if (deploymentMode === 'kraft' && ['controller', 'broker_controller', 'separate'].includes(role)) {
      ports.add(hp.controllerPort);
      if (role === 'controller' || role === 'separate') {
        ports.add(hp.controllerJmxPort);
      }
    }
    if (deploymentMode === 'zookeeper' && ['zookeeper', 'broker_zookeeper'].includes(role)) {
      ports.add(hp.controllerPort);
      ports.add(hp.zookeeperPeerPort);
      ports.add(hp.zookeeperElectionPort);
    }
    if (hostHasAddonRole(hostId, 'schema_registry')) ports.add(schemaConfigFor(hostId).port);
    if (hostHasAddonRole(hostId, 'kafka_connect')) ports.add(connectConfigFor(hostId).port);
    return Array.from(ports);
  };

  const pollSchemaTask = async (hostId: string, taskId: string): Promise<PrereqResult> => {
    for (let attempt = 0; attempt < 60; attempt++) {
      await new Promise(resolve => setTimeout(resolve, 1500));
      const res = await fetch(`/api/v1/ui/hosts/${hostId}/check-prerequisites/${taskId}`);
      if (!res.ok) continue;
      const body = await res.json();
      const status = String(body.status || 'RUNNING').toUpperCase() as PrereqStatus;
      const result = { status, taskId, logOutput: body.logOutput || '', errorMsg: body.errorMsg || '' };
      if (status === 'SUCCESS' || status === 'FAILED') return result;
    }
    return { status: 'FAILED', taskId, logOutput: '', errorMsg: 'Timed out waiting for Schema Registry pre-check.' };
  };

  const dataServiceSafetyParameters = (hostId: string, role: AddonRole) => {
    const otherRole = role === 'schema_registry' ? 'kafka_connect' : 'schema_registry';
    const other = hostHasAddonRole(hostId, otherRole)
      ? (otherRole === 'schema_registry' ? schemaConfigFor(hostId) : connectConfigFor(hostId)) : null;
    return {
      kafka_data_dir: dataDir, kafka_log_dir: logDir,
      other_install_dir: other?.installDir, other_config_dir: other?.configDir,
      other_log_dir: other?.logDir, other_working_dir: other?.workingDir,
      other_plugin_dir: other && 'pluginDir' in other ? other.pluginDir : undefined,
    };
  };

  const runSchemaPrecheck = async (hostId: string) => {
    const revision = addonCheckRevision.current;
    const cfg = schemaConfigFor(hostId);
    if (!cfg.artifactId) return;
    const artifact = schemaArtifacts.find(item => item.id === cfg.artifactId);
    const kafkaArtifact = versions.find(item => item.version === kafkaVersion);
    const kafkaVersionedInstallDir = `${installDir.replace(/\/$/, '')}/kafka_${kafkaArtifact?.scala_version || '2.13'}-${kafkaVersion}`;
    setSchemaChecksByHost(prev => ({ ...prev, [hostId]: { status: 'RUNNING', logOutput: 'Running Schema Registry pre-check...', errorMsg: '' } }));
    try {
      const res = await fetch(`/api/v1/ui/hosts/${hostId}/precheck-schema`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          artifact_url: `${runtimeConfig.artifactApiBasePath}/${cfg.artifactId}/download`,
          checksum: artifact?.sha256 || artifact?.checksum || '',
          schema_version: artifact?.version || '',
          kafka_version: kafkaVersion,
          rest_port: cfg.port, install_dir: cfg.installDir,
          config_dir: cfg.configDir, log_dir: cfg.logDir, working_dir: cfg.workingDir,
          heap_size: cfg.heapSize, compatibility_level: cfg.compatibility, schemas_topic: '_schemas',
          bootstrap_servers: plannedBootstrapServers(),
          kafka_install_dir: kafkaVersionedInstallDir,
          allow_deferred_kafka: true,
          ...dataServiceSafetyParameters(hostId, 'schema_registry'),
        }),
      });
      const queued = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(queued.message || 'Unable to start Schema Registry pre-check.');
      const result = await pollSchemaTask(hostId, queued.taskId);
      if (revision === addonCheckRevision.current) setSchemaChecksByHost(prev => ({ ...prev, [hostId]: result }));
    } catch (error) {
      if (revision !== addonCheckRevision.current) return;
      setSchemaChecksByHost(prev => ({ ...prev, [hostId]: { status: 'FAILED', logOutput: '', errorMsg: error instanceof Error ? error.message : 'Schema Registry pre-check failed.' } }));
    }
  };

  const pollConnectTask = async (hostId: string, taskId: string): Promise<PrereqResult> => {
    for (let i = 0; i < 90; i++) {
      await new Promise(resolve => setTimeout(resolve, 2000));
      const res = await fetch(`/api/v1/ui/hosts/${hostId}/check-prerequisites/${taskId}`);
      if (!res.ok) continue;
      const data = await res.json().catch(() => ({}));
      const status: PrereqStatus = String(data.status || '').toUpperCase() as PrereqStatus;
      if (status === 'SUCCESS' || status === 'FAILED' || status === 'REBOOT_REQUIRED') {
        return { status, taskId, logOutput: data.logOutput || '', errorMsg: data.errorMsg || '' };
      }
    }
    return { status: 'FAILED', taskId, logOutput: '', errorMsg: 'Timed out waiting for Kafka Connect pre-check.' };
  };

  const runConnectPrecheck = async (hostId: string) => {
    const revision = addonCheckRevision.current;
    const cfg = connectConfigFor(hostId);
    if (!cfg.artifactId) return;
    const artifact = connectArtifacts.find(item => item.id === cfg.artifactId);
    setConnectChecksByHost(prev => ({ ...prev, [hostId]: { status: 'RUNNING', logOutput: 'Running Kafka Connect pre-check...', errorMsg: '' } }));
    try {
      const res = await fetch(`/api/v1/ui/hosts/${hostId}/precheck-connect`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          artifact_url: `${runtimeConfig.artifactApiBasePath}/${cfg.artifactId}/download`,
          checksum: artifact?.sha256 || artifact?.checksum || '',
          connect_version: artifact?.version || '',
          rest_port: cfg.port, install_dir: cfg.installDir,
          config_dir: cfg.configDir, log_dir: cfg.logDir, working_dir: cfg.workingDir,
          heap_size: cfg.heapSize, group_id: cfg.groupId, plugin_dir: cfg.pluginDir,
          kafka_version: kafkaVersion, min_free_disk_mb: 5120,
          kafka_install_dir: `${installDir.replace(/\/$/, '')}/kafka_${versions.find(item => item.version === kafkaVersion)?.scala_version || '2.13'}-${kafkaVersion}`,
          bootstrap_servers: plannedBootstrapServers(),
          allow_deferred_kafka: true,
          ...dataServiceSafetyParameters(hostId, 'kafka_connect'),
        }),
      });
      const queued = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(queued.message || 'Unable to start Kafka Connect pre-check.');
      const result = await pollConnectTask(hostId, queued.taskId);
      if (revision === addonCheckRevision.current) setConnectChecksByHost(prev => ({ ...prev, [hostId]: result }));
    } catch (error) {
      if (revision !== addonCheckRevision.current) return;
      setConnectChecksByHost(prev => ({ ...prev, [hostId]: { status: 'FAILED', logOutput: '', errorMsg: error instanceof Error ? error.message : 'Kafka Connect pre-check failed.' } }));
    }
  };

  const pollPortCheck = async (hostId: string, taskId: string) => {
    for (let i = 0; i < 90; i++) {
      await new Promise(resolve => setTimeout(resolve, 1500));
      const res = await fetch(`/api/v1/ui/hosts/${hostId}/check-prerequisites/${taskId}`);
      if (!res.ok) continue;

      const body = await res.json();
      const status = String(body.status || 'RUNNING').toUpperCase();
      setPortCheckResults(prev => ({
        ...prev,
        [hostId]: {
          status: activeStatus(status) ? 'RUNNING' : status === 'SUCCESS' ? 'SUCCESS' : 'FAILED',
          taskId,
          logOutput: body.logOutput || prev[hostId]?.logOutput || '',
          errorMsg: body.errorMsg || '',
        },
      }));
      if (!activeStatus(status)) return;
    }

    setPortCheckResults(prev => ({
      ...prev,
      [hostId]: {
        status: 'FAILED',
        taskId,
        logOutput: prev[hostId]?.logOutput || '',
        errorMsg: 'Port check timed out while waiting for the host agent.',
      },
    }));
  };

  const checkHostPorts = async (hostId: string) => {
    const requiredPorts = prerequisitePortsForHost(hostId);
    if (requiredPorts.length === 0) {
      setPortCheckResults(prev => ({
        ...prev,
        [hostId]: { status: 'FAILED', logOutput: '', errorMsg: 'No required ports are configured for this host.' },
      }));
      return;
    }

    setPortCheckResults(prev => ({
      ...prev,
      [hostId]: { status: 'RUNNING', logOutput: 'Queuing port availability check...', errorMsg: '' },
    }));

    try {
      const res = await fetch(`/api/v1/ui/hosts/${hostId}/check-ports`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ required_ports: requiredPorts.join(',') }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok || !body.taskId) {
        throw new Error(body.message || 'Failed to queue port availability check.');
      }

      setPortCheckResults(prev => ({
        ...prev,
        [hostId]: {
          status: 'RUNNING',
          taskId: body.taskId,
          logOutput: 'Port check queued. Waiting for the host agent...',
          errorMsg: '',
        },
      }));
      await pollPortCheck(hostId, body.taskId);
    } catch (error) {
      setPortCheckResults(prev => ({
        ...prev,
        [hostId]: {
          status: 'FAILED',
          logOutput: '',
          errorMsg: error instanceof Error ? error.message : 'Failed to check ports.',
        },
      }));
    }
  };



  const checkPrerequisites = async () => {
    setCheckingPrereqs(true);
    const initial: Record<string, PrereqResult> = {};
    selectedHosts.forEach(host => {
      initial[host.id] = { status: 'QUEUED', logOutput: 'Queued prerequisite check.', errorMsg: '' };
    });
    setPrereqResults(initial);

    await Promise.all(selectedHosts.map(async host => {
      try {
        const res = await fetch(`/api/v1/ui/hosts/${host.id}/check-prerequisites`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            mode: deploymentMode,
            required_ports: prerequisitePortsForHost(host.id).join(','),
          }),
        });
        const body = await res.json().catch(() => ({}));
        if (!res.ok) {
          setPrereqResults(prev => ({
            ...prev,
            [host.id]: {
              status: 'FAILED',
              logOutput: '',
              errorMsg: body.message || 'Failed to queue prerequisite check.',
            },
          }));
          return;
        }

        setPrereqResults(prev => ({
          ...prev,
          [host.id]: {
            status: 'RUNNING',
            taskId: body.taskId,
            logOutput: 'Task queued. Waiting for agent to report progress...',
            errorMsg: '',
          },
        }));

        await pollPrerequisite(host.id, body.taskId);
      } catch (e) {
        console.error(e);
        setPrereqResults(prev => ({
          ...prev,
          [host.id]: {
            status: 'FAILED',
            logOutput: '',
            errorMsg: 'Network error while queuing prerequisite check.',
          },
        }));
      }
    }));

    await Promise.all([
      ...schemaHosts.map(host => runSchemaPrecheck(host.id)),
      ...connectHosts.map(host => runConnectPrecheck(host.id)),
    ]);
    setCheckingPrereqs(false);
  };

  const pollPrerequisite = async (hostId: string, taskId: string) => {
    for (let i = 0; i < 90; i++) {
      await new Promise(resolve => setTimeout(resolve, 1500));
      const res = await fetch(`/api/v1/ui/hosts/${hostId}/check-prerequisites/${taskId}`);
      if (!res.ok) continue;
      const body = await res.json();
      const status = String(body.status || 'RUNNING').toUpperCase();
      setPrereqResults(prev => ({
        ...prev,
        [hostId]: {
          status: activeStatus(status)
            ? 'RUNNING'
            : status === 'SUCCESS'
              ? 'SUCCESS'
              : status === 'REBOOT_REQUIRED'
                ? 'REBOOT_REQUIRED'
                : 'FAILED',
          taskId,
          logOutput: body.logOutput || prev[hostId]?.logOutput || '',
          errorMsg: body.errorMsg || '',
        },
      }));
      if (!activeStatus(status)) return;
    }
    setPrereqResults(prev => ({
      ...prev,
      [hostId]: {
        status: 'FAILED',
        taskId,
        logOutput: prev[hostId]?.logOutput || '',
        errorMsg: 'Timed out waiting for prerequisite result.',
      },
    }));
  };

  const fixPrerequisites = async () => {
    const failedHosts = selectedHosts.filter(host => prereqResults[host.id]?.status === 'FAILED');
    if (failedHosts.length === 0) return;
    const confirmed = await confirmAction(
      `Apply privileged operating-system changes on ${failedHosts.length} host(s)? This may update limits, sysctl, THP, SELinux, time synchronization, and may require a reboot.`,
    );
    if (!confirmed) return;

    setCheckingPrereqs(true);
    await Promise.all(failedHosts.map(async host => {
      try {
        const res = await fetch(`/api/v1/ui/hosts/${host.id}/fix-prerequisites`, { method: 'POST' });
        const body = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(body.message || 'Failed to queue prerequisite remediation.');
        setPrereqResults(prev => ({
          ...prev,
          [host.id]: { status: 'RUNNING', taskId: body.taskId, logOutput: 'Applying prerequisite fixes...', errorMsg: '' },
        }));
        await pollPrerequisite(host.id, body.taskId);
      } catch (error) {
        setPrereqResults(prev => ({
          ...prev,
          [host.id]: { status: 'FAILED', logOutput: prev[host.id]?.logOutput || '', errorMsg: error instanceof Error ? error.message : 'Failed to fix prerequisites.' },
        }));
      }
    }));
    setCheckingPrereqs(false);
  };

  const rebootHost = async (host: Host) => {
    if (!(await confirmAction(`Reboot ${host.hostname}? The host and agent will be temporarily offline.`))) return;
    setCheckingPrereqs(true);
    try {
      const res = await fetch(`/api/v1/ui/hosts/${host.id}/reboot`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirmed: true }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.message || 'Failed to schedule reboot.');
      setPrereqResults(prev => ({
        ...prev,
        [host.id]: { status: 'RUNNING', taskId: body.taskId, logOutput: 'Scheduling host reboot...', errorMsg: '' },
      }));
      await pollPrerequisite(host.id, body.taskId);
    } catch (error) {
      setPrereqResults(prev => ({
        ...prev,
        [host.id]: { status: 'REBOOT_REQUIRED', logOutput: prev[host.id]?.logOutput || '', errorMsg: error instanceof Error ? error.message : 'Failed to schedule reboot.' },
      }));
    } finally {
      setCheckingPrereqs(false);
    }
  };

  const deployCluster = async () => {
    setDeploying(true);
    try {
      const payload = buildDeploymentPayload();

      const url = isAddNodeMode && addClusterId
        ? `/api/v1/ui/clusters/${addClusterId}/nodes`
        : '/api/v1/ui/clusters/deploy';
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        notifyAction(body.error || body.message || 'Deployment failed to start.');
        return;
      }
      if (onClose) {
        onClose();
      }
      if (body.jobId) {
        navigate(`/jobs/${body.jobId}`);
      } else {
        navigate(`/clusters/${body.id}/logs`);
      }
    } catch (e) {
      console.error(e);
      notifyAction('Network error while starting deployment.');
    } finally {
      setDeploying(false);
    }
  };

  const mainContent = (
    <div
      className={`cluster-deploy-page ${onClose ? 'modal-version' : ''} animate-fade-in`}
      style={!onClose ? { maxWidth: '100%' } : {}}
    >
      {(!onClose || stage === 'preview') && (
        <header className="cd-header">
          <div>
            <h1>
              <ChevronLeft size={24} color="#818181" className="cd-back-icon" onClick={() => {
                if (stage === 'preview') {
                  setStage('details');
                } else {
                  window.history.back();
                }
              }} />
              {stage === 'details' ? (isAddNodeMode ? 'Add Node to Cluster' : 'Create Kafka Cluster') : (isAddNodeMode ? 'Preview Node Addition' : 'Preview Deployment')}
            </h1>
            <p>{stage === 'details'
              ? isAddNodeMode
                ? 'External cluster details are loaded. Select new nodes and roles to add.'
                : 'Define the cluster, select nodes, and choose roles.'
              : 'Run prerequisites across every selected node before deployment.'}</p>
          </div>
          <div className="cd-header-side">
            <div className="cd-stage-tabs" aria-label="Deployment progress">
              <span
                className={stage === 'details' ? 'active' : ''}
                onClick={() => setStage('details')}
                style={{ cursor: 'pointer' }}
              >
                Details
              </span>
              <span
                className={`${stage === 'preview' ? 'active' : ''} ${!canPreview ? 'disabled' : ''}`}
                onClick={() => {
                  if (canPreview) {
                    if (stage === 'details') {
                      openPreview();
                    } else {
                      setStage('preview');
                    }
                  }
                }}
                style={{ cursor: canPreview ? 'pointer' : 'not-allowed', opacity: canPreview ? 1 : 0.5 }}
              >
                Preview
              </span>
            </div>
          </div>
        </header>
      )}

      {stage === 'details' ? (
        <div className="cd-layout">
          {loadingCluster && (
            <section className="cd-panel">
              <div className="cd-template-summary">
                <Loader2 size={16} className="spin" />
                <span>Loading existing cluster details...</span>
              </div>
            </section>
          )}
          <section className="cd-panel">
            <div className="cd-panel-title">
              <h2>Cluster Details</h2>
              <div className="cd-header-toggle">
                <span className={clusterConfigMode === 'default' ? 'active' : ''}>Default</span>
                <label className="cd-toggle-switch">
                  <input type="checkbox" checked={clusterConfigMode === 'custom'} onChange={() => selectClusterConfigMode(clusterConfigMode === 'default' ? 'custom' : 'default')} disabled={isAddNodeMode} />
                  <span className="cd-toggle-slider"></span>
                </label>
                <span className={clusterConfigMode === 'custom' ? 'active' : ''}>Custom</span>
              </div>
            </div>
            {clusterConfigMode === 'custom' && !isAddNodeMode && (
              <div className="cd-custom-import">
                <div className="cd-custom-import-row">
                  <div className="cd-custom-import-info">
                    <strong>Install Customs Cluster configurations</strong>
                    <p>Use the CSV template to import cluster paths and properties. Host details aren't imported.</p>
                  </div>
                  <div className="cd-custom-import-actions">
                    <label className="cd-custom-btn-upload">
                      <Upload size={16} /> Upload CSV
                      <input type="file" accept=".csv,text/csv" hidden onChange={event => {
                        const selected = event.target.files?.[0];
                        if (selected) void importCustomCsv(selected);
                        event.target.value = '';
                      }} />
                    </label>
                    <button type="button" className="cd-custom-btn-download" onClick={downloadCustomTemplate}>
                      <Download size={16} /> Download Template
                    </button>
                  </div>
                </div>
                {customImportSummary && <span className="cd-import-summary">{customImportSummary}</span>}
              </div>
            )}
            <div className="cd-grid-2">
              <label className="cd-field">
                <span>Cluster Name</span>
                <input value={clusterName} onChange={e => setClusterName(e.target.value)} placeholder="production-kraft" disabled={isAddNodeMode} />
              </label>
              {isAddNodeMode && (
                <label className="cd-field">
                  <span>Kafka Cluster ID</span>
                  <input value={existingCluster?.kafkaClusterId || addClusterId || ''} disabled />
                </label>
              )}
              <label className="cd-field">
                <span>Kafka Version</span>
                <div style={{ position: 'relative', width: '100%' }}>
                  <select
                    value={kafkaVersion}
                    onChange={e => changeKafkaVersion(e.target.value)}
                    disabled={isAddNodeMode || loadingVersions || versions.length === 0}
                    style={{
                      appearance: 'none',
                      WebkitAppearance: 'none',
                      color: '#818181',
                      paddingRight: '40px'
                    }}
                  >
                    {availableVersions.map(version => (
                      <option key={version.version} value={version.version}>
                        {version.version} ({version.size_mb} MB)
                      </option>
                    ))}
                    {availableVersions.length === 0 && <option>No available Kafka artifact</option>}
                  </select>
                  <div style={{ position: 'absolute', right: '16px', top: '10px', pointerEvents: 'none', color: '#818181' }}>
                    <ChevronDown size={20} />
                  </div>
                </div>
              </label>
              <div className="cd-field">
                <span>Environment (optional)</span>
                <div className="cd-env-buttons">
                  {['SIT', 'UAT', 'DEV'].map(env => (
                    <button
                      key={env}
                      className={environment.toUpperCase() === env ? 'active' : ''}
                      onClick={() => setEnvironment(env)}
                      disabled={isAddNodeMode}
                    >
                      {env}
                    </button>
                  ))}
                </div>
              </div>
              <div className="cd-field">
                <span>Metadata Mode</span>
                <div className="cd-choice-toggle cd-mode-toggle">
                  <button className={deploymentMode === 'kraft' ? 'active' : ''} onClick={() => changeDeploymentMode('kraft')} disabled={isAddNodeMode}>KRaft</button>
                  {zookeeperSupported && (
                    <button className={deploymentMode === 'zookeeper' ? 'active' : ''} onClick={() => changeDeploymentMode('zookeeper')} disabled={isAddNodeMode}>ZooKeeper</button>
                  )}
                </div>
              </div>
            </div>
          </section>



          <section className="cd-panel">
            <div className="cd-panel-title">
              <h2>Nodes & Roles</h2>
              <button className="cd-add-node-btn" onClick={() => setShowEnrollModal(true)}>
                + Add Node
              </button>
              <button className="cd-refresh-icon" onClick={loadHosts} title="Refresh">
                <CustomRefreshIcon size={14} className={loadingHosts ? 'spin' : ''} />
              </button>
            </div>

            <div className="cd-node-picker-container" style={{ display: 'flex', flexDirection: 'column', gap: '8px', alignSelf: 'stretch', marginBottom: '16px' }}>
              <span style={{ fontFamily: 'Satoshi, sans-serif', fontSize: '14px', fontWeight: 500, color: '#332849' }}>Select node</span>
              <div className="cd-node-picker" ref={dropdownRef}>
                <button className="cd-node-trigger" onClick={() => {
                  setNodeDropdownOpen(open => !open);
                }}>
                  <span>{selectedNodeIds.length ? `${selectedNodeIds.length} node${selectedNodeIds.length > 1 ? 's' : ''} selected` : 'Select'}</span>
                  <ChevronDown size={16} />
                </button>
                {nodeDropdownOpen && dropdownRef.current && (
                  <AnchoredMenu
                    anchor={dropdownRef.current}
                    className="cd-node-menu"
                    onClose={() => setNodeDropdownOpen(false)}
                    align="start"
                    matchAnchorWidth
                  >
                    <div className="cd-search">
                      <Search size={15} />
                      <input value={nodeSearch} onChange={e => setNodeSearch(e.target.value)} placeholder="Search hostname or IP" autoFocus />
                    </div>
                    <button
                      type="button"
                      className={`cd-node-option cd-node-option-select-all ${allFilteredSelected ? 'checked' : ''}`}
                      onClick={toggleSelectAllNodes}
                      disabled={selectableFilteredHosts.length === 0}
                    >
                      <span className="cd-checkbox">{allFilteredSelected && <Check size={12} strokeWidth={3} />}</span>
                      <span className="cd-node-info">
                        <strong>Select all</strong>
                        <small>
                          {selectableFilteredHosts.length
                            ? `Select all ${selectableFilteredHosts.length} available host${selectableFilteredHosts.length > 1 ? 's' : ''}`
                            : 'No available hosts match this search'}
                        </small>
                      </span>
                    </button>
                    <div className="cd-node-options">
                      {filteredHosts.map(host => {
                        const disabled = host.status !== 'AVAILABLE' || host.available === false;
                        const checked = selectedNodeIds.includes(host.id);
                        return (
                          <button
                            key={host.id}
                            className={`cd-node-option ${checked ? 'checked' : ''}`}
                            disabled={disabled}
                            onClick={() => toggleNodeSelection(host.id)}
                          >
                            <span className="cd-checkbox">{checked && <Check size={12} strokeWidth={3} />}</span>
                            <span className="cd-node-info">
                              <strong>{host.hostname}</strong>
                              <small>{displayIp(host)} - {disabled ? nodeAvailabilityMessage(host) : '/srv/tantor-agent/tantor-agent-linux'}</small>
                            </span>
                          </button>
                        );
                      })}
                    </div>
                  </AnchoredMenu>
                )}
              </div>
            </div>


            <section className="cd-panel">
              <div className="cd-panel-title">
                <h2>Deployment Paths</h2>
                <button className="cd-secondary-btn compact" onClick={() => setCommonConfigOpen(true)}>
                  <FileText size={16} />
                  Config
                </button>
              </div>
              <div className="cd-grid-2">
                <label className="cd-field">
                  <span>Install directory</span>
                  <input value={installDir} onChange={e => setInstallDir(e.target.value)} placeholder="/opt" />
                </label>
                <label className="cd-field">
                  <span>Data directory</span>
                  <input value={dataDir} onChange={e => setDataDir(e.target.value)} placeholder="/data/kafka" />
                </label>
                <label className="cd-field">
                  <span>Log directory</span>
                  <input value={logDir} onChange={e => setLogDir(e.target.value)} placeholder="/var/log/kafka" />
                </label>
                <label className="cd-field">
                  <span>Artifacts/Load Directory</span>
                  <input value={artifactLoadDir} onChange={e => setArtifactLoadDir(e.target.value)} placeholder="/srv/tantor-agent/artifacts" />
                </label>
              </div>
            </section>


            <div className="cd-selected-node-list">
              {selectedHosts.map(host => (
                <div className="cd-selected-node" key={host.id} style={{ display: 'flex', flexDirection: 'column', gap: '10px', background: '#FFFFFF', borderRadius: '8px', padding: '10px 16px', border: 'none' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
                    <div className="cd-node-main" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', gap: '4px' }}>
                      <strong style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 500, fontSize: '14px', lineHeight: '19px', color: '#332849', margin: 0 }}>{host.hostname}</strong>
                      <span style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 400, fontSize: '14px', lineHeight: '19px', color: '#818181', margin: 0 }}>{displayIp(host)} - /srv/tantor-agent/tantor-agent-linux</span>
                    </div>
                    <div className="cd-node-actions" style={{ display: 'flex', alignItems: 'center', gap: '8px', position: 'relative' }}>
                      <button
                        className="cd-figma-action-btn"
                        onClick={() => checkHostPorts(host.id)}
                        disabled={portCheckResults[host.id]?.status === 'RUNNING'}
                        onMouseEnter={() => setHoveredPortCheckHostId(host.id)}
                        onMouseLeave={() => setHoveredPortCheckHostId(null)}
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: '6px',
                          cursor: portCheckResults[host.id]?.status === 'RUNNING' ? 'not-allowed' : 'pointer'
                        }}
                      >
                        <span>
                          {portCheckResults[host.id]?.status === 'RUNNING'
                            ? 'Checking...'
                            : portCheckResults[host.id]?.status === 'SUCCESS'
                              ? 'Ports OK'
                              : portCheckResults[host.id]?.status === 'FAILED'
                                ? 'Ports Failed'
                                : 'Check Ports'}
                        </span>
                        {portCheckResults[host.id]?.status === 'RUNNING' ? (
                          <Loader2 size={12} className="spin" style={{ color: '#3E1363' }} />
                        ) : portCheckResults[host.id]?.status === 'SUCCESS' ? (
                          <CheckCircle2 size={14} style={{ color: '#069B68' }} />
                        ) : portCheckResults[host.id]?.status === 'FAILED' ? (
                          <XCircle size={14} style={{ color: '#E15252' }} />
                        ) : (
                          <Play size={10} fill="#3E1363" style={{ transform: 'none' }} />
                        )}
                      </button>

                      {hoveredPortCheckHostId === host.id && portCheckResults[host.id] && (
                        <div
                          style={{
                            position: 'absolute',
                            top: '40px',
                            left: '0',
                            zIndex: 1000,
                            width: '240px',
                            background: '#FAF8FF',
                            border: '1px solid #CCCCCC',
                            borderRadius: '8px',
                            padding: '12px',
                            boxShadow: '0px 4px 12px rgba(0, 0, 0, 0.08)',
                            display: 'flex',
                            flexDirection: 'column',
                            gap: '8px',
                            fontFamily: 'Satoshi, sans-serif',
                            fontSize: '13px',
                            color: '#332849',
                            pointerEvents: 'none',
                            textAlign: 'left',
                          }}
                        >
                          {(() => {
                            const result = portCheckResults[host.id];
                            const log = result.logOutput || '';
                            const lines = log.split('\n');
                            const available: number[] = [];
                            const unavailable: number[] = [];

                            lines.forEach(line => {
                              const match = line.match(/Port (\d+):\s*(Available|Unavailable)/i);
                              if (match) {
                                const port = parseInt(match[1], 10);
                                if (match[2].toLowerCase() === 'available') {
                                  available.push(port);
                                } else {
                                  unavailable.push(port);
                                }
                              }
                            });

                            if (result.status === 'SUCCESS') {
                              return (
                                <>
                                  <div style={{ fontWeight: 600, color: '#069B68' }}>All ports available</div>
                                  {available.map(p => (
                                    <div key={p} style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                      <span style={{ color: '#069B68' }}>-</span>
                                      <span>Port {p}</span>
                                    </div>
                                  ))}
                                </>
                              );
                            }

                            return (
                              <>
                                {unavailable.length > 0 && (
                                  <div>
                                    <div style={{ fontWeight: 600, color: '#332849', marginBottom: '4px' }}>Unavailable (in use):</div>
                                    {unavailable.map(p => (
                                      <div key={p} style={{ display: 'flex', alignItems: 'center', gap: '6px', paddingLeft: '4px' }}>
                                        <span style={{ color: '#E15252' }}>-</span>
                                        <span>Port {p}</span>
                                      </div>
                                    ))}
                                  </div>
                                )}
                                {available.length > 0 && (
                                  <div style={{ marginTop: '4px' }}>
                                    <div style={{ fontWeight: 600, color: '#332849', marginBottom: '4px' }}>Available (free):</div>
                                    {available.map(p => (
                                      <div key={p} style={{ display: 'flex', alignItems: 'center', gap: '6px', paddingLeft: '4px' }}>
                                        <span style={{ color: '#069B68' }}>-</span>
                                        <span>Port {p}</span>
                                      </div>
                                    ))}
                                  </div>
                                )}
                                {unavailable.length === 0 && available.length === 0 && (
                                  <div style={{ color: '#E15252' }}>{result.errorMsg || 'Failed to check ports'}</div>
                                )}
                              </>
                            );
                          })()}
                        </div>
                      )}
                      <div className="cd-role-menu-wrap" style={{ position: 'relative' }}>
                        <button
                          className="cd-figma-action-btn"
                          onClick={event => {
                            const opening = openRoleMenuHostId !== host.id;
                            setOpenRoleMenuHostId(opening ? host.id : null);
                            setRoleMenuAnchor(opening ? event.currentTarget : null);
                          }}
                        >
                          <span>{[
                            (rolesByHost[host.id] || defaultRoleForMode).replace('_', ' + ').replace(/\b\w/g, l => l.toUpperCase()),
                            ...(addonRolesByHost[host.id] || []).map(role => role === 'schema_registry' ? 'Schema Registry' : 'Kafka Connect'),
                          ].join(' + ')}</span>
                          <MoreVertical size={14} />
                        </button>
                        {openRoleMenuHostId === host.id && roleMenuAnchor && (
                          <AnchoredMenu anchor={roleMenuAnchor} className="cd-role-menu" onClose={() => { setOpenRoleMenuHostId(null); setRoleMenuAnchor(null); }}>
                            {roleOptions.filter(r => r.id !== 'separate').map(role => {
                              const currentRole = rolesByHost[host.id] || defaultRoleForMode;
                              let isActive = currentRole === role.id;

                              if (deploymentMode === 'kraft') {
                                if (role.id === 'broker') isActive = currentRole === 'broker' || currentRole === 'separate';
                                if (role.id === 'controller') isActive = currentRole === 'controller' || currentRole === 'separate';
                              }

                              return (
                                <label
                                  key={role.id}
                                  className={`cd-role-label ${isActive ? 'active' : ''}`}
                                >
                                  <input
                                    type="checkbox"
                                    checked={isActive}
                                    onChange={() => {
                                      let nextRole = role.id;

                                      if (deploymentMode === 'kraft') {
                                        if (role.id === 'broker') {
                                          if (currentRole === 'controller') nextRole = 'separate';
                                          else if (currentRole === 'separate') nextRole = 'controller';
                                        } else if (role.id === 'controller') {
                                          if (currentRole === 'broker') nextRole = 'separate';
                                          else if (currentRole === 'separate') nextRole = 'broker';
                                        }
                                      }

                                      setRolesByHost(prev => ({ ...prev, [host.id]: nextRole }));
                                      setPrereqResults({});
                                      setPortCheckResults({});
                                    }}
                                  />
                                  <span>{role.label}</span>
                                </label>
                              );
                            })}
                            {!isAddNodeMode && <>
                              <div className="cd-role-menu-divider" />
                              {SCHEMA_REGISTRY_DEPLOYMENT_ENABLED && (
                                <label className={`cd-role-label ${hostHasAddonRole(host.id, 'schema_registry') ? 'active' : ''}`}>
                                  <input type="checkbox" checked={hostHasAddonRole(host.id, 'schema_registry')} onChange={() => toggleAddonRole(host.id, 'schema_registry')} />
                                  <span>Schema Registry</span>
                                </label>
                              )}
                              {KAFKA_CONNECT_DEPLOYMENT_ENABLED && (
                                <label className={`cd-role-label ${hostHasAddonRole(host.id, 'kafka_connect') ? 'active' : ''}`}>
                                  <input type="checkbox" checked={hostHasAddonRole(host.id, 'kafka_connect')} onChange={() => toggleAddonRole(host.id, 'kafka_connect')} />
                                  <span>Kafka Connect</span>
                                </label>
                              )}
                            </>}
                          </AnchoredMenu>
                        )}
                      </div>
                      <button className="cd-figma-action-btn" onClick={() => setConfigModalHostId(host.id)}>
                        <FileText size={14} />
                        Configuration
                      </button>
                      <button className="cd-figma-icon-btn" onClick={() => removeNode(host.id)} title="Remove node" style={{ width: '24px', height: '24px', padding: 0 }}>
                        <Trash2 size={16} />
                      </button>
                    </div>
                  </div>

                </div>
              ))}
            </div>

          </section>
          <div className="cd-footer-actions">
            <button className="cd-secondary-btn" onClick={() => onClose ? onClose() : navigate(-1)}>Cancel</button>
            <button className="cd-primary-btn" disabled={!canPreview || validatingKraft} onClick={openPreview}>
              {validatingKraft && <Loader2 size={15} className="spin" />}
              {isAddNodeMode ? 'Preview add node' : validatingKraft ? 'Validating topology' : 'Preview'}
            </button>
          </div>
        </div>
      ) : (
        <div className="cd-layout">
          {deploymentMode === 'kraft' && !isAddNodeMode && kraftValidation && (
            <section className="cd-panel cd-kraft-validation">
              <div className="cd-panel-title">
                <h2>KRaft Topology Validation</h2>
                <span className={`cd-validation-state ${kraftValidation.valid ? 'valid' : 'invalid'}`}>
                  {!kraftValidation.valid && <XCircle size={14} />}
                  {kraftValidation.valid ? 'Topology valid' : 'Changes required'}
                </span>
              </div>

              <div className="cd-kraft-facts">
                <div><span>Kafka cluster ID</span><strong>{kraftValidation.clusterId}</strong></div>
                <div><span>Quorum mode</span><strong>{kraftValidation.quorumMode}</strong></div>
                <div><span>Controllers</span><strong>{kraftValidation.controllerCount}</strong></div>
                <div><span>Brokers</span><strong>{kraftValidation.brokerCount}</strong></div>
                <div><span>Failure tolerance</span><strong>{kraftValidation.failureTolerance} controller{kraftValidation.failureTolerance === 1 ? '' : 's'}</strong></div>
              </div>

              <div className="cd-quorum-value">
                <span>{kraftValidation.quorumMode === 'static' ? 'controller.quorum.voters' : 'controller.quorum.bootstrap.servers'}</span>
                <code>{kraftValidation.controllerQuorum}</code>
              </div>

              <div className="cd-kraft-table-wrap">
                <table className="cd-kraft-table">
                  <thead><tr><th>Host</th><th>Address</th><th>Node ID</th><th>Role</th></tr></thead>
                  <tbody>
                    {kraftValidation.nodes.map(node => (
                      <tr key={`${node.hostId}-${node.nodeId}`}>
                        <td>{hosts.find(host => host.id === node.hostId)?.hostname || node.hostId}</td>
                        <td>{node.address}</td>
                        <td>{node.nodeId}</td>
                        <td>{node.role.replace('_', ' + ')}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {kraftValidation.errors.length > 0 && (
                <div className="cd-inline-errors">
                  {kraftValidation.errors.map(error => <span key={error}><XCircle size={13} /> {error}</span>)}
                </div>
              )}
              {kraftValidation.warnings.length > 0 && (
                <div className="cd-warning-list">
                  {kraftValidation.warnings.map(warning => <span key={warning}><AlertTriangle size={13} /> {warning}</span>)}
                </div>
              )}
              {kraftValidation.acknowledgementRequired && (
                <label className="cd-risk-ack">
                  <input
                    type="checkbox"
                    checked={kraftRiskAcknowledged}
                    onChange={event => setKraftRiskAcknowledged(event.target.checked)}
                  />
                  <span>I understand this controller topology has reduced availability and want to continue.</span>
                </label>
              )}
            </section>
          )}
          <section className="cd-panel">
            <div className="cd-panel-title">
              <h2>Nodes Selected for Deployment</h2>
            </div>
            <div className="cd-preview-list">
              {selectedHosts.map(host => {
                const role = allRoleOptions.find(item => item.id === (rolesByHost[host.id] || defaultRoleForMode));
                const addonRoles = addonRolesByHost[host.id] || [];
                const result = prereqResults[host.id];
                return (
                  <div className="cd-preview-row" key={host.id}>
                    <div className="cd-node-main">
                      <strong>{host.hostname}</strong>
                      <span>{displayIp(host)}</span>
                    </div>
                    <div className="cd-role-copy">
                      <strong>{[role?.label, ...addonRoles.map(addon => addon === 'schema_registry' ? 'Schema Registry' : 'Kafka Connect')].filter(Boolean).join(' + ')}</strong>
                      <span>{role?.detail}{addonRoles.length ? ' Includes host-specific data-service configuration.' : ''}</span>
                    </div>
                    <StatusBadge status={result?.status || 'IDLE'} />
                  </div>
                );
              })}
            </div>
            {[...warnings, ...replicationWarnings].length > 0 && (
              <div className="cd-warning-list">
                {[...warnings, ...replicationWarnings].map(warning => <span key={warning}><AlertTriangle size={13} /> {warning}</span>)}
              </div>
            )}
            {[...pathErrors, ...configBlockingIssues, ...selectedPortValidationErrors].length > 0 && (
              <div className="cd-inline-errors">
                {[...pathErrors, ...configBlockingIssues, ...selectedPortValidationErrors].map(item => <span key={item}><AlertTriangle size={13} /> {item}</span>)}
              </div>
            )}
          </section>

          <section className="cd-panel">
            <div className="cd-panel-title">
              <h2>Prerequisites</h2>
              <button className="cd-prereqs-check-btn" disabled={checkingPrereqs || selectedHosts.length === 0 || pathErrors.length > 0 || configBlockingIssues.length > 0 || (kraftValidation?.errors.length || 0) > 0} onClick={checkPrerequisites}>
                {checkingPrereqs ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />}
                Check prerequisites on all nodes
              </button>
              {selectedHosts.some(host => prereqResults[host.id]?.status === 'FAILED') && (
                <button className="cd-secondary-btn compact cd-fix-prereqs-btn" disabled={checkingPrereqs} onClick={fixPrerequisites}>
                  <Settings2 size={14} /> Fix failed prerequisites
                </button>
              )}
            </div>
            {checkingPrereqs && <div className="cd-progress"><span /></div>}
            <div className="cd-prereq-grid">
              {selectedHosts.map(host => {
                const result = prereqResults[host.id] || { status: 'IDLE', logOutput: '', errorMsg: '' };
                return (
                  <details className="cd-prereq-card" key={host.id} open={result.status === 'FAILED' || result.status === 'REBOOT_REQUIRED'}>
                    <summary>
                      <span>{host.hostname}</span>
                      <StatusBadge status={result.status} />
                    </summary>
                    {result.errorMsg || result.logOutput
                      ? <PrerequisiteLog result={result} />
                      : <div className="cd-prereq-log"><span className="neutral">Waiting for prerequisite run...</span></div>}
                    {result.status === 'REBOOT_REQUIRED' && (
                      <div className="cd-prereq-remediation-actions">
                        <span><AlertTriangle size={14} /> Persistent settings were applied, but this host must reboot.</span>
                        <button className="cd-secondary-btn compact" disabled={checkingPrereqs} onClick={() => rebootHost(host)}>Reboot host</button>
                      </div>
                    )}
                  </details>
                );
              })}
            </div>
          </section>

          {schemaHosts.length > 0 && (
            <section className="cd-panel">
              <div className="cd-panel-title"><div><h2>Schema Registry pre-check</h2><p>Checks the selected host and configuration. Broker-dependent checks run again after Kafka starts.</p></div></div>
              <div className="cd-prereq-grid">
                {schemaHosts.map(host => {
                  const result = schemaChecksByHost[host.id] || { status: 'IDLE', logOutput: '', errorMsg: '' };
                  const config = schemaConfigFor(host.id);
                  return <details className="cd-prereq-card" key={host.id} open={result.status !== 'IDLE'}>
                    <summary><span>{host.hostname} · port {config.port}</span><StatusBadge status={result.status} /></summary>
                    <button className="cd-secondary-btn compact" disabled={checkingPrereqs || !config.artifactId || activeStatus(result.status)} onClick={() => runSchemaPrecheck(host.id)}>{activeStatus(result.status) && <Loader2 size={14} className="spin" />} Run Schema Registry pre-check</button>
                    {result.status !== 'IDLE' && <PrerequisiteLog result={result} />}
                  </details>;
                })}
              </div>
            </section>
          )}

          {connectHosts.length > 0 && (
            <section className="cd-panel">
              <div className="cd-panel-title"><div><h2>Kafka Connect pre-check</h2><p>Checks the selected host, plugin JARs and working disk. Broker-dependent checks run again after Kafka starts.</p></div></div>
              <div className="cd-prereq-grid">
                {connectHosts.map(host => {
                  const result = connectChecksByHost[host.id] || { status: 'IDLE', logOutput: '', errorMsg: '' };
                  const config = connectConfigFor(host.id);
                  return <details className="cd-prereq-card" key={host.id} open={result.status !== 'IDLE'}>
                    <summary><span>{host.hostname} · port {config.port}</span><StatusBadge status={result.status} /></summary>
                    <button className="cd-secondary-btn compact" disabled={checkingPrereqs || !config.artifactId || activeStatus(result.status)} onClick={() => runConnectPrecheck(host.id)}>{activeStatus(result.status) && <Loader2 size={14} className="spin" />} Run Kafka Connect pre-check</button>
                    {result.status !== 'IDLE' && <PrerequisiteLog result={result} />}
                  </details>;
                })}
              </div>
            </section>
          )}

          <div className="cd-footer-actions">
            <button className="cd-secondary-btn" disabled={checkingPrereqs || deploying} onClick={() => setStage('details')}>Cancel</button>
            <button className="cd-primary-btn" disabled={checkingPrereqs || !prerequisiteComplete || !schemaPrecheckComplete || !connectPrecheckComplete || deploying || pathErrors.length > 0 || configBlockingIssues.length > 0 || kraftDeploymentBlocked} onClick={deployCluster}>
              {deploying && <Loader2 size={15} className="spin" />}
              {isAddNodeMode ? 'Add node' : 'Deploy'}
            </button>
          </div>
        </div>
      )}
      {showEnrollModal && (
        <AgentConnectivityModal onClose={() => {
          setShowEnrollModal(false);
          loadHosts();
        }} />
      )}
      {configModalHost && createPortal(
        <div className="cd-modal-backdrop" role="dialog" aria-modal="true" onClick={() => setConfigModalHostId(null)}>
          <div className="cd-config-modal" onClick={e => e.stopPropagation()}>
            <div className="cd-config-modal-header">
              <div>
                <h2>Configuration</h2>
                <p>{configModalHost.hostname} - {allRoleOptions.find(role => role.id === (rolesByHost[configModalHost.id] || defaultRoleForMode))?.label}</p>
              </div>
              <button className="cd-icon-btn" onClick={() => setConfigModalHostId(null)} title="Close configuration">
                <X size={16} />
              </button>
            </div>

            <div className="cd-config-modal-body">
              {configKindsForRole(rolesByHost[configModalHost.id] || defaultRoleForMode).map(kind => {
                const cfg = serviceConfigFor(configModalHost.id, kind);
                return (
                  <div className="cd-node-config-editor" key={kind}>
                    <div className="cd-node-config-top">
                      <div style={{ display: 'flex', flexDirection: 'column', gap: '2px', margin: 0 }}>
                        <h3 style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 500, fontSize: '14px', color: '#332849', margin: 0 }}>{configFileName(kind)}</h3>
                        <p style={{ fontFamily: 'Satoshi, sans-serif', fontWeight: 400, fontSize: '14px', color: '#332849', margin: 0 }}>Fill the node-specific values for this service.</p>
                      </div>
                      <div className="cd-config-controls">
                        <label className="cd-heap-field">
                          <span>Heap</span>
                          <input
                            value={cfg.heapSize}
                            onChange={e => updateServiceConfig(configModalHost.id, kind, { heapSize: e.target.value })}
                            placeholder={defaultHeapForKind(kind)}
                          />
                        </label>
                      </div>
                    </div>
                    <div className="cd-node-port-grid" style={{ marginBottom: '12px' }}>
                      {portFieldsForHost(configModalHost.id).filter(field => {
                        if (kind === 'broker') return field.key === 'listenerPort' || field.key === 'brokerJmxPort';
                        if (kind === 'controller') return field.key === 'controllerPort' || field.key === 'controllerJmxPort';
                        if (kind === 'zookeeper') return ['controllerPort', 'zookeeperPeerPort', 'zookeeperElectionPort'].includes(field.key);
                        return true;
                      }).map(field => (
                        <label className="cd-node-port-field" key={field.key}>
                          <span>{field.label}</span>
                          <input
                            type="number"
                            min={1}
                            max={65535}
                            value={getHostPorts(configModalHost.id)[field.key]}
                            onChange={event => updateHostPort(configModalHost.id, field.key, Number(event.target.value))}
                          />
                        </label>
                      ))}
                      {kind === 'zookeeper' ? (
                        <label className="cd-node-port-field read-only">
                          <span>ZooKeeper JMX Port</span>
                          <input type="number" value={7071} readOnly aria-label="ZooKeeper JMX port" />
                        </label>
                      ) : null}
                    </div>
                    {selectedPortValidationErrors.filter(error => error.startsWith(`${configModalHost.hostname}:`)).map(error => (
                      <div className="cd-node-port-error" key={error}>{error.replace(`${configModalHost.hostname}: `, '')}</div>
                    ))}
                    <PropertyTable
                      rows={cfg.rows}
                      hostIp={displayIp(configModalHost)}
                      onUseHostIp={() => updatePropertyValue(configModalHost.id, kind, ipRowKeyForKind(kind), displayIp(configModalHost))}
                      onChange={(key, value) => updatePropertyValue(configModalHost.id, kind, key, value)}
                    />
                  </div>
                );
              })}
              {hostHasAddonRole(configModalHost.id, 'schema_registry') && <SchemaRegistryFields cfg={schemaConfigFor(configModalHost.id)} artifacts={schemaArtifacts} onChange={patch => updateSchemaConfig(configModalHost.id, patch)} />}
              {hostHasAddonRole(configModalHost.id, 'kafka_connect') && <KafkaConnectFields cfg={connectConfigFor(configModalHost.id)} artifacts={connectArtifacts} onChange={patch => updateConnectConfig(configModalHost.id, patch)} />}
            </div>
          </div>
        </div>,
        document.body
      )}
      {commonConfigOpen && createPortal(
        <div className="cd-modal-backdrop" role="dialog" aria-modal="true" onClick={() => setCommonConfigOpen(false)}>
          <div className="cd-config-modal common" onClick={e => e.stopPropagation()}>
            <div className="cd-config-modal-header">
              <div>
                <h2>Common Configuration</h2>
                <p>{deploymentMode === 'kraft' ? 'KRaft' : 'ZooKeeper'} properties shared across selected nodes.</p>
              </div>
              <button className="cd-icon-btn" onClick={() => setCommonConfigOpen(false)} title="Close common configuration">
                <X size={16} />
              </button>
            </div>
            <div className="cd-config-modal-body">
              <div className="cd-config-tabs">
                {commonConfigKinds.map(kind => (
                  <button
                    key={kind}
                    className={commonConfigKind === kind ? 'active' : ''}
                    onClick={() => setCommonConfigKind(kind)}
                  >
                    {configFileName(kind)}
                  </button>
                ))}
              </div>
              <PropertyTable
                rows={commonConfigs[commonConfigKind]}
                hostIp=""
                onUseHostIp={() => { }}
                onChange={(key, value) => updateCommonConfigValue(commonConfigKind, key, value)}
              />
            </div>
            <div className="cd-config-modal-footer" style={{ display: 'flex', justifyContent: 'flex-end', gap: '16px', borderTop: 'none', padding: '16px 24px', boxShadow: '0px -4px 9px rgba(0, 0, 0, 0.1)' }}>
              <button className="cd-secondary-btn" onClick={() => setCommonConfigOpen(false)} style={{ border: '1px solid #8E77BB', color: '#8E77BB', background: '#FFFFFF', borderRadius: '8px', padding: '10px 16px', fontSize: '14px', fontWeight: 500 }}>Cancel</button>
              <button className="cd-primary-btn" onClick={() => setCommonConfigOpen(false)} style={{ background: '#CBC0E0', color: '#FFFFFF', border: 'none', borderRadius: '8px', padding: '10px 16px', fontSize: '14px', fontWeight: 500 }}>Save</button>
            </div>
          </div>
        </div>,
        document.body
      )}
    </div>
  );

  if (onClose) {
    if (stage === 'preview') {
      return (
        <div className="cd-preview-fullscreen-container animate-fade-in">
          {mainContent}
        </div>
      );
    }
    return (
      <div className="cd-modal-backdrop" onMouseDown={onClose}>
        <div className="cd-deployment-modal-container" onMouseDown={e => e.stopPropagation()}>
          <header className="cd-deployment-modal-header">
            <div>
              <h2>Create New Cluster</h2>
              <p>Configure and deploy a Kafka cluster to your hosts</p>
            </div>
            <button className="cd-modal-close-btn" onClick={onClose} title="Close">
              <X size={20} />
            </button>
          </header>
          <div className="cd-deployment-modal-body">
            {mainContent}
          </div>
        </div>
      </div>
    );
  }

  if (!onClose) {
    return (
      <div className="cluster-deployment-page-wrapper" style={{ padding: '24px', backgroundColor: '#F5F6FA', flex: 1, minHeight: 'calc(100vh - 60px)', display: 'flex', flexDirection: 'column', alignItems: 'stretch', width: '100%', boxSizing: 'border-box' }}>
        {mainContent}
      </div>
    );
  }

  return mainContent;
}

function PropertyTable({
  rows,
  hostIp,
  onUseHostIp,
  onChange,
}: {
  rows: PropertyRow[];
  hostIp: string;
  onUseHostIp: () => void;
  onChange: (key: string, value: string) => void;
}) {
  return (
    <div className="cd-property-table-wrap">
      <table className="cd-property-table">
        <thead>
          <tr>
            <th style={{ width: hostIp ? '33.33%' : '64%' }}>Key</th>
            <th style={{ width: hostIp ? '33.33%' : '36%' }}>Value</th>
            {hostIp && <th style={{ width: '33.33%' }}>Action</th>}
          </tr>
        </thead>
        <tbody>
          {rows.map(row => (
            <tr key={row.key} className={row.required && !row.value.trim() ? 'required-missing' : ''}>
              <td>
                <span className="cd-prop-key">{row.key}</span>
                {row.required && <small><b>*</b> Required</small>}
              </td>
              <td>
                <div style={{ display: 'flex', alignItems: 'center', gap: '24px' }}>
                  <input
                    value={row.value}
                    onChange={e => onChange(row.key, e.target.value)}
                    placeholder={row.required ? 'Required before preview' : ''}
                    style={{ flex: 1 }}
                  />
                </div>
              </td>
              {hostIp && (
                <td>
                  <button type="button" onClick={onUseHostIp} style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', height: '40px', boxSizing: 'border-box', background: '#FFFFFF', border: '1px solid #CCCCCC', borderRadius: '8px', padding: '10px 16px', color: '#332849', fontSize: '14px', fontFamily: 'Satoshi, sans-serif', fontWeight: 400, whiteSpace: 'nowrap' }}>
                    Use {hostIp}
                  </button>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function StatusBadge({ status }: { status: PrereqStatus }) {
  const normalized = status || 'IDLE';
  const icon = normalized === 'SUCCESS'
    ? null
    : normalized === 'FAILED'
      ? <XCircle size={13} />
      : normalized === 'REBOOT_REQUIRED'
        ? <AlertTriangle size={13} />
        : normalized === 'RUNNING' || normalized === 'QUEUED'
          ? <Loader2 size={13} className="spin" />
          : null;

  let text;
  if (normalized === 'IDLE') {
    text = 'Idle';
  } else if (normalized === 'SUCCESS') {
    text = 'Success';
  } else if (normalized === 'FAILED') {
    text = 'Failed';
  } else if (normalized === 'RUNNING') {
    text = 'Running';
  } else if (normalized === 'QUEUED') {
    text = 'Queued';
  } else if (normalized === 'REBOOT_REQUIRED') {
    text = 'Reboot Required';
  } else {
    text = normalized;
  }

  return <span className={`cd-status ${normalized.toLowerCase()}`}>{icon}{text}</span>;
}
