import { Fragment, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useParams } from 'react-router-dom';
import { AlertOctagon, Check, ChevronDown, ChevronLeft, ChevronRight, ClipboardPaste, Copy, Edit3, FileDown, FileText, GitCompare, MoreVertical, Paperclip, Plus, RefreshCw, Save, Search, Settings, Trash2, X } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import orangeBanner from '../assets/orange.png';
import { AnchoredMenu } from '../components/AnchoredMenu';
import { readDataServiceSession, writeDataServiceSession } from '../utils/dataServiceSessionCache';
import { compareSchemaFields } from '../utils/schemaFieldDiff';
import { schemaSubmissionError } from '../utils/schemaSubmissionError';
import { SchemaSubjectFilter } from '../components/SchemaSubjectFilter';
import { filterSchemaSubjects, schemaTypes, schemaCompatibilityLevels, subjectCompatibilityLevel } from '../utils/schemaSubjectFilters';
import { SchemaRegistrySettingModal } from '../components/SchemaRegistrySettingModal';
import './DataServiceTabs.css';

interface SchemaSubject {
  subject: string;
  type: string;
  version: number;
  id: number | null;
  schemaType: string;
  schema: string;
  deleted?: boolean;
  compatibility?: string;
}

interface SchemaVersion {
  version: number;
  id: number | null;
  schemaType: string;
  schema: string;
  deleted?: boolean;
}

interface PendingSchemaDelete {
  subject: string;
  version?: number;
  permanent: boolean;
}

interface SubjectDetails {
  subject: string;
  latest: SchemaVersion;
  versions: SchemaVersion[];
  compatibility: string;
}

interface SchemaSummary {
  connection: string;
  subjects: SchemaSubject[];
  totalSubjects: number;
  keySubjects: number;
  valueSubjects: number;
}

type CertificateType = 'PEM' | 'PKCS12';

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
  certificateType?: CertificateType;
  certificateFileName?: string;
}

interface ClusterTruststoreInfo {
  available: boolean;
  certificateType?: CertificateType;
  displayName?: string;
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

const emptySchema = `{
  "type": "record",
  "name": "Example",
  "fields": [
    { "name": "id", "type": "string" }
  ]
}`;

const compatibilityOptions = [
  'BACKWARD',
  'BACKWARD_TRANSITIVE',
  'FORWARD',
  'FORWARD_TRANSITIVE',
  'FULL',
  'FULL_TRANSITIVE',
  'NONE'
];

const errorMessage = (error: unknown, fallback: string): string =>
  error instanceof Error && error.message ? error.message : fallback;

/** Unwraps Schema Registry responses that contain an encoded schema string. */
const schemaSource = (schema: unknown, fallback = '{}'): string => {
  if (schema === null || schema === undefined || schema === '') return fallback;

  let source = typeof schema === 'string' ? schema : JSON.stringify(schema);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const parsed = JSON.parse(source);
      if (typeof parsed !== 'string') return JSON.stringify(parsed);
      source = parsed;
    } catch {
      break;
    }
  }
  return source;
};

/**
 * Protobuf is not JSON, so it needs a small syntax-aware formatter. Quoted
 * strings and comments are copied verbatim while braces and semicolons control
 * indentation. If the input is already formatted, the result remains stable.
 */
const formatProtobuf = (source: string): string => {
  const lines: string[] = [];
  let current = '';
  let indent = 0;
  let quote = '';
  let escaped = false;

  const append = (text: string) => { current += text; };
  const flush = () => {
    const value = current.trim();
    if (value) lines.push(`${'  '.repeat(Math.max(0, indent))}${value}`);
    current = '';
  };
  const nextNonWhitespace = (from: number) => {
    let cursor = from;
    while (cursor < source.length && /\s/.test(source[cursor])) cursor += 1;
    return source[cursor] || '';
  };

  for (let index = 0; index < source.length; index += 1) {
    const char = source[index];
    const next = source[index + 1] || '';

    if (quote) {
      append(char);
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === quote) quote = '';
      continue;
    }

    if (char === '"' || char === "'") {
      quote = char;
      append(char);
      continue;
    }

    if (char === '/' && next === '/') {
      flush();
      let comment = '//';
      index += 2;
      while (index < source.length && source[index] !== '\n' && source[index] !== '\r') {
        comment += source[index];
        index += 1;
      }
      lines.push(`${'  '.repeat(Math.max(0, indent))}${comment.trimEnd()}`);
      continue;
    }

    if (char === '/' && next === '*') {
      flush();
      let comment = '/*';
      index += 2;
      while (index < source.length) {
        comment += source[index];
        if (source[index] === '*' && source[index + 1] === '/') {
          comment += '/';
          index += 1;
          break;
        }
        index += 1;
      }
      comment.split(/\r?\n/).forEach(line => {
        if (line.trim()) lines.push(`${'  '.repeat(Math.max(0, indent))}${line.trim()}`);
      });
      continue;
    }

    if (char === '{') {
      if (current && !/\s$/.test(current)) append(' ');
      append('{');
      flush();
      indent += 1;
      continue;
    }

    if (char === '}') {
      flush();
      indent = Math.max(0, indent - 1);
      append('}');
      if (nextNonWhitespace(index + 1) !== ';') flush();
      continue;
    }

    if (char === ';') {
      append(';');
      flush();
      continue;
    }

    if (/\s/.test(char)) {
      if (current && !/\s$/.test(current)) append(' ');
      continue;
    }

    append(char);
  }
  flush();
  return lines.join('\n') || source;
};

/** Formats Avro/JSON Schema as JSON and Protobuf as native proto syntax. */
const formatSchema = (schema: unknown, schemaType?: string, fallback = '{}'): string => {
  const source = schemaSource(schema, fallback);
  if ((schemaType || '').toUpperCase() === 'PROTOBUF') return formatProtobuf(source);

  try {
    const parsed = JSON.parse(source);
    if (typeof parsed === 'string') {
      try {
        return JSON.stringify(JSON.parse(parsed), null, 2);
      } catch {
        return parsed;
      }
    }
    return JSON.stringify(parsed, null, 2);
  } catch {
    return source;
  }
};

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
        <svg className={`ds-custom-select-arrow ${isOpen ? 'open' : ''}`} xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#A1A1AA" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m6 9 6 6 6-6" /></svg>
      </div>

      {isOpen && anchor && (
        <AnchoredMenu
          anchor={anchor}
          className={`ds-custom-select-dropdown${className === 'ds-compat-select' ? ' ds-sr-global-compat-menu' : ''}`}
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

type View = 'list' | 'detail' | 'edit';
interface PendingSetting {
  kind: 'compatibility' | 'mode' | 'edit-compatibility';
  subject?: string; oldValue: string; value: string; connectionId: string | null;
}
const modeOptions = [{ value: 'READONLY', label: 'Read only' }, { value: 'READWRITE', label: 'Read / Write' }];

export function SchemaRegistry() {
  const { id } = useParams<{ id: string }>();
  const { canManage } = usePermissions();
  const [initialSession] = useState(() => readDataServiceSession<SchemaSummary>('schema-registry', id));
  const [view, setView] = useState<View>('list');
  const [summary, setSummary] = useState<SchemaSummary | null>(initialSession?.summary ?? null);
  const [loading, setLoading] = useState(false);
  const [hasFetched, setHasFetched] = useState(initialSession?.hasFetched ?? false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [pendingSetting, setPendingSetting] = useState<PendingSetting | null>(null);
  const [settingError, setSettingError] = useState<string | null>(null);
  const [globalModeState, setGlobalModeState] = useState<{ context: string; value: string | null; error: string | null }>({ context: '', value: null, error: null });
  const [subjectModeState, setSubjectModeState] = useState<{ context: string; value: string | null; error: string | null }>({ context: '', value: null, error: null });
  const [showConnection, setShowConnection] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [pendingDelete, setPendingDelete] = useState<PendingSchemaDelete | null>(null);
  const [deleteConfirmation, setDeleteConfirmation] = useState('');
  const [showDeletedSubjects, setShowDeletedSubjects] = useState(false);
  const [subjectFilters, setSubjectFilters] = useState({ context: '', search: '', types: [] as string[], compatibility: [] as string[] });
  const [showDeletedVersions, setShowDeletedVersions] = useState(false);
  const [selected, setSelected] = useState<SchemaSubject | null>(null);
  const [details, setDetails] = useState<SubjectDetails | null>(null);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [showCompare, setShowCompare] = useState(false);
  const [compareVersionA, setCompareVersionA] = useState<number | null>(null);
  const [compareVersionB, setCompareVersionB] = useState<number | null>(null);
  const [expandedVersions, setExpandedVersions] = useState<Set<number>>(new Set());
  const [copiedText, setCopiedText] = useState<string | null>(null);

  const handleCopy = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedText(text);
      setTimeout(() => setCopiedText(null), 2000);
    } catch (err) {
      console.error('Failed to copy text: ', err);
    }
  };

  // Edit form state
  const [editSchemaType, setEditSchemaType] = useState('AVRO');
  const [editCompatibility, setEditCompatibility] = useState('BACKWARD');
  const [newSchema, setNewSchema] = useState(emptySchema);

  // Create schema modal state
  const [showCreate, setShowCreate] = useState(false);
  const [createSubject, setCreateSubject] = useState('');
  const [createSchemaType, setCreateSchemaType] = useState('AVRO');
  const [createSchema, setCreateSchema] = useState(emptySchema);

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Multi-instance state ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  const [savedConnections, setSavedConnections] = useState<SavedConnection[]>([]);
  const [clusterTruststore, setClusterTruststore] = useState<ClusterTruststoreInfo | null>(null);
  const [selectedConnectionId, setSelectedConnectionId] = useState<string | null>(initialSession?.selectedConnectionId ?? null);
  const loadRequestId = useRef(0);
  const modeContext = `${id}:${selectedConnectionId}`;
  const selectedSubject = selected?.subject;
  const subjectModeContext = `${modeContext}:${selectedSubject}`;
  const globalMode = globalModeState.context === modeContext ? globalModeState.value : null;
  const subjectMode = subjectModeState.context === subjectModeContext ? subjectModeState.value : null;
  const modeError = globalModeState.context === modeContext ? globalModeState.error : null;
  const subjectModeError = subjectModeState.context === subjectModeContext ? subjectModeState.error : null;


  // Connection form state
  const [formConnectionName, setFormConnectionName] = useState('');
  const [customIp, setCustomIp] = useState('');
  const [customPort, setCustomPort] = useState('');
  const [protocol, setProtocol] = useState('http');
  const [certType, setCertType] = useState<CertificateType>('PEM');
  const [certFile, setCertFile] = useState<File | null>(null);
  const [certFileName, setCertFileName] = useState('');
  const [certPassword, setCertPassword] = useState('');
  const [certPasteMode, setCertPasteMode] = useState(false);
  const [certPasteText, setCertPasteText] = useState('');
  const [formIsDefault, setFormIsDefault] = useState(false);
  /** ID of the connection being edited ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â set when editing an existing connection. */
  const [editingConnectionId, setEditingConnectionId] = useState<string | null>(null);
  const [globalCompatibility, setGlobalCompatibility] = useState(initialSession?.metadata?.globalCompatibility ?? 'BACKWARD');
  const [subjectCompatibility, setSubjectCompatibility] = useState('BACKWARD');
  const [appliedGlobalCompatibility, setAppliedGlobalCompatibility] = useState(initialSession?.metadata?.globalCompatibility ?? 'BACKWARD');
  const [connectSaving, setConnectSaving] = useState(false);
  const [connectError, setConnectError] = useState<string | null>(null);

  // Derived: currently selected connection object
  const selectedConn = useMemo(
    () => savedConnections.find(c => c.id === selectedConnectionId) ?? null,
    [savedConnections, selectedConnectionId]
  );

  useEffect(() => {
    if (!id) return;
    const controller = new AbortController();
    fetch(`/api/v1/clusters/${id}/data-services/cluster-truststore`, { signal: controller.signal })
      .then(response => response.ok ? response.json() : null)
      .then(info => { if (!controller.signal.aborted) setClusterTruststore(info); })
      .catch(() => { if (!controller.signal.aborted) setClusterTruststore(null); });
    return () => controller.abort();
  }, [id]);

  const comparableVersions = useMemo(() => {
    const versions = [...(details?.versions || [])];
    const latest = details?.latest;

    if (latest && !versions.some(version => version.version === latest.version)) {
      versions.push(latest);
    }

    return versions
      .filter(version => Number.isFinite(version.version))
      .sort((a, b) => b.version - a.version);
  }, [details]);
  const filters = subjectFilters.context === modeContext ? subjectFilters : { search: '', types: [], compatibility: [] };
  const updateSubjectFilters = (patch: Partial<typeof filters>) => setSubjectFilters({ ...filters, ...patch, context: modeContext });
  const scopeSubjects = filterSchemaSubjects(summary?.subjects || [], '', [], [], showDeletedSubjects);
  const visibleSubjects = filterSchemaSubjects(scopeSubjects, filters.search, filters.types, filters.compatibility);
  const typeCounts: Record<string, number> = {};
  const compatibilityCounts: Record<string, number> = {};
  for (const subject of filterSchemaSubjects(scopeSubjects, filters.search, [], filters.compatibility)) {
    const type = subject.schemaType.toUpperCase(); typeCounts[type] = (typeCounts[type] || 0) + 1;
  }
  for (const subject of filterSchemaSubjects(scopeSubjects, filters.search, filters.types, [])) {
    const level = subjectCompatibilityLevel(subject); compatibilityCounts[level] = (compatibilityCounts[level] || 0) + 1;
  }
  const availableTypes = Array.from(new Set([...schemaTypes, ...scopeSubjects.map(subject => subject.schemaType.toUpperCase())]));
  const availableCompatibility = Array.from(new Set([...schemaCompatibilityLevels, ...scopeSubjects.map(subjectCompatibilityLevel)]));
  const filtersActive = Boolean(filters.search || filters.types.length || filters.compatibility.length);
  const visibleVersions = (details?.versions || []).filter(version => showDeletedVersions || !version.deleted);

  const switchDeletedSubjects = (show: boolean) => {
    if (show === showDeletedSubjects) return;
    setShowDeletedSubjects(show);
    setSummary(null);
    void load(selectedConnectionId, undefined, show);
  };

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Cert helpers ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬

  /**
   * Safely appends ?connectionId=... to any URL using URLSearchParams.
   * Works even if the base URL already contains query params (avoids unsafe string concatenation).
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

  const readFileAsText = (file: File): Promise<string> => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ''));
    reader.onerror = reject;
    reader.readAsText(file);
  });


  const safeBase64Encode = (str: string): string => {
    try {
      return btoa(str);
    } catch {
      // fallback for unicode
      return btoa(encodeURIComponent(str).replace(/%([0-9A-F]{2})/g, (_, p1) => String.fromCharCode(parseInt(p1, 16))));
    }
  };

  /** Build certificate data for PUT /connection body. */
  const buildCertData = async (): Promise<string | undefined> => {
    if (certType === 'PEM') {
      if (certPasteMode && certPasteText.trim()) return safeBase64Encode(certPasteText.trim());
      if (!certPasteMode && certFile) return safeBase64Encode(await readFileAsText(certFile));
    } else if (certType === 'PKCS12' && certFile) {
      return await readFileAsBase64(certFile);
    }
    return undefined;
  };

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Data fetching ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬

  /** Load all saved SR connections for the instance switcher. */
  const loadConnections = useCallback(async () => {
    try {
      const res = await fetch(`/api/v1/clusters/${id}/data-services/schema-registry/connections`);
      if (!res.ok) return;
      const data: SavedConnection[] = await res.json().catch(() => []);
      setSavedConnections(data);
      if (data.length > 0) {
        const defaultConn = data.find(c => c.isDefault) ?? data[0];
        setSelectedConnectionId(prev => prev ?? defaultConn.id);
      }
    } catch { /* non-fatal */ }
  }, [id]);

  /** Open connection modal, optionally pre-filling from an existing connection. */
  const openConnectionModal = (conn?: SavedConnection) => {
    if (!canManage) return;
    if (conn) {
      setEditingConnectionId(conn.id);
      setFormConnectionName(conn.connectionName);
      setProtocol(conn.protocol || 'http');
      setCustomIp(conn.host || '');
      setCustomPort(conn.port ? String(conn.port) : '');
      setCertType(conn.certificateType || clusterTruststore?.certificateType || 'PEM');
      setFormIsDefault(conn.isDefault);
    } else {
      setEditingConnectionId(null);
      setFormConnectionName('');
      setProtocol('http');
      setCustomIp('');
      setCustomPort('');
      setCertType(clusterTruststore?.certificateType || 'PEM');
      setFormIsDefault(false);
    }
    setCertFile(null); setCertFileName(''); setCertPasteText(''); setCertPasteMode(false); setCertPassword('');
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
        port: parseInt(customPort.trim()) || 8081,
        certificateType: protocol === 'https' ? certType : null,
        certificateData: protocol === 'https' ? certData : null,
        certificateFileName: protocol === 'https' && certData ? certFile?.name : undefined,
        truststorePassword: protocol === 'https' ? (certPassword || null) : null,
        isDefault: formIsDefault
      };

      // Use PUT /connections/{id} when editing existing (prevents rename creating duplicate rows).
      // Use PUT /connection (upsert-by-name) when creating new.
      const url = editingConnectionId
        ? `/api/v1/clusters/${id}/data-services/schema-registry/connections/${editingConnectionId}`
        : `/api/v1/clusters/${id}/data-services/schema-registry/connection`;

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

  const handleDeleteConnection = () => {
    if (!canManage || !selectedConnectionId) return;
    setShowDeleteConfirm(true);
  };

  const confirmDeleteConnection = async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/v1/clusters/${id}/data-services/schema-registry/connections/${selectedConnectionId}`, {
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

  const loadGlobalCompatibility = async (
    connectionId: string | null = selectedConnectionId,
    requestId: number = loadRequestId.current
  ): Promise<string> => {
    try {
      const res = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/config`, connectionId));
      const data = await res.json().catch(() => ({}));
      const compatibility = res.ok
        ? data.compatibilityLevel || data.compatibility || 'BACKWARD'
        : 'BACKWARD';
      if (requestId === loadRequestId.current) { setGlobalCompatibility(compatibility); setAppliedGlobalCompatibility(compatibility); }
      return compatibility;
    } catch {
      if (requestId === loadRequestId.current) setGlobalCompatibility('BACKWARD');
      return 'BACKWARD';
    }
  };

  const load = async (
    connectionId: string | null = selectedConnectionId,
    discovered?: DiscoveredConnection,
    includeDeleted: boolean = showDeletedSubjects
  ): Promise<boolean> => {
    const requestId = ++loadRequestId.current;
    setHasFetched(true);
    setLoading(true);
    setError(null);

    try {
      let url = withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/summary`, connectionId);
      if (includeDeleted) url += `${url.includes('?') ? '&' : '?'}includeDeleted=true`;
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
      if (!res.ok) throw new Error(data.message || 'Failed to load Schema Registry.');
      if (requestId !== loadRequestId.current) return false;
      setSummary(data);
      const compatibility = await loadGlobalCompatibility(connectionId, requestId);
      if (requestId !== loadRequestId.current) return false;

      // Persist the successful response directly as well as through the layout
      // effect below. This makes the fetched snapshot durable even if the user
      // changes routes as soon as the response is painted.
      writeDataServiceSession('schema-registry', id, {
        selectedConnectionId: connectionId,
        summary: data,
        hasFetched: true,
        metadata: { globalCompatibility: compatibility }
      });
      return true;
    } catch (e: unknown) {
      if (requestId === loadRequestId.current) setError(errorMessage(e, 'Failed to load Schema Registry.'));
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
    setPendingSetting(null); setSuccess(null); setSelected(null); setView('list');
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
    setCustomPort(discovered.port ? String(discovered.port) : '8081');
    setFormIsDefault(true);
    setCertType(existing?.certificateType || clusterTruststore?.certificateType || 'PEM');
    setCertFile(null);
    setCertFileName('');
    setCertPasteText('');
    setCertPasteMode(false);
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
      const response = await fetch(`/api/v1/clusters/${id}/data-services/schema-registry/discover`);
      const discovered: DiscoveredConnection = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(discovered.message || 'Failed to detect Schema Registry.');

      if (!discovered.detected) {
        setHasFetched(false);
        if (canManage) prefillDiscoveredConnection(
          discovered,
          savedConnections.find(connection => connection.id === existingId)
        );
        else setError(discovered.message || 'No Schema Registry endpoint could be detected.');
        return;
      }

      if (canManage) {
        const existing = savedConnections.find(connection => connection.id === existingId);
        const saveUrl = existingId
          ? `/api/v1/clusters/${id}/data-services/schema-registry/connections/${existingId}`
          : `/api/v1/clusters/${id}/data-services/schema-registry/connection`;
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
        if (!saveResponse.ok) throw new Error(saved.message || 'Detected Schema Registry, but could not save the connection.');
        setSelectedConnectionId(saved.id);
        await loadConnections();
        await load(saved.id);
      } else {
        await load(null, discovered);
      }
    } catch (e: unknown) {
      setHasFetched(false);
      setError(errorMessage(e, 'Failed to detect Schema Registry.'));
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
    writeDataServiceSession('schema-registry', id, {
      selectedConnectionId,
      summary,
      hasFetched,
      metadata: { globalCompatibility }
    });
  }, [globalCompatibility, hasFetched, id, selectedConnectionId, summary]);

  useEffect(() => {
    const abort = new AbortController();
    if (!summary) return () => abort.abort();
    const readMode = async (subject?: string) => {
      try {
        const path = subject ? `/subjects/${encodeURIComponent(subject)}/mode` : '/mode';
        const query = selectedConnectionId ? `?${new URLSearchParams({ connectionId: selectedConnectionId })}` : '';
        const response = await fetch(`/api/v1/clusters/${id}/data-services/schema-registry${path}${query}`, { signal: abort.signal });
        const data = await response.json();
        if (!response.ok || !data.mode) throw new Error(data.message || 'Unable to read Schema Registry mode.');
        if (!abort.signal.aborted) {
          if (subject) setSubjectModeState({ context: subjectModeContext, value: data.mode, error: null });
          else setGlobalModeState({ context: modeContext, value: data.mode, error: null });
        }
      } catch (reason) {
        if (!abort.signal.aborted) {
          const message = errorMessage(reason, 'Unable to read Schema Registry mode.');
          if (subject) setSubjectModeState({ context: subjectModeContext, value: null, error: message });
          else setGlobalModeState({ context: modeContext, value: null, error: message });
        }
      }
    };
    void readMode();
    if (selectedSubject) void readMode(selectedSubject);
    return () => abort.abort();
  }, [id, selectedConnectionId, selectedSubject, summary, modeContext, subjectModeContext]);

  const requestSetting = (kind: PendingSetting['kind'], value: string, subject?: string) => {
    if (!canManage || saving) return;
    const oldValue = kind === 'mode' ? (subject ? subjectMode : globalMode)
      : subject ? details?.compatibility : appliedGlobalCompatibility;
    if (!oldValue || oldValue === value) return;
    setSettingError(null); setSuccess(null);
    setPendingSetting({ kind, value, subject, oldValue, connectionId: selectedConnectionId });
  };

  const openSubject = async (item: SchemaSubject) => {
    setSelected(item);
    setView('detail');
    setDetails(null);
    setLoadingDetails(true);
    setError(null);
    setExpandedVersions(new Set());
    setShowDeletedVersions(Boolean(item.deleted));

    try {
      const detailUrl = withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(item.subject)}/details`);
      const res = await fetch(`${detailUrl}${detailUrl.includes('?') ? '&' : '?'}includeDeleted=true`);
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || 'Failed to load subject details.');
      setDetails(data);
      setSubjectCompatibility(data.compatibility || globalCompatibility);
    } catch (e: unknown) {
      setDetails({
        subject: item.subject,
        latest: { version: item.version, id: item.id, schemaType: item.schemaType, schema: item.schema, deleted: item.deleted },
        versions: [{ version: item.version, id: item.id, schemaType: item.schemaType, schema: item.schema, deleted: item.deleted }],
        compatibility: globalCompatibility
      });
      setSubjectCompatibility(globalCompatibility);
      setError(errorMessage(e, 'Failed to load subject details.'));
    } finally {
      setLoadingDetails(false);
    }
  };

  const openEdit = () => {
    if (!canManage) return;
    const latest = details?.latest;
    if (!selected || !latest) return;
    setEditSchemaType(latest.schemaType || 'AVRO');
    setEditCompatibility(subjectCompatibility);
    setNewSchema(formatSchema(latest.schema || emptySchema, latest.schemaType));
    setView('edit');
  };

  const backToList = () => {
    setView('list');
    setSelected(null);
    setDetails(null);
    setExpandedVersions(new Set());
    setError(null);
  };

  const backToDetail = () => {
    setView('detail');
    setError(null);
  };

  const openCompare = () => {
    if (comparableVersions.length < 2) {
      setError('At least two schema versions are required to compare.');
      return;
    }
    setError(null);
    setCompareVersionA(comparableVersions[0].version);
    setCompareVersionB(comparableVersions[1].version);
    setShowCompare(true);
  };

  const comparedSchemaA = comparableVersions.find(v => v.version === compareVersionA);
  const comparedSchemaB = comparableVersions.find(v => v.version === compareVersionB);
  const olderSchema = comparedSchemaA && comparedSchemaB
    ? (comparedSchemaA.version < comparedSchemaB.version ? comparedSchemaA : comparedSchemaB)
    : null;
  const newerSchema = comparedSchemaA && comparedSchemaB
    ? (comparedSchemaA.version > comparedSchemaB.version ? comparedSchemaA : comparedSchemaB)
    : null;
  const schemaComparison = olderSchema && newerSchema
    ? compareSchemaFields(olderSchema.schema, newerSchema.schema, olderSchema.schemaType, newerSchema.schemaType)
    : null;
  const schemasAreIdentical = schemaComparison?.identical ?? false;

  const toggleVersion = (version: number) => {
    setExpandedVersions(prev => {
      const next = new Set(prev);
      if (next.has(version)) next.delete(version);
      else next.add(version);
      return next;
    });
  };

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Actions ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  const submitCreateSchema = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!canManage) return;
    if (!createSubject.trim() || !createSchema.trim()) return;
    setSaving(true);
    setError(null);
    try {
      const res = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(createSubject.trim())}/versions`), {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ schemaType: createSchemaType, schema: createSchema })
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(schemaSubmissionError(data, 'Failed to create schema.'));
      setShowCreate(false);
      setCreateSubject('');
      setCreateSchema(emptySchema);
      setCreateSchemaType('AVRO');
      await load();
    } catch (e: unknown) {
      setError(errorMessage(e, 'Failed to create schema.'));
    } finally {
      setSaving(false);
    }
  };

  const submitEditSchema = async (e?: React.FormEvent, confirmed = false) => {
    e?.preventDefault();
    if (!canManage) return;
    if (!selected || !newSchema.trim()) return;
    if (!confirmed && editCompatibility !== (details?.compatibility ?? subjectCompatibility)) {
      requestSetting('edit-compatibility', editCompatibility, selected.subject); return;
    }
    setSaving(true);
    setError(null);
    try {
      const res = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(selected.subject)}/versions`), {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ schemaType: editSchemaType, schema: newSchema })
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(schemaSubmissionError(data, 'Failed to update schema.'));
      // Also save compatibility if changed
      if (editCompatibility !== (details?.compatibility ?? subjectCompatibility)) {
        const compatibilityResponse = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(selected.subject)}/config`), {
          method: 'PUT', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ compatibility: editCompatibility })
        });
        const compatibilityData = await compatibilityResponse.json().catch(() => ({}));
        if (!compatibilityResponse.ok) throw new Error(compatibilityData.message || 'Schema was saved, but compatibility could not be updated.');
        setSubjectCompatibility(editCompatibility);
        setSuccess(`Subject ${selected.subject}: compatibility changed from ${details?.compatibility ?? subjectCompatibility} to ${editCompatibility}.`);
      }
      await load();
      // Reload detail
      const refreshed = summary?.subjects.find(s => s.subject === selected.subject) || selected;
      await openSubject(refreshed);
      setPendingSetting(null);
    } catch (e: unknown) {
      const message = errorMessage(e, 'Failed to update schema.');
      setError(message); if (confirmed) setSettingError(message);
    } finally {
      setSaving(false);
    }
  };

  const requestSchemaDelete = (subject: string, permanent: boolean, version?: number) => {
    if (!canManage) return;
    setError(null);
    setDeleteConfirmation('');
    setPendingDelete({ subject, version, permanent });
  };

  const confirmSchemaDelete = async () => {
    if (!canManage || !pendingDelete) return;
    const { subject, version, permanent } = pendingDelete;
    if (permanent && deleteConfirmation !== (version ? `${subject}/${version}` : subject)) return;
    setSaving(true);
    setError(null);
    try {
      const path = `/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(subject)}`
        + (version ? `/versions/${version}` : '');
      let url = withConnId(path);
      if (permanent) url += `${url.includes('?') ? '&' : '?'}permanent=true`;
      const res = await fetch(url, {
        method: 'DELETE'
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || `Failed to ${permanent ? 'permanently delete' : 'soft-delete'} ${version ? 'version' : 'subject'}.`);
      setPendingDelete(null);
      if (version && selected?.subject === subject) {
        await openSubject(selected);
        setShowDeletedVersions(true);
        await load(selectedConnectionId, undefined, true);
      } else {
        backToList();
        if (!permanent) setShowDeletedSubjects(true);
        await load(selectedConnectionId, undefined, !permanent || showDeletedSubjects);
      }
    } catch (e: unknown) {
      setError(errorMessage(e, 'Failed to delete schema.'));
    } finally {
      setSaving(false);
    }
  };

  const confirmSetting = async () => {
    const change = pendingSetting;
    if (!change || !canManage || saving) return;
    if (change.kind === 'edit-compatibility') { await submitEditSchema(undefined, true); return; }
    setSaving(true); setSettingError(null); setError(null);
    try {
      const scopePath = change.subject ? `/subjects/${encodeURIComponent(change.subject)}` : '';
      const path = scopePath + (change.kind === 'mode' ? '/mode' : '/config');
      const response = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry${path}`, change.connectionId), {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [change.kind]: change.value })
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(data.message || `Failed to update ${change.kind}.`);
      if (change.kind === 'mode') {
        if (change.subject) setSubjectModeState({ context: subjectModeContext, value: change.value, error: null });
        else setGlobalModeState({ context: modeContext, value: change.value, error: null });
      } else if (change.subject) {
        setSubjectCompatibility(change.value);
        setDetails(previous => previous ? { ...previous, compatibility: change.value } : previous);
        setSummary(previous => previous ? { ...previous, subjects: previous.subjects.map(subject =>
          subject.subject === change.subject ? { ...subject, compatibility: change.value } : subject) } : previous);
      } else { setGlobalCompatibility(change.value); setAppliedGlobalCompatibility(change.value); }
      setSuccess(`${change.subject ? `Subject ${change.subject}` : 'Global'}: ${change.kind} changed from ${change.oldValue} to ${change.value}.`);
      setPendingSetting(null);
    } catch (reason) { setSettingError(errorMessage(reason, 'Unable to save Schema Registry setting.')); }
    finally { setSaving(false); }
  };

  const connStatusColor = (s: string) =>
    s === 'ONLINE' ? '#80e8a2' : (s === 'OFFLINE' || s === 'ERROR') ? '#e88080' : '#a8c5c0';

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Render ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  return (
    <div className="data-services-page animate-fade-in">
      {success && <div className="ds-settings-success" role="status">{success}<button aria-label="Dismiss notification" onClick={() => setSuccess(null)}><X size={16} /></button></div>}
      {pendingSetting && <SchemaRegistrySettingModal
        title={`Change ${pendingSetting.kind === 'mode' ? 'mode' : 'compatibility'}?`}
        description={pendingSetting.kind === 'mode'
          ? pendingSetting.value === 'READONLY' ? 'Read only mode prevents schema registration and deletion in this scope.' : 'Read / Write mode allows schema registration and deletion in this scope.'
          : 'Change schema compatibility rules for future registrations? Existing schemas are retained.'}
        detail={`${pendingSetting.subject ? `Subject: ${pendingSetting.subject}` : 'Global setting'} · ${pendingSetting.oldValue} → ${pendingSetting.value}${pendingSetting.kind === 'edit-compatibility' ? ' · The new schema version will also be submitted.' : ''}`}
        saving={saving} error={settingError} onClose={() => { setPendingSetting(null); setSettingError(null); }}
        onConfirm={() => void confirmSetting()} />}


      {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ LIST VIEW ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
      {view === 'list' && (
        <>
          <div className="ds-header ds-sr-header">
            <div className="ds-actions">
              <div className="ds-selectors-group">
                {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Instance Selector ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
                <div className="ds-compat-control" style={{ display: 'flex', flexDirection: 'column', gap: '6px', alignItems: 'flex-start' }}>
                  <span style={{ fontSize: 'var(--text-sm)', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Instance Selector</span>
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
                            label: c.isDefault ? 'Default connection' : c.connectionName
                          }))
                          : [{ value: '', label: 'Default connection' }]
                      }
                    />

                  </div>
                </div>

                {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Global Compatibility Selector ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
                <div className="ds-compat-control" style={{ display: 'flex', flexDirection: 'column', gap: '6px', alignItems: 'flex-start' }}>
                  <span style={{ fontSize: 'var(--text-sm)', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Global Compatibility Selector</span>
                  <CustomSelect
                    className="ds-compat-select"
                    value={globalCompatibility}
                    onChange={setGlobalCompatibility}
                    disabled={!canManage || saving}
                    options={compatibilityOptions.map(o => ({
                      value: o,
                      label: o.charAt(0) + o.slice(1).toLowerCase().replace('_', ' ')
                    }))}
                  />
                </div>
              </div>

              <div className="ds-compat-control ds-mode-control">
                <span>Global mode</span>
                <CustomSelect value={globalMode ?? ''} placeholder="Mode unavailable" options={modeOptions}
                  disabled={!canManage || saving || !globalMode || !summary} onChange={value => requestSetting('mode', value)} />
                {modeError && <span className="ds-settings-error" role="alert">{modeError}</span>}
              </div>
              <div className="ds-buttons-group">
                {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Buttons ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
                {canManage && (
                  <button
                    className="ds-sr-save-button"
                    onClick={() => requestSetting('compatibility', globalCompatibility)}
                    disabled={saving}
                    style={{
                      boxSizing: 'border-box',
                      display: 'inline-flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      padding: '0 8px',
                      gap: 'var(--space-2)',
                      height: '32px',
                      background: "var(--bg-surface)",
                      border: '1px solid var(--button-primary)',
                      borderRadius: 'var(--radius-md)',
                      color: 'var(--button-primary)',
                      fontFamily: 'Satoshi, sans-serif',
                      fontWeight: 'var(--font-medium)',
                      fontSize: 'var(--text-base)',
                      cursor: 'pointer',
                      transition: 'all 0.2s'
                    }}
                  >
                    <Save size={16} /> Save
                  </button>
                )}

                {canManage && (
                  <button
                    className="ds-button ds-kafka-connect-action-button"
                    onClick={() => openConnectionModal()}
                  >
                    <Settings size={16} /> Add Connection
                  </button>
                )}

                {canManage && (
                  <button className="ds-button create-schema" onClick={() => setShowCreate(true)}>
                    <Plus size={16} /> Create Schema
                  </button>
                )}

                {canManage && (
                  <button className="ds-icon-button icon-gray" onClick={handleDeleteConnection} disabled={!selectedConn} title="Delete connection" style={{ width: '35px', height: '35px' }}>
                    <Trash2 size={16} />
                  </button>
                )}

                <button className="ds-icon-button icon-gray" onClick={() => void fetchWithDiscovery()} disabled={loading} title="Refresh" style={{ width: '35px', height: '35px' }}>
                  <RefreshCw size={16} className={loading ? 'spin' : ''} />
                </button>

                {canManage && (
                  <button className="ds-icon-button icon-gray" onClick={() => selectedConn && openConnectionModal(selectedConn)} disabled={!selectedConn} title="Edit connection" style={{ width: '35px', height: '35px' }}>
                    <MoreVertical size={16} />
                  </button>
                )}
              </div>
            </div>
          </div>

          {error && <div className="ds-alert">{error}</div>}

          {!hasFetched ? (
            <div className="ds-fetch-prompt ds-sr-fetch-prompt">
              <p style={{ margin: 0, fontFamily: 'Satoshi, sans-serif', fontSize: 'var(--text-md)', fontWeight: 'var(--font-regular)', color: 'var(--text-tertiary)' }}>
                Schema Registry data is not loaded automatically.
              </p>
              <button
                className="ds-sr-fetch-button"
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
                  transition: 'all 0.2s'
                }}
              >
                <RefreshCw size={14} className={loading ? 'spin' : ''} />
                {loading ? 'Fetching Schema Registry...' : 'Fetch Schema Registry for this cluster'}
              </button>
            </div>
          ) : <>
            <div className="ds-metrics ds-sr-metrics">
              <div className="ds-metric-card"><span>Total Subjects</span><strong>{scopeSubjects.length}</strong></div>
              <div className="ds-metric-card"><span>Value Subjects</span><strong>{scopeSubjects.filter(subject => subject.type === 'VALUE').length}</strong></div>
              <div className="ds-metric-card"><span>Key Subjects</span><strong>{scopeSubjects.filter(subject => subject.type === 'KEY').length}</strong></div>
              <div className="ds-metric-card">
                <span>REST Endpoint</span>
                <strong className="ds-sr-endpoint-value">
                  {summary?.connection ? (
                    <a
                      href={summary.connection}
                      target="_blank"
                      rel="noopener noreferrer"

                    >
                      {summary.connection}
                    </a>
                  ) : (
                    '-'
                  )}
                </strong>
              </div>
            </div>

            <div className="ds-panel ds-sr-subjects-panel">
              <div className="ds-subject-filters ds-schema-subject-filters" role="group" aria-label="Filter schema subjects">
                <label className="ds-subject-search"><Search size={18} /><input type="search" aria-label="Search subjects by name or schema ID"
                  placeholder="Filter by subject name or schema ID" value={filters.search} onChange={event => updateSubjectFilters({ search: event.target.value })} /></label>
                <SchemaSubjectFilter label="Type" options={availableTypes.map(value => ({ value, label: ({ AVRO: 'Avro', PROTOBUF: 'Protobuf', JSON: 'JSON' } as Record<string, string>)[value] || value }))}
                  selected={filters.types} counts={typeCounts} onChange={types => updateSubjectFilters({ types })} />
                <SchemaSubjectFilter label="Compatibility" options={availableCompatibility.map(value => ({ value, label: value === 'UNKNOWN' ? 'Unavailable' : value }))}
                  selected={filters.compatibility} counts={compatibilityCounts} onChange={compatibility => updateSubjectFilters({ compatibility })} />
                <label className="ds-subject-deleted"><input type="checkbox" checked={showDeletedSubjects} onChange={event => switchDeletedSubjects(event.target.checked)} /> Show only soft-deleted</label>
                {filtersActive && <button type="button" className="ds-subject-filter-reset" onClick={() => updateSubjectFilters({ search: '', types: [], compatibility: [] })}>Clear filters</button>}
                <span className="ds-subject-results" role="status">{visibleSubjects.length} of {scopeSubjects.length} subjects</span>
              </div>
              <table className="ds-table ds-sr-subjects-table">
                <thead>
                  <tr><th>Subject</th><th>Type</th><th>Latest Version</th><th>Schema ID</th><th>Schema Type</th><th>Compatibility</th>{canManage && <th>Actions</th>}</tr>
                </thead>
                <tbody>
                  {loading && !summary ? (
                    <tr><td colSpan={canManage ? 7 : 6} className="ds-empty">Loading schemas...</td></tr>
                  ) : visibleSubjects.length > 0 ? (
                    visibleSubjects.map(item => (
                      <tr key={item.subject} className="ds-hoverable-row" onClick={() => openSubject(item)} style={{ cursor: 'pointer' }}>
                        <td><span className="ds-link-button">{item.subject}</span>{item.deleted && <span className="ds-subject-deleted-tag">Soft-deleted</span>}</td>
                        <td><span className="ds-status">{item.type}</span></td>
                        <td>{item.version || '-'}</td>
                        <td>{item.id || '-'}</td>
                        <td>{item.schemaType}</td>
                        <td title={item.compatibility === 'DEFAULT' ? `Inherits global compatibility (${appliedGlobalCompatibility})` : undefined}>{subjectCompatibilityLevel(item) === 'UNKNOWN' ? 'Unavailable' : subjectCompatibilityLevel(item)}</td>
                        {canManage && (
                          <td>
                            <div className="ds-inline-actions" onClick={e => e.stopPropagation()}>
                              <button className="ds-table-delete-btn" onClick={() => requestSchemaDelete(item.subject, Boolean(item.deleted))} disabled={saving}>
                                <Trash2 size={14} /> {item.deleted ? 'Permanently delete' : 'Soft delete'}
                              </button>
                            </div>
                          </td>
                        )}
                      </tr>
                    ))
                  ) : (
                    <tr><td colSpan={canManage ? 7 : 6} className="ds-empty">{filtersActive ? 'No subjects match your search and filters.' : 'No schemas found in this registry.'}</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </>}
        </>
      )}

      {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ DETAIL VIEW ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
      {view === 'detail' && selected && (
        <>
          {/* Breadcrumb bar */}
          <div className="ds-page-bar ds-sr-detail-bar">
            <div className="ds-breadcrumb-nav">
              <button className="ds-detail-back-button" onClick={backToList} aria-label="Back to Schema Registry">
                <ChevronLeft size={16} />
              </button>
              <span className="ds-breadcrumb-current">{selected.subject}</span>
            </div>
            <div className="ds-inline-actions">
              <button type="button" className="ds-button outlined" onClick={openCompare} disabled={loadingDetails} title="Compare versions">
                <GitCompare size={16} /> Compare Versions
              </button>
              {canManage && (
                <>
                  {!selected.deleted && <button className="ds-button primary" onClick={openEdit} disabled={!details?.latest || details.latest.version < 1}>
                    <Edit3 size={16} /> Edit Schema
                  </button>}
                  <button className="ds-button outlined" onClick={() => requestSchemaDelete(selected.subject, Boolean(selected.deleted))} disabled={saving}>
                    <Trash2 size={16} /> {selected.deleted ? 'Permanently delete subject' : 'Soft delete subject'}
                  </button>
                </>
              )}
            </div>
          </div>

          {error && <div className="ds-alert">{error}</div>}

          {/* Actual version */}
          <div className="ds-schema-shell ds-sr-detail-shell">
            <div className="ds-schema-code-card">
              <div className="ds-schema-card-header">
                <button
                  type="button"
                  className="ds-schema-copy-btn"
                  onClick={() => handleCopy(formatSchema(
                    details?.latest?.schema || selected.schema,
                    details?.latest?.schemaType || selected.schemaType
                  ))}
                  title="Copy schema to clipboard"
                >
                  {copiedText === formatSchema(
                    details?.latest?.schema || selected.schema,
                    details?.latest?.schemaType || selected.schemaType
                  ) ? (
                    <span className="ds-copied-text">Copied!</span>
                  ) : (
                    <Copy size={16} />
                  )}
                </button>
              </div>
              <pre className="ds-schema-code">
                {loadingDetails ? 'Loading schema...' : formatSchema(
                  details?.latest?.schema || selected.schema,
                  details?.latest?.schemaType || selected.schemaType
                )}
              </pre>
            </div>
            <div className="ds-schema-meta-card">
              <div className="ds-schema-card-header" />
              <div className="ds-meta-content">
                <div className="ds-meta-row"><span>Latest version</span><strong>{details?.latest?.version ?? selected.version ?? '-'}</strong></div>
                <div className="ds-meta-row"><span>ID</span><strong>{details?.latest?.id ?? selected.id ?? '-'}</strong></div>
                <div className="ds-meta-row"><span>Type</span><strong>{details?.latest?.schemaType || selected.schemaType || '-'}</strong></div>
                <div className="ds-meta-row ds-compat-row-block">
                  <span>Compatibility</span>
                  <div className="ds-compat-inline-custom">
                    <CustomSelect
                      value={subjectCompatibility}
                      onChange={setSubjectCompatibility}
                      disabled={!canManage || saving || Boolean(selected.deleted)}
                      options={compatibilityOptions.map(o => ({ value: o, label: o }))}
                    />
                    {canManage && !selected.deleted && (
                      <button className="ds-icon-button ds-compat-save-btn" onClick={() => requestSetting('compatibility', subjectCompatibility, selected.subject)} disabled={saving} title="Save">
                        <FileDown size={16} />
                      </button>
                    )}
                  </div>
                </div>
                <div className="ds-meta-row ds-compat-row-block">
                  <span>Subject mode</span>
                  <CustomSelect value={subjectMode ?? ''} placeholder="Mode unavailable" options={modeOptions}
                    disabled={!canManage || saving || !subjectMode || Boolean(selected.deleted)}
                    onChange={value => requestSetting('mode', value, selected.subject)} />
                  {subjectModeError && <span className="ds-settings-error" role="alert">{subjectModeError}</span>}
                </div>
              </div>
            </div>
          </div>

          <div className="ds-schema-delete-toolbar">
            <h4 className="ds-section-title">Versions</h4>
            <label><input type="checkbox" checked={showDeletedVersions} onChange={event => setShowDeletedVersions(event.target.checked)} /> Show soft-deleted versions</label>
          </div>
          <div className="ds-panel ds-sr-versions-panel">
            <table className="ds-table ds-sr-versions-table">
              <thead>
                <tr>
                  <th style={{ width: 40 }}></th>
                  <th>Version</th>
                  <th>ID</th>
                  <th>Type</th>
                  <th>Status</th>
                  {canManage && <th>Actions</th>}
                </tr>
              </thead>
              <tbody>
                {visibleVersions.map(version => (
                  <Fragment key={version.version}>
                    <tr key={version.version} className="ds-hoverable-row" style={{ cursor: 'pointer' }} onClick={() => toggleVersion(version.version)}>
                      <td>
                        <button
                          type="button"
                          className="ds-mini-button ds-expand-btn"
                          aria-label={`${expandedVersions.has(version.version) ? 'Collapse' : 'Expand'} version ${version.version}`}
                          aria-expanded={expandedVersions.has(version.version)}
                        >
                          {expandedVersions.has(version.version)
                            ? <ChevronDown size={14} aria-hidden="true" />
                            : <ChevronRight size={14} aria-hidden="true" />}
                        </button>
                      </td>
                      <td>{version.version}</td>
                      <td>{version.id}</td>
                      <td>{version.schemaType}</td>
                      <td>{version.deleted ? 'Soft-deleted' : 'Active'}</td>
                      {canManage && <td onClick={event => event.stopPropagation()}>
                        <button type="button" className="ds-table-delete-btn" disabled={saving}
                          onClick={() => requestSchemaDelete(selected.subject, Boolean(version.deleted), version.version)}>
                          <Trash2 size={14} /> {version.deleted ? 'Permanently delete' : 'Soft delete'}
                        </button>
                      </td>}
                    </tr>
                    {expandedVersions.has(version.version) && (
                      <tr key={`${version.version}-schema`} className="ds-version-expand-row">
                        <td colSpan={canManage ? 6 : 5} style={{ padding: '16px 20px' }}>
                          <div className="ds-expanded-schema-container">
                            <div className="ds-schema-card-header">
                              <button
                                type="button"
                                className="ds-schema-copy-btn"
                                onClick={() => handleCopy(formatSchema(version.schema, version.schemaType))}
                                title="Copy schema to clipboard"
                              >
                                {copiedText === formatSchema(version.schema, version.schemaType) ? (
                                  <span className="ds-copied-text">Copied!</span>
                                ) : (
                                  <Copy size={16} />
                                )}
                              </button>
                            </div>
                            <pre className="ds-version-schema-code">{formatSchema(version.schema, version.schemaType)}</pre>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                ))}
                {!loadingDetails && visibleVersions.length === 0 && (
                  <tr><td colSpan={canManage ? 6 : 5} className="ds-empty">No versions in this view.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
      {showCompare && details && createPortal(
        <div className="ds-modal-backdrop ds-compare-backdrop" role="dialog" aria-modal="true" aria-labelledby="compare-versions-title">
          <div className="ds-modal ds-compare-modal animate-fade-in">
            <div className="ds-modal-header">
              <div>
                <h3 id="compare-versions-title" style={{ fontSize: '18px', fontWeight: 'var(--font-bold)' }}>Compare Versions</h3>
                <span className="ds-muted-line">{details.subject}</span>
              </div>
              <button type="button" className="ds-close-btn" onClick={() => setShowCompare(false)} title="Close">
                <X size={20} />
              </button>
            </div>

            <div className="ds-compare-selectors">
              <div className="ds-field">
                <label>Version A</label>
                <CustomSelect
                  value={compareVersionA ? String(compareVersionA) : ''}
                  onChange={val => setCompareVersionA(Number(val))}
                  options={comparableVersions.map(v => ({
                    value: String(v.version),
                    label: `Version ${v.version} (ID ${v.id ?? 'Unavailable'})`
                  }))}
                />
              </div>
              <div className="ds-field">
                <label>Version B</label>
                <CustomSelect
                  value={compareVersionB ? String(compareVersionB) : ''}
                  onChange={val => setCompareVersionB(Number(val))}
                  options={comparableVersions.map(v => ({
                    value: String(v.version),
                    label: `Version ${v.version} (ID ${v.id ?? 'Unavailable'})`
                  }))}
                />
              </div>
            </div>

            {compareVersionA === compareVersionB ? (
              <div className="ds-compare-alert warning">Select two different versions to compare.</div>
            ) : (
              <>
                <div className={`ds-compare-alert ${schemasAreIdentical ? 'success' : 'different'}`}>
                  {schemasAreIdentical
                    ? 'The selected schema versions are identical.'
                    : schemaComparison?.changes.length
                      ? `${schemaComparison.changes.length} field change${schemaComparison.changes.length === 1 ? '' : 's'} found.`
                      : 'The selected schema versions are different.'}
                </div>
                {!schemasAreIdentical && schemaComparison && (
                  <section className="ds-compare-summary" aria-label="Field changes">
                    <div className="ds-compare-summary-heading">
                      <strong>Field changes</strong>
                      <span>Version {olderSchema?.version} → Version {newerSchema?.version}</span>
                    </div>
                    {schemaComparison.supported && schemaComparison.changes.length > 0 ? (
                      <ul className="ds-compare-changes">
                        {schemaComparison.changes.map((change, index) => (
                          <li key={`${change.field}-${change.kind}-${index}`}>
                            <span className={`ds-compare-change-kind ${change.kind}`}>{change.kind === 'type' ? 'Type changed' : change.kind === 'required' ? 'Required changed' : change.kind === 'default' ? 'Default changed' : change.kind}</span>
                            <code>{change.field}</code>
                            {change.kind === 'added'
                              ? <span>added as <code>{change.after}</code></span>
                              : change.kind === 'removed'
                                ? <span>removed (was <code>{change.before}</code>)</span>
                                : <span><code>{change.before}</code> → <code>{change.after}</code></span>}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p>{schemaComparison.reason || 'No field-level changes found. Other schema details or field order may differ.'}</p>
                    )}
                  </section>
                )}
                <div className="ds-compare-grid">
                  <div className="ds-compare-card">
                    <div className="ds-schema-card-header">
                      <span>Version {compareVersionA}</span>
                      <span>Schema ID: {comparedSchemaA?.id ?? '-'}</span>
                    </div>
                    <pre className="ds-compare-code">
                      {formatSchema(comparedSchemaA?.schema, comparedSchemaA?.schemaType, 'Schema unavailable')}
                    </pre>
                  </div>
                  <div className="ds-compare-card">
                    <div className="ds-schema-card-header">
                      <span>Version {compareVersionB}</span>
                      <span>Schema ID: {comparedSchemaB?.id ?? '-'}</span>
                    </div>
                    <pre className="ds-compare-code">
                      {formatSchema(comparedSchemaB?.schema, comparedSchemaB?.schemaType, 'Schema unavailable')}
                    </pre>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>,
        document.body
      )}

      {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ EDIT VIEW ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
      {canManage && view === 'edit' && selected && (
        <form className="ds-sr-edit-form" onSubmit={submitEditSchema}>
          {/* Breadcrumb bar */}
          <div className="ds-page-bar ds-sr-detail-bar ds-sr-edit-bar">
            <div className="ds-breadcrumb-nav">
              <button type="button" className="ds-detail-back-button" onClick={backToDetail} aria-label="Back to subject details">
                <ChevronLeft size={16} />
              </button>
              <span className="ds-breadcrumb-current">{selected.subject}</span>
            </div>
          </div>

          {error && <div className="ds-alert">{error}</div>}

          {/* Type + Compatibility selectors */}
          <div className="ds-edit-controls">
            <div className="ds-edit-selectors">
              <div className="ds-field">
                <label>Type</label>
                <CustomSelect
                  value={editSchemaType}
                  onChange={setEditSchemaType}
                  options={[
                    { value: 'AVRO', label: 'Avro' },
                    { value: 'JSON', label: 'Json' },
                    { value: 'PROTOBUF', label: 'Protobuf' }
                  ]}
                />
              </div>
              <div className="ds-field">
                <label>Compatibility level</label>
                <CustomSelect
                  value={editCompatibility}
                  onChange={setEditCompatibility}
                  options={compatibilityOptions.map(o => ({
                    value: o,
                    label: o.charAt(0) + o.slice(1).toLowerCase().replace('_', ' ')
                  }))}
                />
              </div>
            </div>
            <button type="submit" className="ds-button primary ds-edit-submit" disabled={saving}>
              {saving && <RefreshCw size={14} className="spin" />} Submit
            </button>
          </div>

          {/* Side-by-side editors */}
          <div className="ds-edit-shell">
            <div className="ds-edit-pane">
              <div className="ds-edit-pane-header">
                <span>Latest Schema</span>
                <button
                  type="button"
                  className="ds-schema-copy-btn"
                  onClick={() => handleCopy(formatSchema(
                    details?.latest?.schema || selected.schema || '{}',
                    details?.latest?.schemaType || selected.schemaType
                  ))}
                  title="Copy schema to clipboard"
                >
                  {copiedText === formatSchema(
                    details?.latest?.schema || selected.schema || '{}',
                    details?.latest?.schemaType || selected.schemaType
                  ) ? (
                    <span className="ds-copied-text">Copied!</span>
                  ) : (
                    <Copy size={16} />
                  )}
                </button>
              </div>
              <pre className="ds-edit-code ds-edit-readonly">
                {formatSchema(
                  details?.latest?.schema || selected.schema || '{}',
                  details?.latest?.schemaType || selected.schemaType
                )}
              </pre>
            </div>
            <div className="ds-edit-pane">
              <div className="ds-edit-pane-header">
                <span>New Schema</span>
                <button
                  type="button"
                  className="ds-schema-copy-btn"
                  onClick={() => handleCopy(newSchema)}
                  title="Copy schema to clipboard"
                >
                  {copiedText === newSchema ? (
                    <span className="ds-copied-text">Copied!</span>
                  ) : (
                    <Copy size={16} />
                  )}
                </button>
              </div>
              <textarea
                className="ds-edit-code ds-edit-editable"
                value={newSchema}
                onChange={e => setNewSchema(e.target.value)}
                spellCheck={false}
                required
              />
            </div>
          </div>
        </form>
      )}

      {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Connection modal ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
      {canManage && showConnection && createPortal(
        <div className="ds-modal-backdrop" role="dialog" aria-modal="true">
          <div className="ds-modal ds-connection-modal ds-upload-style-modal">
            <div className="ds-modal-header">
              <div>
                <h3>{editingConnectionId ? 'Edit Connection' : 'Add Schema Registry Connection'}</h3>
                <span className="ds-muted-line">{formConnectionName ? formConnectionName : 'New connection'}</span>
              </div>
              <button type="button" className="ds-close-btn" onClick={() => setShowConnection(false)}><X size={20} /></button>
            </div>
            <div className="ds-form ds-compact-form" style={{ padding: 0 }}>
              <div className="ds-modal-inner-card">
                {connectError && <div className="ds-alert" style={{ marginBottom: 12 }}>{connectError}</div>}
                {selectedConn?.status && editingConnectionId && (
                  <div style={{ padding: '10px 14px', background: "var(--bg-surface)", borderRadius: 8, border: '1px solid #E8E6E1', fontSize: 13, color: 'var(--text-primary)' }}>
                    Status: <strong style={{ color: connStatusColor(selectedConn.status) }}>{selectedConn.status}</strong>
                    {selectedConn.certificateConfigured && <span className="ds-configured-status"><Check size={14} aria-hidden="true" /> Cert Configured</span>}
                    {selectedConn.truststoreConfigured && <span className="ds-configured-status"><Check size={14} aria-hidden="true" /> Truststore Password Configured</span>}
                  </div>
                )}
                <div className="ds-field">
                  <label>Connection Name</label>
                  <input
                    value={formConnectionName}
                    onChange={e => setFormConnectionName(e.target.value)}
                    placeholder="e.g. Team A Registry"
                    required
                  />
                </div>
              </div>

              <div className="ds-modal-inner-card">
                <div className="ds-form-grid three">
                  <div className="ds-field">
                    <label>Protocol</label>
                    <CustomSelect
                      value={protocol}
                      onChange={setProtocol}
                      options={[
                        { value: 'http', label: 'http://' },
                        { value: 'https', label: 'https://' }
                      ]}
                    />
                  </div>
                  <div className="ds-field"><label>Host / IP</label><input value={customIp} onChange={e => setCustomIp(e.target.value)} placeholder="Host or IP address" required /></div>
                  <div className="ds-field"><label>Port</label><input type="number" value={customPort} onChange={e => setCustomPort(e.target.value)} placeholder="8081" required /></div>
                </div>
                <div className="ds-form-grid two">
                  <div className="ds-field">
                    <label>Certificate Type</label>
                    <CustomSelect
                      value={certType}
                      onChange={val => {
                        setCertType(val as CertificateType);
                        setCertFile(null);
                        setCertFileName('');
                        setCertPasteText('');
                      }}
                      options={[
                        { value: 'PEM', label: 'PEM (.pem / .crt)' },
                        { value: 'PKCS12', label: 'PKCS12 (.p12 / .pfx)' }
                      ]}
                    />
                  </div>
                  {certType === 'PEM' ? (
                    <div className="ds-field ds-certificate-field">
                      <div className="ds-certificate-label-row">
                        <label>Certificate</label>
                        <button type="button" className="ds-mini-button" onClick={() => { setCertPasteMode(!certPasteMode); setCertFile(null); setCertFileName(''); setCertPasteText(''); }}>
                          {certPasteMode
                            ? <><Paperclip size={14} aria-hidden="true" /> Upload file</>
                            : <><ClipboardPaste size={14} aria-hidden="true" /> Paste text</>}
                        </button>
                      </div>
                      {certPasteMode ? (
                        <textarea value={certPasteText} onChange={e => setCertPasteText(e.target.value)}
                          placeholder="-----BEGIN CERTIFICATE-----&#10;MIIDXTCCAkWgAwIBAgIJAMEn...&#10;-----END CERTIFICATE-----"
                          rows={5} style={{ fontFamily: 'monospace', fontSize: 12 }} />
                      ) : (
                        <div className="ds-file-upload-row">
                          <label className="ds-upload-btn-label">
                            <FileText size={16} /> Choose file
                            <input type="file" accept=".pem,.crt,.cer" onChange={e => { const f = e.target.files?.[0] || null; setCertFile(f); setCertFileName(f ? f.name : ''); }} style={{ display: 'none' }} />
                          </label>
                          {certFileName && (
                            <div className="ds-uploaded-file-pill">
                              <FileText size={16} />
                              <span>{certFileName}</span>
                              <button type="button" className="ds-clear-file-btn" onClick={() => { setCertFile(null); setCertFileName(''); }}><X size={14} /></button>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="ds-field">
                      <label>Truststore File (.p12 / .pfx)</label>
                      <div className="ds-file-upload-row">
                        <label className="ds-upload-btn-label">
                          <FileText size={16} /> Choose file
                          <input type="file" accept=".p12,.pfx" onChange={e => { const f = e.target.files?.[0] || null; setCertFile(f); setCertFileName(f ? f.name : ''); }} style={{ display: 'none' }} />
                        </label>
                        {certFileName && (
                          <div className="ds-uploaded-file-pill">
                            <FileText size={16} />
                            <span>{certFileName}</span>
                            <button type="button" className="ds-clear-file-btn" onClick={() => { setCertFile(null); setCertFileName(''); }}><X size={14} /></button>
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                  {!certFileName && !certPasteText.trim() && editingConnectionId && selectedConn?.certificateConfigured && (
                    <div className="ds-saved-certificate">
                      <FileText size={16} aria-hidden="true" />
                      <span>Saved file: {selectedConn.certificateFileName || (selectedConn.certificateType === 'PEM' ? 'Certificate (name unavailable)' : 'Truststore (name unavailable)')}. Kept unless replaced.</span>
                    </div>
                  )}
                  {!certFileName && !certPasteText.trim() && !selectedConn?.certificateConfigured && protocol === 'https' && clusterTruststore?.available && (
                    <div className="ds-saved-certificate">
                      <FileText size={16} aria-hidden="true" />
                      <span>{clusterTruststore.displayName} ({clusterTruststore.certificateType}) will be used when saved.</span>
                    </div>
                  )}
                </div>
                {certType === 'PKCS12' && <div className="ds-field"><label>Truststore Password {selectedConn?.truststoreConfigured ? '(Leave blank to keep existing)' : ''}</label><input type="password" value={certPassword} onChange={e => setCertPassword(e.target.value)} placeholder="Password" /></div>}
                <div className="ds-default-toggle-row" style={{ border: 'none', paddingTop: 0, marginTop: 4 }}>
                  <label className="ds-toggle-switch" htmlFor="sr-is-default">
                    <input
                      type="checkbox"
                      id="sr-is-default"
                      checked={formIsDefault}
                      onChange={e => setFormIsDefault(e.target.checked)}
                    />
                    <span className="ds-toggle-track">
                      <span className="ds-toggle-thumb" />
                    </span>
                  </label>
                  <label htmlFor="sr-is-default" className="ds-toggle-label" style={{ color: 'var(--text-tertiary)', fontSize: 'var(--text-sm)', cursor: 'pointer' }}>
                    Set as default connection for this cluster
                  </label>
                </div>
              </div>
            </div>
            <div className="ds-modal-footer">
              <button className="ds-button" onClick={() => setShowConnection(false)} disabled={connectSaving}>Cancel</button>
              <button className="ds-button primary" onClick={handleSaveConnection} disabled={connectSaving || !customIp.trim() || !customPort.trim()}>
                {connectSaving ? 'Saving...' : 'Save & Connect'}
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}

      {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Create Schema modal ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
      {canManage && showCreate && createPortal(
        <div className="ds-modal-backdrop" role="dialog" aria-modal="true">
          <form className="ds-modal ds-upload-style-modal" onSubmit={submitCreateSchema}>
            <div className="ds-modal-header">
              <div>
                <h3>Create Schema</h3>
                <span className="ds-muted-line">Register a new schema subject</span>
              </div>
              <button type="button" className="ds-close-btn" onClick={() => setShowCreate(false)}><X size={20} /></button>
            </div>
            {error && <div className="ds-alert" role="alert">{error}</div>}
            <div className="ds-form" style={{ padding: 0 }}>
              <div className="ds-modal-inner-card">
                <div className="ds-field"><label>Subject</label><input value={createSubject} onChange={e => setCreateSubject(e.target.value)} placeholder="orders-value" required /></div>
                <div className="ds-field">
                  <label>Schema Type</label>
                  <CustomSelect
                    value={createSchemaType}
                    onChange={setCreateSchemaType}
                    options={[
                      { value: 'AVRO', label: 'AVRO' },
                      { value: 'JSON', label: 'JSON' },
                      { value: 'PROTOBUF', label: 'PROTOBUF' }
                    ]}
                  />
                </div>
                <div className="ds-field"><label>Schema</label><textarea value={createSchema} onChange={e => setCreateSchema(e.target.value)} required /></div>
              </div>
            </div>
            <div className="ds-modal-footer">
              <button type="button" className="ds-button" onClick={() => setShowCreate(false)}>Cancel</button>
              <button type="submit" className="ds-button primary" disabled={saving}>
                {saving ? 'Saving...' : 'Save'}
              </button>
            </div>
          </form>
        </div>,
        document.body
      )}

      {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Custom Delete Connection Confirmation Modal ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
      {showDeleteConfirm && createPortal(
        <div className="ds-modal-backdrop ds-delete-backdrop" role="dialog" aria-modal="true">
          <div className="ds-delete-confirm-modal animate-fade-in">
            <div className="ds-delete-modal-banner">
              <img src={orangeBanner} alt="banner" className="ds-delete-banner-img" />
              <button className="ds-delete-modal-close" onClick={() => setShowDeleteConfirm(false)}><X size={20} /></button>
            </div>
            <div className="ds-delete-confirm-content">
              <div className="ds-delete-confirm-title-row">
                <AlertOctagon className="ds-delete-alert-icon" size={24} />
                <h2>{selectedConn ? `${selectedConn.host || selectedConn.connectionName} says` : 'Delete Connection'}</h2>
              </div>
              <p className="ds-delete-confirm-desc">Are you sure you want to delete this connection?</p>
              <div className="ds-delete-modal-footer">
                <button className="ds-delete-btn-cancel" onClick={() => setShowDeleteConfirm(false)}>Cancel</button>
                <button className="ds-delete-btn-ok" onClick={() => {
                  setShowDeleteConfirm(false);
                  confirmDeleteConnection();
                }}>OK</button>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}

      {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Custom Delete Subject Confirmation Modal ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
      {pendingDelete && createPortal(
        <div className="ds-modal-backdrop ds-delete-backdrop" role="dialog" aria-modal="true">
          <div className="ds-delete-confirm-modal animate-fade-in">
            <div className="ds-delete-modal-banner">
              <img src={orangeBanner} alt="banner" className="ds-delete-banner-img" />
              <button className="ds-delete-modal-close" onClick={() => setPendingDelete(null)} disabled={saving}><X size={20} /></button>
            </div>
            <div className="ds-delete-confirm-content">
              <div className="ds-delete-confirm-title-row">
                <AlertOctagon className="ds-delete-alert-icon" size={24} />
                <h2>{pendingDelete.permanent ? 'Permanently delete' : 'Soft delete'} {pendingDelete.version ? 'schema version' : 'subject'}</h2>
              </div>
              <p className="ds-delete-confirm-desc">
                {pendingDelete.subject}{pendingDelete.version ? ` / version ${pendingDelete.version}` : ' / all versions'}
              </p>
              <p className="ds-delete-confirm-desc">{pendingDelete.permanent
                ? 'This permanently removes Schema Registry metadata and cannot be undone. Kafka records are not deleted.'
                : 'This hides the schema from normal views. Schema Registry keeps its metadata and schema ID; an admin can permanently delete it later.'}</p>
              {pendingDelete.permanent && <label className="ds-delete-confirm-field">
                Type <strong>{pendingDelete.version ? `${pendingDelete.subject}/${pendingDelete.version}` : pendingDelete.subject}</strong> to confirm
                <input value={deleteConfirmation} onChange={event => setDeleteConfirmation(event.target.value)} autoComplete="off" />
              </label>}
              {error && <div className="ds-alert" role="alert">{error}</div>}
              <div className="ds-delete-modal-footer">
                <button className="ds-delete-btn-cancel" onClick={() => setPendingDelete(null)} disabled={saving}>Cancel</button>
                <button className="ds-delete-btn-ok" onClick={() => void confirmSchemaDelete()}
                  disabled={saving || (pendingDelete.permanent && deleteConfirmation !== (pendingDelete.version ? `${pendingDelete.subject}/${pendingDelete.version}` : pendingDelete.subject))}>
                  {saving ? 'Deleting...' : pendingDelete.permanent ? 'Permanently delete' : 'Soft delete'}
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
