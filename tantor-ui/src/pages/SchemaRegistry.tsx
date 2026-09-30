import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useParams } from 'react-router-dom';
import { AlertOctagon, Check, ChevronDown, ChevronLeft, ChevronRight, ClipboardPaste, Copy, Edit3, FileDown, FileText, GitCompare, MoreVertical, Paperclip, Plus, RefreshCw, Save, Settings, Trash2, X } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import orangeBanner from '../assets/orange.png';
import { AnchoredMenu } from '../components/AnchoredMenu';
import { readDataServiceSession, writeDataServiceSession } from '../utils/dataServiceSessionCache';
import './DataServiceTabs.css';

interface SchemaSubject {
  subject: string;
  type: string;
  version: number;
  id: number | null;
  schemaType: string;
  schema: string;
}

interface SchemaVersion {
  version: number;
  id: number | null;
  schemaType: string;
  schema: string;
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
  'FULL_TRANSITIVE'
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

type View = 'list' | 'detail' | 'edit';

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
  const [showConnection, setShowConnection] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [subjectToDelete, setSubjectToDelete] = useState<string | null>(null);
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
  const [selectedConnectionId, setSelectedConnectionId] = useState<string | null>(initialSession?.selectedConnectionId ?? null);
  const loadRequestId = useRef(0);

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
  const [connectSaving, setConnectSaving] = useState(false);
  const [connectError, setConnectError] = useState<string | null>(null);

  // Derived: currently selected connection object
  const selectedConn = useMemo(
    () => savedConnections.find(c => c.id === selectedConnectionId) ?? null,
    [savedConnections, selectedConnectionId]
  );

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
      if (requestId === loadRequestId.current) setGlobalCompatibility(compatibility);
      return compatibility;
    } catch {
      if (requestId === loadRequestId.current) setGlobalCompatibility('BACKWARD');
      return 'BACKWARD';
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
      let url = withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/summary`, connectionId);
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
    setCertType('PEM');
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

  const openSubject = async (item: SchemaSubject) => {
    setSelected(item);
    setView('detail');
    setDetails(null);
    setLoadingDetails(true);
    setError(null);
    setExpandedVersions(new Set());

    try {
      const res = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(item.subject)}/details`));
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || 'Failed to load subject details.');
      setDetails(data);
      setSubjectCompatibility(data.compatibility || globalCompatibility);
    } catch (e: unknown) {
      setDetails({
        subject: item.subject,
        latest: { version: item.version, id: item.id, schemaType: item.schemaType, schema: item.schema },
        versions: [{ version: item.version, id: item.id, schemaType: item.schemaType, schema: item.schema }],
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
  const schemasAreIdentical = Boolean(
    comparedSchemaA && comparedSchemaB && comparedSchemaA.schema === comparedSchemaB.schema
  );

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
      if (!res.ok) throw new Error(data.message || 'Failed to create schema.');
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

  const submitEditSchema = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!canManage) return;
    if (!selected || !newSchema.trim()) return;
    setSaving(true);
    setError(null);
    try {
      const res = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(selected.subject)}/versions`), {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ schemaType: editSchemaType, schema: newSchema })
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || 'Failed to update schema.');
      // Also save compatibility if changed
      if (editCompatibility !== subjectCompatibility) {
        await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(selected.subject)}/config`), {
          method: 'PUT', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ compatibility: editCompatibility })
        });
        setSubjectCompatibility(editCompatibility);
      }
      await load();
      // Reload detail
      const refreshed = summary?.subjects.find(s => s.subject === selected.subject) || selected;
      await openSubject(refreshed);
    } catch (e: unknown) {
      setError(errorMessage(e, 'Failed to update schema.'));
    } finally {
      setSaving(false);
    }
  };

  const deleteSubject = (name: string) => {
    if (!canManage) return;
    setSubjectToDelete(name);
  };

  const confirmDeleteSubject = async (name: string) => {
    setSaving(true);
    setError(null);
    try {
      const res = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(name)}`), {
        method: 'DELETE'
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || 'Failed to delete subject.');
      backToList();
      await load();
    } catch (e: unknown) {
      setError(errorMessage(e, 'Failed to delete subject.'));
    } finally {
      setSaving(false);
    }
  };

  const saveGlobalCompatibility = async (val: string) => {
    setGlobalCompatibility(val);
    if (!canManage) return;
    setSaving(true); setError(null);
    try {
      const res = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/config`), {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ compatibility: val })
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || 'Failed to update global compatibility.');
    } catch (e: unknown) {
      setError(errorMessage(e, 'Failed to update global compatibility.'));
    } finally { setSaving(false); }
  };

  const saveSubjectCompatibility = async () => {
    if (!canManage) return;
    if (!selected) return;
    setSaving(true); setError(null);
    try {
      const res = await fetch(withConnId(`/api/v1/clusters/${id}/data-services/schema-registry/subjects/${encodeURIComponent(selected.subject)}/config`), {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ compatibility: subjectCompatibility })
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.message || 'Failed to update subject compatibility.');
      setDetails(prev => prev ? { ...prev, compatibility: subjectCompatibility } : prev);
    } catch (e: unknown) {
      setError(errorMessage(e, 'Failed to update subject compatibility.'));
    } finally { setSaving(false); }
  };

  const connStatusColor = (s: string) =>
    s === 'ONLINE' ? '#80e8a2' : (s === 'OFFLINE' || s === 'ERROR') ? '#e88080' : '#a8c5c0';

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Render ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  return (
    <div className="data-services-page animate-fade-in">

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
                    onChange={saveGlobalCompatibility}
                    disabled={!canManage}
                    options={compatibilityOptions.map(o => ({
                      value: o,
                      label: o.charAt(0) + o.slice(1).toLowerCase().replace('_', ' ')
                    }))}
                  />
                </div>
              </div>

              <div className="ds-buttons-group">
                {/* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Buttons ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */}
                {canManage && (
                  <button 
                    className="ds-sr-save-button"
                    onClick={() => saveGlobalCompatibility(globalCompatibility)} 
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
            <div className="ds-metric-card"><span>Total Subjects</span><strong>{summary?.totalSubjects ?? 0}</strong></div>
            <div className="ds-metric-card"><span>Value Subjects</span><strong>{summary?.valueSubjects ?? 0}</strong></div>
            <div className="ds-metric-card"><span>Key Subjects</span><strong>{summary?.keySubjects ?? 0}</strong></div>
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
            <table className="ds-table ds-sr-subjects-table">
              <thead>
                <tr><th>Subject</th><th>Type</th><th>Latest Version</th><th>Schema ID</th><th>Schema Type</th>{canManage && <th>Actions</th>}</tr>
              </thead>
              <tbody>
                {loading && !summary ? (
                  <tr><td colSpan={canManage ? 6 : 5} className="ds-empty">Loading schemas...</td></tr>
                ) : summary && summary.subjects.length > 0 ? (
                  summary.subjects.map(item => (
                    <tr key={item.subject} className="ds-hoverable-row" onClick={() => openSubject(item)} style={{ cursor: 'pointer' }}>
                      <td><span className="ds-link-button">{item.subject}</span></td>
                      <td><span className="ds-status">{item.type}</span></td>
                      <td>{item.version || '-'}</td>
                      <td>{item.id || '-'}</td>
                      <td>{item.schemaType}</td>
                      {canManage && (
                        <td>
                          <div className="ds-inline-actions" onClick={e => e.stopPropagation()}>
                            <button className="ds-table-delete-btn" onClick={() => deleteSubject(item.subject)} disabled={saving}>
                              <Trash2 size={14} /> Delete
                            </button>
                          </div>
                        </td>
                      )}
                    </tr>
                  ))
                ) : (
                  <tr><td colSpan={canManage ? 6 : 5} className="ds-empty">No schemas found in this registry.</td></tr>
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
                  <button className="ds-button primary" onClick={openEdit} disabled={!details?.latest}>
                    <Edit3 size={16} /> Edit Schema
                  </button>
                  <button className="ds-icon-button" onClick={() => deleteSubject(selected.subject)} disabled={saving} title="Delete subject">
                    <Trash2 size={16} />
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
                      disabled={!canManage}
                      options={compatibilityOptions.map(o => ({ value: o, label: o }))}
                    />
                    {canManage && (
                      <button className="ds-icon-button ds-compat-save-btn" onClick={saveSubjectCompatibility} disabled={saving} title="Save">
                        <FileDown size={16} />
                      </button>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Old versions */}
          <h4 className="ds-section-title">Old versions</h4>
          <div className="ds-panel ds-sr-versions-panel">
            <table className="ds-table ds-sr-versions-table">
              <thead>
                <tr>
                  <th style={{ width: 40 }}></th>
                  <th>Version</th>
                  <th>ID</th>
                  <th>Type</th>
                </tr>
              </thead>
              <tbody>
                {(details?.versions || []).map(version => (
                  <>
                    <tr key={version.version} className="ds-hoverable-row" style={{ cursor: 'pointer' }} onClick={() => toggleVersion(version.version)}>
                      <td>
                        <button className="ds-mini-button ds-expand-btn">
                          {expandedVersions.has(version.version)
                            ? <ChevronDown size={14} aria-hidden="true" />
                            : <ChevronRight size={14} aria-hidden="true" />}
                        </button>
                      </td>
                      <td>{version.version}</td>
                      <td>{version.id}</td>
                      <td>{version.schemaType}</td>
                    </tr>
                    {expandedVersions.has(version.version) && (
                      <tr key={`${version.version}-schema`} className="ds-version-expand-row">
                        <td colSpan={4} style={{ padding: '16px 20px' }}>
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
                  </>
                ))}
                {!loadingDetails && (!details?.versions || details.versions.length === 0) && (
                  <tr><td colSpan={4} className="ds-empty">No older versions found.</td></tr>
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
                  {schemasAreIdentical ? 'The selected schema versions are identical.' : 'The selected schema versions are different.'}
                </div>
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
      {subjectToDelete && createPortal(
        <div className="ds-modal-backdrop ds-delete-backdrop" role="dialog" aria-modal="true">
          <div className="ds-delete-confirm-modal animate-fade-in">
            <div className="ds-delete-modal-banner">
              <img src={orangeBanner} alt="banner" className="ds-delete-banner-img" />
              <button className="ds-delete-modal-close" onClick={() => setSubjectToDelete(null)}><X size={20} /></button>
            </div>
            <div className="ds-delete-confirm-content">
              <div className="ds-delete-confirm-title-row">
                <AlertOctagon className="ds-delete-alert-icon" size={24} />
                <h2>{selectedConn ? `${selectedConn.host || selectedConn.connectionName} says` : 'Delete Subject'}</h2>
              </div>
              <p className="ds-delete-confirm-desc">Are you sure you want to delete this subject?</p>
              <div className="ds-delete-modal-footer">
                <button className="ds-delete-btn-cancel" onClick={() => setSubjectToDelete(null)}>Cancel</button>
                <button className="ds-delete-btn-ok" onClick={() => {
                  confirmDeleteSubject(subjectToDelete);
                  setSubjectToDelete(null);
                }}>OK</button>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}
    </div>
  );
}
