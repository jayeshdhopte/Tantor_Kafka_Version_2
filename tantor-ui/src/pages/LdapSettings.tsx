import { useState, useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { KeyRound, Save, TestTube, RefreshCw, X, Check } from 'lucide-react';
import './LdapSettings.css';

interface LdapConfig {
  id?: string;
  enabled: boolean;
  serverUrl: string;
  useSsl: boolean;
  tlsValidateCert: boolean;
  tlsCaCertPresent?: boolean;
  tlsCaCert?: string;
  bindDn: string;
  userSearchBase: string;
  userSearchFilter: string;
  groupSearchBase: string;
  adminGroupDn: string;
  monitorGroupDn: string;
  defaultRole: string;
  connectionTimeout: number;
}

const FILTER_PRESETS = [
  { label: 'Active Directory (sAMAccountName)', value: '(sAMAccountName={username})' },
  { label: 'Active Directory (userPrincipalName)', value: '(userPrincipalName={username}@DOMAIN)' },
  { label: 'OpenLDAP (uid)', value: '(uid={username})' },
  { label: 'OpenLDAP (cn)', value: '(cn={username})' },
];

const DEFAULT_CONFIG: LdapConfig = {
  enabled: false,
  serverUrl: '',
  useSsl: false,
  tlsValidateCert: true,
  tlsCaCertPresent: false,
  bindDn: '',
  userSearchBase: '',
  userSearchFilter: '(sAMAccountName={username})',
  groupSearchBase: '',
  adminGroupDn: '',
  monitorGroupDn: '',
  defaultRole: 'monitor',
  connectionTimeout: 10,
};

export function LdapSettings() {
  const [config, setConfig] = useState<LdapConfig>(DEFAULT_CONFIG);
  const [bindPassword, setBindPassword] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [showTestModal, setShowTestModal] = useState(false);
  const [testUsername, setTestUsername] = useState('');
  const [testPassword, setTestPassword] = useState('');
  const [testResult, setTestResult] = useState<{ success: boolean; message: string; userDn?: string; groups?: string[] } | null>(null);

  const fetchConfig = useCallback(async () => {
    try {
      const res = await fetch('/api/v1/ldap/config');
      if (res.ok) {
        const data = await res.json();
        if (data && Object.keys(data).length > 0) {
          setConfig(data);
        }
      }
    } catch {
      // No config yet
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void (async () => { await fetchConfig(); })();
  }, [fetchConfig]);

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!bindPassword && !config.id) {
      setError('Bind password is required for initial configuration');
      return;
    }
    setSaving(true);
    setError('');
    setSuccess('');
    try {
      const payload: LdapConfig & { bindPassword?: string } = {
        ...config,
        bindPassword: bindPassword || undefined,
      };

      const res = await fetch('/api/v1/ldap/config', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload)
      });
      
      if (!res.ok) throw new Error('Failed to save config');
      const result = await res.json();
      setConfig(result);
      setBindPassword('');
      setSuccess('LDAP configuration saved successfully');
      setTimeout(() => setSuccess(''), 5000);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to save configuration');
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    if (!testUsername || !testPassword) return;
    setTesting(true);
    setTestResult(null);
    try {
      const res = await fetch('/api/v1/ldap/test', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username: testUsername, password: testPassword })
      });
      if (!res.ok) throw new Error('Test failed');
      const result = await res.json();
      setTestResult(result);
    } catch (err: unknown) {
      setTestResult({ success: false, message: err instanceof Error ? err.message : 'Test failed' });
    } finally {
      setTesting(false);
    }
  };

  if (loading) {
    return (
      <div className="state-center">
        Loading LDAP Configuration...
      </div>
    );
  }

  return (
    <div className="ldap-settings-container animate-fade-in">
      <div className="ldap-header">
        <div>
          <h1 className="ldap-title">
            <KeyRound size={26} />
            LDAP / Active Directory
          </h1>
          <p className="ldap-subtitle">Configure directory-based authentication and role mapping.</p>
        </div>
        <button
          className="btn-secondary"
          onClick={() => setShowTestModal(true)}
          disabled={!config.id}
        >
          <TestTube size={18} />
          Test Connection
        </button>
      </div>

      {error && (
        <div className="alert alert-error">
          <div style={{ display: 'flex', gap: 'var(--space-2)', alignItems: 'center' }}>
            <X size={18} /> {error}
          </div>
          <button style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'inherit' }} onClick={() => setError('')}>
            <X size={16} />
          </button>
        </div>
      )}

      {success && (
        <div className="alert alert-success">
          <div style={{ display: 'flex', gap: 'var(--space-2)', alignItems: 'center' }}>
            <Check size={18} /> {success}
          </div>
        </div>
      )}

      <form onSubmit={handleSave} style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-6)' }}>
        
        {/* Enable / Disable */}
        <div className="settings-card">
          <div className="card-header" style={{ marginBottom: 0, alignItems: 'center' }}>
            <div>
              <div className="card-title">LDAP Authentication</div>
              <div className="card-description">
                {config.enabled
                  ? 'LDAP authentication is enabled. Users can log in with their directory credentials.'
                  : 'LDAP authentication is disabled. Only local users can log in.'}
              </div>
            </div>
            <label className="toggle-switch">
              <input 
                type="checkbox" 
                checked={config.enabled} 
                onChange={(e) => setConfig({ ...config, enabled: e.target.checked })} 
              />
              <span className="slider"></span>
            </label>
          </div>
        </div>

        {/* Server Connection */}
        <div className="settings-card">
          <div className="card-header">
            <div className="card-title">Server Connection</div>
          </div>
          
          <div className="grid-2">
            <div className="form-group">
              <label>Server URL</label>
              <input
                className="form-control"
                type="text"
                value={config.serverUrl || ''}
                onChange={(e) => setConfig({ ...config, serverUrl: e.target.value })}
                placeholder="ldap://ad.company.com:389"
                required
              />
            </div>
            <div className="form-group">
              <label>Connection Timeout (seconds)</label>
              <input
                className="form-control"
                type="number"
                value={config.connectionTimeout}
                onChange={(e) => setConfig({ ...config, connectionTimeout: parseInt(e.target.value) || 10 })}
                min={1}
                max={60}
              />
            </div>
          </div>

          <div className="form-group" style={{ marginTop: '8px', marginBottom: '16px' }}>
            <label className="checkbox-label">
              <input
                type="checkbox"
                checked={config.useSsl}
                onChange={(e) => setConfig({ ...config, useSsl: e.target.checked })}
              />
              Use SSL/TLS (LDAPS)
            </label>
          </div>

          <div className="grid-2">
            <div className="form-group">
              <label>Bind DN</label>
              <input
                className="form-control"
                type="text"
                value={config.bindDn || ''}
                onChange={(e) => setConfig({ ...config, bindDn: e.target.value })}
                placeholder="cn=admin,dc=example,dc=com"
                required
              />
            </div>
            <div className="form-group">
              <label>Bind Password <span className="label-hint">{config.id ? '(leave blank to keep current)' : ''}</span></label>
              <input
                className="form-control"
                type="password"
                value={bindPassword}
                onChange={(e) => setBindPassword(e.target.value)}
                placeholder={config.id ? '********' : 'Enter password'}
                required={!config.id}
              />
            </div>
          </div>
        </div>

        {/* User Search */}
        <div className="settings-card">
          <div className="card-header">
            <div className="card-title">User Search</div>
          </div>

          <div className="form-group">
            <label>User Search Base</label>
            <input
              className="form-control"
              type="text"
              value={config.userSearchBase || ''}
              onChange={(e) => setConfig({ ...config, userSearchBase: e.target.value })}
              placeholder="ou=users,dc=example,dc=com"
              required
            />
          </div>

          <div className="form-group">
            <label>User Search Filter</label>
            <div className="flex-row">
              <input
                className="form-control flex-1"
                type="text"
                value={config.userSearchFilter || ''}
                onChange={(e) => setConfig({ ...config, userSearchFilter: e.target.value })}
                placeholder="(sAMAccountName={username})"
                required
              />
              <select
                className="form-control"
                value={FILTER_PRESETS.some(p => p.value === config.userSearchFilter) ? config.userSearchFilter : ""}
                onChange={(e) => {
                  if (e.target.value) setConfig({ ...config, userSearchFilter: e.target.value });
                }}
              >
                <option value="">Presets...</option>
                {FILTER_PRESETS.map((p) => (
                  <option key={p.value} value={p.value}>{p.label}</option>
                ))}
              </select>
            </div>
          </div>
        </div>

        {/* Group Mapping */}
        <div className="settings-card">
          <div className="card-header">
            <div>
              <div className="card-title">Group Mapping</div>
              <div className="card-description">Map LDAP groups to Tantor roles. Leave blank to assign the default role to all LDAP users.</div>
            </div>
          </div>

          <div className="form-group">
            <label>Group Search Base</label>
            <input
              className="form-control"
              type="text"
              value={config.groupSearchBase || ''}
              onChange={(e) => setConfig({ ...config, groupSearchBase: e.target.value })}
              placeholder="ou=groups,dc=example,dc=com"
            />
          </div>

          <div className="grid-2">
            <div className="form-group">
              <label>Admin Group DN</label>
              <input
                className="form-control"
                type="text"
                value={config.adminGroupDn || ''}
                onChange={(e) => setConfig({ ...config, adminGroupDn: e.target.value })}
              />
            </div>
            <div className="form-group">
              <label>Monitor Group DN</label>
              <input
                className="form-control"
                type="text"
                value={config.monitorGroupDn || ''}
                onChange={(e) => setConfig({ ...config, monitorGroupDn: e.target.value })}
              />
            </div>
          </div>

          <div className="form-group" style={{ maxWidth: '200px' }}>
            <label>Default Role</label>
            <select
              className="form-control"
              value={config.defaultRole}
              onChange={(e) => setConfig({ ...config, defaultRole: e.target.value })}
            >
              <option value="monitor">Monitor</option>
              <option value="admin">Admin</option>
            </select>
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '8px' }}>
          <button type="submit" className="btn-primary" disabled={saving}>
            <Save size={18} />
            {saving ? 'Saving...' : 'Save Configuration'}
          </button>
        </div>
      </form>

      {/* Test Connection Modal */}
      {showTestModal && createPortal(
        <div className="modal-overlay">
          <div className="modal-content">
            <div className="modal-header">
              <h3 className="modal-title">Test Connection</h3>
              <button className="modal-close" onClick={() => { setShowTestModal(false); setTestResult(null); }}>
                <X size={20} />
              </button>
            </div>
            
            <div className="form-group">
              <label>Username</label>
              <input
                className="form-control"
                type="text"
                value={testUsername}
                onChange={(e) => setTestUsername(e.target.value)}
              />
            </div>
            
            <div className="form-group">
              <label>Password</label>
              <input
                className="form-control"
                type="password"
                value={testPassword}
                onChange={(e) => setTestPassword(e.target.value)}
              />
            </div>

            {testResult && (
              <div className={`alert ${testResult.success ? 'alert-success' : 'alert-error'}`} style={{ marginTop: '16px', display: 'block' }}>
                <div style={{ fontWeight: '600', marginBottom: '4px' }}>{testResult.success ? 'Success' : 'Failed'}</div>
                <div>{testResult.message}</div>
              </div>
            )}

            <div className="modal-footer">
              <button className="btn-secondary" onClick={() => { setShowTestModal(false); setTestResult(null); }}>
                Close
              </button>
              <button
                className="btn-primary"
                onClick={handleTest}
                disabled={testing || !testUsername || !testPassword}
              >
                <RefreshCw size={16} className={testing ? 'animate-spin' : ''} />
                {testing ? 'Testing...' : 'Test'}
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}
    </div>
  );
}
