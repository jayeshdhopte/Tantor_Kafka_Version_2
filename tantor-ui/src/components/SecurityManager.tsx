import { useState, useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';
import {
  Plus, Trash2, RefreshCw, Loader2, Search, AlertCircle, X,
} from 'lucide-react';
import {
  getAcls, createAcl, deleteAcl,
} from '../lib/api';
import { usePermissions } from '../hooks/usePermissions';
import { CustomSelect } from './CustomSelect';

interface Props {
  clusterId: string;
}

export interface AclEntry {
  principal: string;
  host: string;
  operation: string;
  permissionType: string;
  resourceType: string;
  resourceName: string;
  patternType: string;
}

const OPERATIONS = ['Read', 'Write', 'Create', 'Describe', 'Alter', 'Delete', 'All'];

export default function SecurityManager({ clusterId }: Props) {
  const { canManage } = usePermissions();
  // Ã¢â€â‚¬Ã¢â€â‚¬ ACLs state Ã¢â€â‚¬Ã¢â€â‚¬
  const [acls, setAcls] = useState<AclEntry[]>([]);
  const [aclsLoading, setAclsLoading] = useState(false);
  const [aclsError, setAclsError] = useState('');
  const [showCreateAcl, setShowCreateAcl] = useState(false);
  const [aclPrincipal, setAclPrincipal] = useState('');
  const [aclResourceType, setAclResourceType] = useState('Topic');
  const [aclResourceName, setAclResourceName] = useState('');
  const [aclPatternType, setAclPatternType] = useState('Literal');
  const [aclOperations, setAclOperations] = useState<string[]>([]);
  const [aclPermission, setAclPermission] = useState('Allow');
  const [aclHost, setAclHost] = useState('');
  const [aclCreating, setAclCreating] = useState(false);
  const [aclFilterPrincipal, setAclFilterPrincipal] = useState('');
  const [aclFilterResource, setAclFilterResource] = useState('');
  const [aclToDelete, setAclToDelete] = useState<AclEntry | null>(null);
  const [alertMessage, setAlertMessage] = useState<string | null>(null);

  const fetchAcls = useCallback(async () => {
    setAclsLoading(true);
    setAclsError('');
    try {
      const data = await getAcls(clusterId);
      setAcls(data.acls || []);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Failed to load ACLs';
      const axErr = err as { response?: { data?: { detail?: string } } };
      setAclsError(axErr.response?.data?.detail || msg);
    } finally {
      setAclsLoading(false);
    }
  }, [clusterId]);

  useEffect(() => { void (async () => { await fetchAcls(); })(); }, [fetchAcls]);

  const handleCreateAcl = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!canManage) return;
    const normalizedPrincipal = aclPrincipal.trim().replace(/^User:/i, '').trim();
    if (!normalizedPrincipal) {
      setAlertMessage('Please enter a Kafka principal.');
      return;
    }
    if (aclOperations.length === 0) {
      setAlertMessage("Please select at least one operation.");
      return;
    }
    setAclCreating(true);
    try {
      for (const op of aclOperations) {
        await createAcl(clusterId, {
          resourceType: aclResourceType,
          resourceName: aclResourceName,
          pattern_type: aclPatternType,
          principal: `User:${normalizedPrincipal}`,
          host: aclHost,
          operation: op,
          permission_type: aclPermission,
        });
      }
      setShowCreateAcl(false);
      setAclOperations([]);
      setAclPrincipal('');
      setAclResourceName('');
      fetchAcls();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } }; message?: string })?.response?.data?.detail
        || (err instanceof Error ? err.message : 'Failed to create ACL');
      setAlertMessage(msg);
    } finally {
      setAclCreating(false);
    }
  };

  const handleDeleteAcl = (acl: AclEntry) => {
    if (!canManage) return;
    setAclToDelete(acl);
  };

  const confirmDeleteAcl = async (acl: AclEntry) => {
    try {
      await deleteAcl(clusterId, {
        resourceType: acl.resourceType,
        resourceName: acl.resourceName,
        pattern_type: acl.patternType,
        principal: acl.principal,
        host: acl.host,
        operation: acl.operation,
        permission_type: acl.permissionType,
      });
      fetchAcls();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } }; message?: string })?.response?.data?.detail
        || (err instanceof Error ? err.message : 'Failed to delete ACL');
      setAlertMessage(msg);
    }
  };

  const toggleAclOperation = (op: string) => {
    if (!canManage) return;
    if (op === 'All') {
      setAclOperations(prev => prev.includes('All') ? [] : ['All']);
    } else {
      setAclOperations(prev => {
        const withoutAll = prev.filter(o => o !== 'All');
        if (withoutAll.includes(op)) return withoutAll.filter(o => o !== op);
        return [...withoutAll, op];
      });
    }
  };

  const filteredAcls = acls.filter(acl => {
    if (aclFilterPrincipal && !acl.principal.toLowerCase().includes(aclFilterPrincipal.toLowerCase())) return false;
    if (aclFilterResource && !acl.resourceName.toLowerCase().includes(aclFilterResource.toLowerCase())) return false;
    return true;
  });

  return (
    <div className="security-manager" style={{ padding: '0px 10px' }}>
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        gap: '1rem',
        marginBottom: '1.25rem',
        marginTop: '0.5rem'
      }}>
        {/* Left side: Filters */}
        <div style={{ display: 'flex', gap: '1rem', flex: 1 }}>
          <div style={{ position: 'relative', flex: 1, maxWidth: '240px' }}>
            <Search size={16} style={{ position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)', color: '#9ca3af' }} />
            <input 
              type="text" 
              placeholder="Filter by principal..." 
              value={aclFilterPrincipal} 
              onChange={e => setAclFilterPrincipal(e.target.value)} 
              style={{
                width: '100%',
                padding: '10px 12px 10px 36px',
                borderRadius: 'var(--radius-md)',
                border: '1px solid var(--border-subtle)',
                fontSize: 'var(--text-base)',
                fontFamily: 'Satoshi, Inter, sans-serif',
                outline: 'none',
                color: 'var(--button-primary-active)'
              }}
            />
          </div>
          <div style={{ position: 'relative', flex: 1, maxWidth: '240px' }}>
            <Search size={16} style={{ position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)', color: '#9ca3af' }} />
            <input 
              type="text" 
              placeholder="Filter by resources..." 
              value={aclFilterResource} 
              onChange={e => setAclFilterResource(e.target.value)} 
              style={{
                width: '100%',
                padding: '10px 12px 10px 36px',
                borderRadius: 'var(--radius-md)',
                border: '1px solid var(--border-subtle)',
                fontSize: 'var(--text-base)',
                fontFamily: 'Satoshi, Inter, sans-serif',
                outline: 'none',
                color: 'var(--button-primary-active)'
              }}
            />
          </div>
        </div>

        {/* Right side: Actions */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <button 
            onClick={fetchAcls} 
            disabled={aclsLoading} 
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: '42px',
              height: '42px',
              borderRadius: 'var(--radius-md)',
              border: '1px solid var(--border-subtle)',
              background: "var(--bg-surface)",
              cursor: 'pointer',
              color: '#475569',
              transition: 'all 0.2s'
            }}
            title="Refresh"
          >
            <RefreshCw size={18} className={aclsLoading ? 'spin' : ''} />
          </button>
          {canManage && (
            <button 
              onClick={() => setShowCreateAcl(true)} 
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 'var(--space-2)',
                height: '42px',
                padding: '0 20px',
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
              <Plus size={16} /> Add ACL
            </button>
          )}
        </div>
      </div>

      {aclsError && (
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: '10px',
          background: '#FFF9EB',
          border: '1px solid #FFE0B2',
          borderRadius: 'var(--radius-md)',
          padding: '12px 16px',
          color: '#B78103',
          fontSize: 'var(--text-base)',
          fontWeight: 'var(--font-medium)',
          marginBottom: '1.25rem',
          fontFamily: 'Satoshi, Inter, sans-serif'
        }}>
          <AlertCircle size={18} style={{ color: '#F5A623' }} />
          <span>Failed to load ACLs</span>
        </div>
      )}

      {canManage && showCreateAcl && createPortal(
        <div
          className="modal-overlay"
          role="presentation"
          onClick={() => setShowCreateAcl(false)}
          style={{
          position: 'fixed',
          inset: 0,
          background: 'rgba(15, 23, 42, 0.48)',
          backdropFilter: 'blur(2px)',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          padding: '32px',
          overflowY: 'auto',
          boxSizing: 'border-box',
          zIndex: 10000,
          fontFamily: 'Satoshi, Inter, sans-serif'
        }}>
          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="add-acl-modal-title"
            onClick={event => event.stopPropagation()}
            style={{
            background: "var(--bg-surface)",
            borderRadius: '16px',
            width: '100%',
            maxWidth: '780px',
            maxHeight: 'calc(100vh - 64px)',
            boxShadow: '0 10px 25px rgba(0,0,0,0.1)',
            overflowY: 'auto',
            flexShrink: 0
          }}>
            {/* Modal Header */}
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              padding: '20px 24px',
              borderBottom: '1px solid #f1f5f9'
            }}>
              <h3 id="add-acl-modal-title" style={{ margin: 0, fontSize: 'var(--text-xl)', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Add New ACL Binding</h3>
              <button 
                onClick={() => setShowCreateAcl(false)} 
                style={{
                  background: 'none',
                  border: 'none',
                  fontSize: 'var(--text-2xl)',
                  color: '#94a3b8',
                  cursor: 'pointer',
                  padding: '4px'
                }}
              >
                <X size={20} aria-hidden="true" />
              </button>
            </div>

            {/* Modal Body */}
            <form onSubmit={handleCreateAcl} style={{ padding: 'var(--space-6)' }}>
              <div style={{
                background: '#F9F9FB',
                borderRadius: 'var(--radius-md)',
                padding: 'var(--space-6)',
                display: 'grid',
                gridTemplateColumns: 'repeat(3, 1fr)',
                gap: '20px 16px',
                marginBottom: '20px'
              }}>
                {/* Principal (Username) */}
                <div>
                  <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-base)', color: 'var(--button-primary-active)' }}>Principal (Username)</label>
                  <input
                    type="text"
                    value={aclPrincipal}
                    onChange={e => setAclPrincipal(e.target.value)}
                    placeholder="User:admin"
                    required
                    style={{
                      width: '100%',
                      padding: '10px 12px',
                      borderRadius: 'var(--radius-md)',
                      border: '1px solid var(--border-default)',
                      fontSize: 'var(--text-base)',
                      background: 'var(--bg-surface)',
                      color: 'var(--button-primary-active)',
                      outline: 'none'
                    }}
                  />
                </div>

                {/* Host */}
                <div>
                  <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-base)', color: 'var(--button-primary-active)' }}>Host</label>
                  <input 
                    type="text" 
                    value={aclHost} 
                    onChange={e => setAclHost(e.target.value)} 
                    placeholder="* or client IP"
                    required 
                    style={{
                      width: '100%',
                      padding: '10px 12px',
                      borderRadius: 'var(--radius-md)',
                      border: '1px solid var(--border-default)',
                      fontSize: 'var(--text-base)',
                      background: "var(--bg-surface)",
                      color: 'var(--button-primary-active)',
                      outline: 'none'
                    }}
                  />
                </div>

                {/* Resource Type */}
                <div>
                  <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-base)', color: 'var(--button-primary-active)' }}>Resource Type</label>
                  <CustomSelect
                    value={aclResourceType}
                    onChange={setAclResourceType}
                    width="100%"
                    variant="audit"
                    options={[
                      { value: 'Topic', label: 'Topic' },
                      { value: 'Group', label: 'Group' },
                      { value: 'Cluster', label: 'Cluster' },
                      { value: 'TransactionalId', label: 'TransactionalId' },
                    ]}
                  />
                </div>

                {/* Resource Name */}
                <div>
                  <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-base)', color: 'var(--button-primary-active)' }}>Resource Name</label>
                  <input 
                    type="text" 
                    value={aclResourceName} 
                    onChange={e => setAclResourceName(e.target.value)} 
                    placeholder="e.g. topics_name" 
                    required 
                    style={{
                      width: '100%',
                      padding: '10px 12px',
                      borderRadius: 'var(--radius-md)',
                      border: '1px solid var(--border-default)',
                      fontSize: 'var(--text-base)',
                      background: "var(--bg-surface)",
                      color: 'var(--button-primary-active)',
                      outline: 'none'
                    }}
                  />
                </div>

                {/* Pattern Type */}
                <div>
                  <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-base)', color: 'var(--button-primary-active)' }}>Pattern Type</label>
                  <CustomSelect
                    value={aclPatternType}
                    onChange={setAclPatternType}
                    width="100%"
                    variant="audit"
                    options={[
                      { value: 'Literal', label: 'Literal' },
                      { value: 'Prefixed', label: 'Prefixed' },
                    ]}
                  />
                </div>

                {/* Permission */}
                <div>
                  <label style={{ display: 'block', marginBottom: '8px', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-base)', color: 'var(--button-primary-active)' }}>Permission</label>
                  <CustomSelect
                    value={aclPermission}
                    onChange={setAclPermission}
                    width="100%"
                    variant="audit"
                    options={[
                      { value: 'Allow', label: 'Allow' },
                      { value: 'Deny', label: 'Deny' },
                    ]}
                  />
                </div>
              </div>

              {/* Operations */}
              <div style={{
                background: '#F9F9FB',
                borderRadius: 'var(--radius-md)',
                padding: 'var(--space-6)',
                marginBottom: '24px'
              }}>
                <label style={{ display: 'block', marginBottom: '16px', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', color: 'var(--button-primary-hover)' }}>Operations</label>
                <div style={{ display: 'flex', gap: 'var(--space-2)', flexWrap: 'wrap' }}>
                  {OPERATIONS.map(op => {
                    const isSelected = aclOperations.includes(op);
                    return (
                      <button 
                        type="button" 
                        key={op} 
                        onClick={() => toggleAclOperation(op)}
                        style={{
                          padding: '8px 16px',
                          borderRadius: 'var(--radius-md)',
                          fontSize: 'var(--text-base)',
                          fontWeight: 'var(--font-medium)',
                          border: '1px solid var(--border-default)',
                          background: isSelected ? 'var(--button-primary)' : '#FFFFFF',
                          color: isSelected ? '#FFFFFF' : '#5F6368',
                          cursor: 'pointer',
                          transition: 'all 0.15s'
                        }}
                      >
                        {op}
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Modal Footer Actions */}
              <div style={{
                display: 'flex',
                justifyContent: 'flex-end',
                gap: '12px',
                borderTop: '1px solid #f1f5f9',
                paddingTop: '20px',
                marginTop: '10px'
              }}>
                <button 
                  type="button" 
                  onClick={() => setShowCreateAcl(false)} 
                  style={{
                    height: '38px',
                    padding: '0 24px',
                    borderRadius: 'var(--radius-md)',
                    border: '1px solid var(--button-primary)',
                    background: "var(--bg-surface)",
                    color: 'var(--button-primary)',
                    fontWeight: 'var(--font-medium)',
                    fontSize: 'var(--text-base)',
                    cursor: 'pointer'
                  }}
                >
                  Cancel
                </button>
                <button 
                  type="submit" 
                  disabled={aclCreating} 
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    height: '38px',
                    padding: '0 24px',
                    borderRadius: 'var(--radius-md)',
                    background: 'var(--button-primary)',
                    color: "var(--text-light)",
                    fontWeight: 'var(--font-medium)',
                    fontSize: 'var(--text-base)',
                    border: 'none',
                    cursor: 'pointer'
                  }}
                >
                  {aclCreating && <Loader2 size={14} className="spin" />}
                  Create
                </button>
              </div>
            </form>
          </div>
        </div>,
        document.body
      )}

      <div className="table-container" style={{overflowX:'auto',border:'1px solid var(--bg-neutral)',borderRadius:8}}>
        <table style={{width:'100%',borderCollapse:'collapse',textAlign:'left'}}>
          <thead style={{background:'#f9fafb',borderBottom:'1px solid var(--bg-neutral)'}}>
            <tr>
              <th style={{padding:'0.75rem',fontSize:'0.85rem',fontWeight: 'var(--font-semibold)',color:'#4b5563'}}>Principal</th>
              <th style={{padding:'0.75rem',fontSize:'0.85rem',fontWeight: 'var(--font-semibold)',color:'#4b5563'}}>Host</th>
              <th style={{padding:'0.75rem',fontSize:'0.85rem',fontWeight: 'var(--font-semibold)',color:'#4b5563'}}>Resource</th>
              <th style={{padding:'0.75rem',fontSize:'0.85rem',fontWeight: 'var(--font-semibold)',color:'#4b5563'}}>Operation</th>
              <th style={{padding:'0.75rem',fontSize:'0.85rem',fontWeight: 'var(--font-semibold)',color:'#4b5563'}}>Permission</th>
              {canManage && <th style={{padding:'0.75rem',fontSize:'0.85rem',fontWeight: 'var(--font-semibold)',color:'#4b5563'}}>Actions</th>}
            </tr>
          </thead>
          <tbody>
            {aclsLoading && acls.length === 0 ? (
              <tr><td colSpan={canManage ? 6 : 5} style={{padding:'2rem',textAlign:'center'}}><Loader2 className="spin" size={24} style={{margin:'0 auto'}}/></td></tr>
            ) : filteredAcls.length === 0 ? (
              <tr><td colSpan={canManage ? 6 : 5} style={{padding:'2rem',textAlign:'center',color:'#6b7280'}}>No ACLs found.</td></tr>
            ) : (
              filteredAcls.map((acl, i) => (
                <tr key={i} style={{borderBottom:'1px solid var(--bg-neutral)'}}>
                  <td style={{padding:'0.75rem',fontWeight: 'var(--font-medium)'}}>{acl.principal}</td>
                  <td style={{padding:'0.75rem',color:'#6b7280'}}>{acl.host}</td>
                  <td style={{padding:'0.75rem'}}>
                    <span style={{fontSize:'0.75rem',textTransform:'uppercase',background:'#f3f4f6',padding:'2px 6px',borderRadius:4,marginRight:6}}>{acl.resourceType}</span>
                    <span style={{fontWeight: 'var(--font-medium)'}}>{acl.resourceName}</span>
                    {acl.patternType !== 'LITERAL' && <span style={{fontSize:'0.75rem',color:'#9ca3af',marginLeft:6}}>({acl.patternType})</span>}
                  </td>
                  <td style={{padding:'0.75rem'}}>{acl.operation}</td>
                  <td style={{padding:'0.75rem'}}>
                    <span style={{
                      padding:'2px 8px', borderRadius:12, fontSize:'0.75rem', fontWeight: 'var(--font-semibold)',
                      background: acl.permissionType === 'ALLOW' ? '#dcfce7' : '#fee2e2',
                      color: acl.permissionType === 'ALLOW' ? '#166534' : '#991b1b'
                    }}>
                      {acl.permissionType}
                    </span>
                  </td>
                  {canManage && (
                    <td style={{padding:'0.75rem'}}>
                      <button onClick={() => handleDeleteAcl(acl)} style={{color:'#ef4444',background:'none',border:'none',cursor:'pointer',padding:4}} title="Delete ACL">
                        <Trash2 size={16} />
                      </button>
                    </td>
                  )}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {aclToDelete && createPortal(
        <div className="modal-overlay" style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background: 'rgba(0, 0, 0, 0.4)',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          zIndex: 1000,
          fontFamily: 'Satoshi, Inter, sans-serif'
        }}>
          <div style={{
            background: "var(--bg-surface)",
            borderRadius: '16px',
            width: '100%',
            maxWidth: '540px',
            boxShadow: '0 10px 25px rgba(0,0,0,0.1)',
            overflow: 'hidden'
          }}>
            {/* Banner */}
            <div className="confirm-modal-banner" style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', padding: '0 24px', boxSizing: 'border-box', height: '72px' }}>
              <button onClick={() => setAclToDelete(null)} className="confirm-modal-close-btn" style={{ color: 'var(--text-tertiary)', background: 'transparent', border: 'none', cursor: 'pointer', fontSize: 'var(--text-xl)' }} aria-label="Close modal">
                <X size={20} aria-hidden="true" />
              </button>
            </div>
            
            {/* Body */}
            <div className="confirm-modal-body" style={{ padding: 'var(--space-6)', display: 'flex', flexDirection: 'column', gap: 'var(--space-4)', boxSizing: 'border-box' }}>
              <div className="confirm-modal-title-row" style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
                <AlertCircle size={20} color="var(--color-danger)" style={{ flexShrink: 0 }} />
                <h2 style={{ margin: 0, fontSize: '18px', fontWeight: 'var(--font-semibold)', color: 'var(--button-primary-active)' }}>
                  Confirm action
                </h2>
              </div>
              
              <p style={{ margin: 0, fontSize: '15px', color: '#5F6368', lineHeight: '1.5' }}>
                Are you sure you want to delete this ACL binding?
                <br />
                <span style={{ fontWeight: 'var(--font-semibold)', color: 'var(--button-primary-active)', display: 'inline-block', marginTop: '8px' }}>
                  Delete ACL for {aclToDelete.principal} on {aclToDelete.resourceType} {aclToDelete.resourceName}?
                </span>
              </p>
              
              {/* Footer Actions */}
              <div style={{
                display: 'flex',
                justifyContent: 'flex-end',
                gap: '12px',
                marginTop: '8px'
              }}>
                <button 
                  type="button" 
                  onClick={() => setAclToDelete(null)} 
                  style={{
                    height: '38px',
                    padding: '0 24px',
                    borderRadius: 'var(--radius-md)',
                    border: '1px solid var(--color-danger)',
                    background: "var(--bg-surface)",
                    color: 'var(--color-danger)',
                    fontWeight: 'var(--font-medium)',
                    fontSize: 'var(--text-base)',
                    cursor: 'pointer'
                  }}
                >
                  Cancel
                </button>
                <button 
                  onClick={async () => {
                    const acl = aclToDelete;
                    setAclToDelete(null);
                    await confirmDeleteAcl(acl);
                  }} 
                  style={{
                    height: '38px',
                    padding: '0 24px',
                    borderRadius: 'var(--radius-md)',
                    background: 'var(--button-primary)',
                    color: "var(--text-light)",
                    fontWeight: 'var(--font-medium)',
                    fontSize: 'var(--text-base)',
                    border: 'none',
                    cursor: 'pointer'
                  }}
                >
                  OK
                </button>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}

      {alertMessage && createPortal(
        <div className="modal-overlay" style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background: 'rgba(0, 0, 0, 0.4)',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          zIndex: 1100,
          fontFamily: 'Satoshi, Inter, sans-serif'
        }}>
          <div style={{
            background: "var(--bg-surface)",
            borderRadius: '16px',
            width: '100%',
            maxWidth: '480px',
            boxShadow: '0 10px 25px rgba(0,0,0,0.1)',
            overflow: 'hidden'
          }}>
            {/* Banner */}
            <div className="confirm-modal-banner" style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', padding: '0 24px', boxSizing: 'border-box', height: '72px' }}>
              <button onClick={() => setAlertMessage(null)} className="confirm-modal-close-btn" style={{ color: 'var(--text-tertiary)', background: 'transparent', border: 'none', cursor: 'pointer', fontSize: 'var(--text-xl)' }} aria-label="Close modal">
                <X size={20} aria-hidden="true" />
              </button>
            </div>
            
            {/* Body */}
            <div className="confirm-modal-body" style={{ padding: 'var(--space-6)', display: 'flex', flexDirection: 'column', gap: 'var(--space-4)', boxSizing: 'border-box' }}>
              <div className="confirm-modal-title-row" style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
                <AlertCircle size={20} color="var(--color-danger)" style={{ flexShrink: 0 }} />
                <h2 style={{ margin: 0, fontSize: '18px', fontWeight: 'var(--font-semibold)', color: 'var(--button-primary-active)' }}>
                  Notice
                </h2>
              </div>
              
              <p style={{ margin: 0, fontSize: '15px', color: '#5F6368', lineHeight: '1.5' }}>
                {alertMessage}
              </p>
              
              {/* Footer Actions */}
              <div style={{
                display: 'flex',
                justifyContent: 'flex-end',
                marginTop: '8px'
              }}>
                <button 
                  onClick={() => setAlertMessage(null)} 
                  style={{
                    height: '38px',
                    padding: '0 28px',
                    borderRadius: 'var(--radius-md)',
                    background: 'var(--button-primary)',
                    color: "var(--text-light)",
                    fontWeight: 'var(--font-medium)',
                    fontSize: 'var(--text-base)',
                    border: 'none',
                    cursor: 'pointer'
                  }}
                >
                  OK
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
