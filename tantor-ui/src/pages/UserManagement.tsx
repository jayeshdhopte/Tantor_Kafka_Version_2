import { useState, useEffect, useCallback } from 'react';
import { Users, Plus, Shield, Eye, Trash2, Key, Check, X } from 'lucide-react';
import { getUsers, createAuthUser, updateAuthUser, deleteAuthUser } from '../lib/api.ts';
import type { UserResponse } from '../types/index.ts';
import { confirmAction } from '../components/confirmUtils';
import './UserManagement.css';

export default function UserManagement() {
  const [users, setUsers] = useState<UserResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [newUser, setNewUser] = useState({ username: '', password: '', role: 'monitor' });
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState('');
  const [editingPassword, setEditingPassword] = useState<string | null>(null);
  const [newPassword, setNewPassword] = useState('');

  const fetchUsers = useCallback(async () => {
    try {
      const data = await getUsers();
      setUsers(data);
    } catch {
      setError('Failed to load users');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void (async () => { await fetchUsers(); })(); }, [fetchUsers]);

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreating(true);
    setError('');
    try {
      await createAuthUser(newUser);
      setShowCreate(false);
      setNewUser({ username: '', password: '', role: 'monitor' });
      fetchUsers();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Failed to create user';
      setError(msg);
    } finally {
      setCreating(false);
    }
  };

  const handleToggleRole = async (user: UserResponse) => {
    const newRole = user.role === 'admin' ? 'monitor' : 'admin';
    try {
      await updateAuthUser(user.id, { role: newRole });
      fetchUsers();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Failed to update role';
      setError(msg);
    }
  };

  const handleToggleActive = async (user: UserResponse) => {
    try {
      await updateAuthUser(user.id, { active: !user.is_active });
      fetchUsers();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Failed to update status';
      setError(msg);
    }
  };

  const handleDelete = async (user: UserResponse) => {
    if (!(await confirmAction(`Delete user "${user.username}"? This cannot be undone.`))) return;
    try {
      await deleteAuthUser(user.id);
      fetchUsers();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Failed to delete user';
      setError(msg);
    }
  };

  const handlePasswordChange = async (userId: string) => {
    if (!newPassword) return;
    try {
      await updateAuthUser(userId, { password: newPassword });
      setEditingPassword(null);
      setNewPassword('');
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Failed to change password';
      setError(msg);
    }
  };

  if (loading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '16rem' }}>
        <div style={{ width: '2rem', height: '2rem', border: '4px solid rgba(83, 74, 183, 0.3)', borderTopColor: 'var(--accent-primary)', borderRadius: '50%', animation: 'spin 1s linear infinite' }} />
      </div>
    );
  }

  return (
    <div className="user-management-page animate-fade-in">
      <header className="page-header flex-between">
        <div>
          <h1 style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
            <Users size={20} /> User Management
          </h1>
          <p>Manage application users and their roles</p>
        </div>
        <div className="header-actions">
          <button
            onClick={() => setShowCreate(!showCreate)}
            className="btn btn-primary-action"
          >
            <Plus size={14} />
            Add User
          </button>
        </div>
      </header>

      {error && (
        <div className="error-banner">
          {error}
          <button onClick={() => setError('')} className="btn icon-only danger">
            <X size={14} />
          </button>
        </div>
      )}

      {/* Create Form */}
      {showCreate && (
        <div className="create-user-form animate-fade-in">
          <h3>Create New User</h3>
          <form onSubmit={handleCreate} className="form-row">
            <div className="form-group">
              <label>Username</label>
              <input
                type="text"
                value={newUser.username}
                onChange={(e) => setNewUser({ ...newUser, username: e.target.value })}
                required
                placeholder="username"
              />
            </div>
            <div className="form-group">
              <label>Password</label>
              <input
                type="password"
                value={newUser.password}
                onChange={(e) => setNewUser({ ...newUser, password: e.target.value })}
                required
                placeholder="password"
              />
            </div>
            <div className="form-group" style={{ flex: '0 1 150px', minWidth: '150px' }}>
              <label>Role</label>
              <select
                value={newUser.role}
                onChange={(e) => setNewUser({ ...newUser, role: e.target.value })}
              >
                <option value="monitor">Monitor</option>
                <option value="admin">Admin</option>
              </select>
            </div>
            <div className="form-actions">
              <button
                type="submit"
                disabled={creating}
                className="btn btn-primary-action"
              >
                {creating ? 'Creating...' : 'Create'}
              </button>
              <button
                type="button"
                onClick={() => setShowCreate(false)}
                className="btn"
              >
                Cancel
              </button>
            </div>
          </form>
        </div>
      )}

      {/* Users Table */}
      <div className="table-container">
        <table className="data-table">
          <thead>
            <tr>
              <th>User</th>
              <th>Source</th>
              <th>Role</th>
              <th>Status</th>
              <th>Last Login</th>
              <th>Created</th>
              <th style={{ textAlign: 'right' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {users.map((user) => (
              <tr key={user.id}>
                <td className="font-medium">
                  {user.username}
                </td>
                <td>
                  {user.auth_source === 'ldap' ? (
                    <span
                      className="auth-source-badge ldap"
                      title={user.ldap_dn || 'Synced from directory'}
                    >
                      LDAP
                    </span>
                  ) : (
                    <span className="auth-source-badge local">
                      local
                    </span>
                  )}
                </td>
                <td>
                  <button
                    onClick={() => handleToggleRole(user)}
                    className={`role-badge ${user.role}`}
                  >
                    {user.role === 'admin' ? <Shield size={12} /> : <Eye size={12} />}
                    {user.role}
                  </button>
                </td>
                <td>
                  <button
                    onClick={() => handleToggleActive(user)}
                    className={`status-badge ${user.is_active ? 'online' : 'offline'}`}
                  >
                    {user.is_active ? 'Active' : 'Disabled'}
                  </button>
                </td>
                <td className="text-secondary">
                  {user.last_login ? new Date(user.last_login).toLocaleString() : 'Never'}
                </td>
                <td className="text-secondary">
                  {new Date(user.created_at).toLocaleDateString()}
                </td>
                <td>
                  <div className="actions-cell">
                    {editingPassword === user.id ? (
                      <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                        <input
                          type="password"
                          value={newPassword}
                          onChange={(e) => setNewPassword(e.target.value)}
                          placeholder="New password"
                          style={{
                            width: '120px', padding: '4px 8px', fontSize: '0.8rem',
                            border: '1px solid var(--border-default)', borderRadius: 'var(--radius-sm)'
                          }}
                          autoFocus
                        />
                        <button
                          onClick={() => handlePasswordChange(user.id)}
                          className="btn icon-only" style={{ color: 'var(--color-success)' }}
                        >
                          <Check size={14} />
                        </button>
                        <button
                          onClick={() => { setEditingPassword(null); setNewPassword(''); }}
                          className="btn icon-only danger"
                        >
                          <X size={14} />
                        </button>
                      </div>
                    ) : (
                      user.auth_source === 'ldap' ? (
                        <span
                          className="btn icon-only"
                          title="Password is managed by your directory (LDAP)"
                          style={{ opacity: 0.5, cursor: 'not-allowed' }}
                        >
                          <Key size={14} />
                        </span>
                      ) : (
                        <button
                          onClick={() => setEditingPassword(user.id)}
                          className="btn icon-only"
                          title="Change password"
                        >
                          <Key size={14} />
                        </button>
                      )
                    )}
                    <button
                      onClick={() => handleDelete(user)}
                      className="btn icon-only danger"
                      title="Delete user"
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
            {users.length === 0 && (
              <tr>
                <td colSpan={7}>
                  <div className="empty-state">
                    No users found.
                  </div>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
