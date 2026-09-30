import { useMemo, useState, useEffect, useRef } from 'react';
import { Bell, LogOut } from 'lucide-react';
import { useAuth } from '../contexts/useAuth';
import { usePermissions } from '../hooks/usePermissions';
import { useNavigate } from 'react-router-dom';
import { isAuthEnabled } from '../services/KeycloakService';
import './TopNavbar.css';
import tantorLogo from '../assets/Tantor-pink-logo.png';

export function TopNavbar() {
  const { decodedToken, logout } = useAuth();
  const { isAdmin } = usePermissions();
  const authEnabled = isAuthEnabled();
  const navigate = useNavigate();

  // State
  const [isProfileOpen, setIsProfileOpen] = useState(false);
  const [alertsCount, setAlertsCount] = useState(0);

  const profileRef = useRef<HTMLDivElement>(null);

  // Resolve the first letter of the username
  const userInitial = useMemo(() => {
    const rawName = decodedToken?.preferred_username || decodedToken?.name || 'User';
    return rawName.charAt(0).toUpperCase();
  }, [decodedToken]);

  const applicationRole = isAdmin ? 'Admin' : 'Monitor';

  // Fetch alerts count
  useEffect(() => {
    const fetchAlerts = async () => {
      try {
        const res = await fetch('/api/v1/ui/alerts');
        if (res.ok) {
          const data = await res.json();
          setAlertsCount(Array.isArray(data)
            ? data.filter((alert: { status?: string }) => alert.status?.toUpperCase() !== 'RESOLVED').length
            : 0);
        }
      } catch (e) {
        console.error('Failed to fetch alerts count', e);
      }
    };
    fetchAlerts();
    const interval = setInterval(fetchAlerts, 10000);
    return () => clearInterval(interval);
  }, []);



  // Click outside handlers
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (profileRef.current && !profileRef.current.contains(event.target as Node)) {
        setIsProfileOpen(false);
      }

    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);


  const handleSignOut = async () => {
    setIsProfileOpen(false);
    // keycloak.logout() triggers a full-page redirect to the Keycloak
    // end-session endpoint, which then 302-redirects back to the app.
    // Do not set window.location.href here; it races with the Keycloak
    // redirect and can prevent proper session termination.
    try {
      await logout();
    } catch (e) {
      console.error('Logout failed', e);
    }
  };

  return (
    <div className="top-navbar" style={{ position: 'relative' }}>
      <div className="navbar-left">
        <div className="logo-container" style={{ cursor: 'pointer' }} onClick={() => navigate('/dashboard')}>
          <img src={tantorLogo} alt="Tantor" className="header-logo" />
        </div>
      </div>
      
      <div className="navbar-right">


        {/* Notifications Bell */}
        <button className="nav-action-btn" title="Notifications" onClick={() => navigate('/alerts')} style={{ position: 'relative' }}>
          <Bell size={20} />
          {alertsCount > 0 && (
            <span style={{
              position: 'absolute',
              top: '4px',
              right: '4px',
              background: '#EF4444',
              color: "var(--text-light)",
              borderRadius: '50%',
              width: '8px',
              height: '8px',
              display: 'block'
            }} />
          )}
        </button>

        {/* Profile Dropdown Container */}
        <div ref={profileRef} style={{ position: 'relative' }}>
          <div className="profile-badge" onClick={() => setIsProfileOpen(!isProfileOpen)} style={{ cursor: 'pointer' }}>
            <span>{userInitial}</span>
          </div>

          {/* Profile Dropdown Card */}
          {isProfileOpen && (
            <div style={{
              position: 'absolute',
              top: '48px',
              right: 0,
              width: '360px',
              background: "var(--bg-surface)",
              border: '1px solid var(--bg-neutral-2)',
              borderRadius: '16px',
              boxShadow: '0 10px 40px rgba(0,0,0,0.1)',
              padding: 'var(--space-6)',
              zIndex: 1000,
              fontFamily: 'Satoshi, Inter, sans-serif'
            }}>
              {/* Profile Header */}
              <div style={{ display: 'flex', gap: 'var(--space-4)', marginBottom: '16px' }}>
                <div style={{
                  width: '48px',
                  height: '48px',
                  borderRadius: '50%',
                  background: '#A78BFA',
                  color: "var(--text-light)",
                  display: 'flex',
                  justifyContent: 'center',
                  alignItems: 'center',
                  fontWeight: 'var(--font-semibold)',
                  fontSize: '18px',
                  textTransform: 'uppercase'
                }}>
                  {userInitial}
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  <span style={{ fontSize: 'var(--text-md)', fontWeight: 'var(--font-semibold)', color: 'var(--text-primary)' }}>
                    {decodedToken?.preferred_username || decodedToken?.name || 'User'}
                  </span>
                  {decodedToken?.email && (
                    <span style={{ fontSize: 'var(--text-sm)', color: 'var(--text-muted)' }}>
                      {decodedToken.email}
                    </span>
                  )}
                </div>
              </div>

              {/* Application role: the UI exposes only Admin and Monitor. */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', marginBottom: '20px' }}>
                <span style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  background: '#EFF6FF',
                  color: '#3B82F6',
                  border: '1px solid #DBEAFE',
                  fontSize: 'var(--text-xs)',
                  fontWeight: 'var(--font-medium)',
                  padding: '4px 10px',
                  borderRadius: 'var(--radius-md)'
                }}>
                  {applicationRole}
                </span>
              </div>

              {/* Divider */}
              <hr style={{ border: 'none', borderTop: '1px solid #f1f5f9', margin: '0 -24px 16px -24px' }} />


              {/* Sign Out Row */}
              {authEnabled && <button
                type="button"
                onClick={() => void handleSignOut()}
                style={{ 
                  display: 'flex', 
                  alignItems: 'center', 
                  gap: 'var(--space-2)', 
                  color: '#EF4444', 
                  fontSize: 'var(--text-base)', 
                  fontWeight: 'var(--font-semibold)', 
                  cursor: 'pointer',
                  padding: '4px 0',
                  border: 'none',
                  background: 'transparent'
                }}
              >
                <LogOut size={16} />
                Sign Out
              </button>}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
