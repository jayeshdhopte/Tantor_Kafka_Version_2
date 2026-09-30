import { useState } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import {
  Bell,
  ChevronDown,
  ChevronRight,
  LayoutDashboard,
  LineChart,
  Network,
  Package,
  PlayCircle,
  Server,
  Settings,
  ShieldAlert,
  Users,
} from 'lucide-react';
import './Sidebar.css';
import collapseIcon from '../assets/collapse.png';

type NavItem = {
  icon?: React.ElementType;
  label: string;
  path?: string;
  subItems?: NavItem[];
};

type NavSection = {
  label: string;
  items: NavItem[];
};

const navSections: NavSection[] = [
  {
    label: '',
    items: [
      { icon: LayoutDashboard, label: 'Dashboard', path: '/dashboard' },
      { icon: Package, label: 'Artifacts', path: '/artifacts' },
      { icon: Network, label: 'Cluster', path: '/clusters' },
      { icon: Server, label: 'Hosts', path: '/hosts' },
      { icon: LineChart, label: 'Monitoring', path: '/monitoring' },
      { icon: Bell, label: 'Alerts', path: '/alerts' },
      { icon: ShieldAlert, label: 'Audits', path: '/audit' },
      { icon: PlayCircle, label: 'Jobs', path: '/jobs' },
      { icon: Settings, label: 'LDAP', path: '/ldap-settings' },
      { icon: Users, label: 'User Management', path: '/user-management' },
    ],
  },
];

const hiddenNavPaths = new Set(['/user-management', '/commands', '/ldap-settings', '/admin']);

export function Sidebar() {
  const [isCollapsed, setIsCollapsed] = useState(() => {
    return localStorage.getItem('tantor.sidebarCollapsed') === 'true';
  });

  const toggleCollapse = () => {
    setIsCollapsed(prev => {
      const next = !prev;
      localStorage.setItem('tantor.sidebarCollapsed', String(next));
      return next;
    });
  };

  const [expandedItems, setExpandedItems] = useState<Record<string, boolean>>({
    Clusters: true,
  });
  const location = useLocation();

  const toggleExpand = (label: string) => {
    setExpandedItems(prev => ({ ...prev, [label]: !prev[label] }));
  };

  const renderItem = (item: NavItem, depth = 0) => {
    if (item.subItems) {
      const isExpanded = expandedItems[item.label];
      const isActive = item.subItems.some(sub => location.pathname === sub.path || (sub.path && sub.path !== '/' && location.pathname.startsWith(sub.path + '/')));

      return (
        <div key={item.label} className="nav-item-group">
          <div
            className={`nav-item ${isActive ? 'active-parent' : ''}`}
            style={{ paddingLeft: isCollapsed ? '0px' : `${18 + depth * 12}px` }}
            onClick={() => toggleExpand(item.label)}
          >
            {item.icon && <item.icon size={15} className="nav-item-icon" />}
            <span style={{ flex: 1 }}>{item.label}</span>
            {!isCollapsed && (isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />)}
          </div>
          {!isCollapsed && isExpanded && (
            <div className="nav-subitems">
              {item.subItems.map(sub => renderItem(sub, depth + 1))}
            </div>
          )}
        </div>
      );
    }

    return (
      <NavLink
        key={item.label}
        to={item.path!}
        end={item.path === '/dashboard'}
        className={({ isActive }) => `nav-item${isActive ? ' active' : ''}`}
      >
        {item.icon && <item.icon size={20} className="nav-item-icon" />}
        {!item.icon && <span className="nav-item-dot" />}
        <span>{item.label}</span>
      </NavLink>
    );
  };

  return (
    <aside className={`sidebar ${isCollapsed ? 'collapsed' : ''}`}>
      <nav className="sidebar-nav">
        {navSections.map(section => (
          <div key={section.label} className="nav-section">
            {section.label && <span className="nav-section-label">{section.label}</span>}
            {section.items
              .filter(item => !item.path || !hiddenNavPaths.has(item.path))
              .map(item => renderItem(item))}
          </div>
        ))}
      </nav>

      <div className="sidebar-footer">
        <img
          src={collapseIcon}
          alt="Collapse Sidebar"
          className="sidebar-toggle-icon"
          onClick={toggleCollapse}
          style={{ transform: isCollapsed ? 'rotate(180deg)' : 'none' }}
        />
      </div>
    </aside>
  );
}
