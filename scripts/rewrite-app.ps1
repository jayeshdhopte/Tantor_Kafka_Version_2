$appContent = @"
import React, { useState, useEffect, Suspense } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate, useParams, useNavigate } from 'react-router-dom';
import { Sidebar } from './components/Sidebar';
import { ClusterProvider } from './contexts/ClusterContext';
import { TopNavbar } from './components/TopNavbar';
import { GlobalConfirmDialog } from './components/ConfirmDialog';
import { usePermissions } from './hooks/usePermissions';
import './App.css';

// Lazy load all pages to reduce initial bundle size
const Dashboard = React.lazy(() => import('./pages/Dashboard').then(m => ({ default: m.Dashboard })));
const Hosts = React.lazy(() => import('./pages/Hosts').then(m => ({ default: m.Hosts })));
const Clusters = React.lazy(() => import('./pages/Clusters').then(m => ({ default: m.Clusters })));
const Artifacts = React.lazy(() => import('./pages/Artifacts').then(m => ({ default: m.Artifacts })));
const Monitoring = React.lazy(() => import('./pages/Monitoring').then(m => ({ default: m.Monitoring })));
const Alerts = React.lazy(() => import('./pages/Alerts').then(m => ({ default: m.Alerts })));
const AuditLogs = React.lazy(() => import('./pages/AuditLogs').then(m => ({ default: m.AuditLogs })));
const ClusterDetails = React.lazy(() => import('./pages/ClusterDetails').then(m => ({ default: m.ClusterDetails })));
const ClusterOverview = React.lazy(() => import('./pages/ClusterOverview').then(m => ({ default: m.ClusterOverview })));
const Topics = React.lazy(() => import('./pages/Topics').then(m => ({ default: m.Topics })));
const TopicDetails = React.lazy(() => import('./pages/TopicDetails').then(m => ({ default: m.TopicDetails })));
const Consumers = React.lazy(() => import('./pages/Consumers').then(m => ({ default: m.Consumers })));
const ConfigEditor = React.lazy(() => import('./pages/ConfigEditor').then(m => ({ default: m.ConfigEditor })));
const Partitions = React.lazy(() => import('./pages/Partitions').then(m => ({ default: m.Partitions })));
const ClusterActions = React.lazy(() => import('./pages/ClusterActions').then(m => ({ default: m.ClusterActions })));
const DeploymentLogs = React.lazy(() => import('./pages/DeploymentLogs').then(m => ({ default: m.DeploymentLogs })));
const Brokers = React.lazy(() => import('./pages/Brokers').then(m => ({ default: m.Brokers })));
const ExternalClusters = React.lazy(() => import('./pages/ExternalClusters').then(m => ({ default: m.ExternalClusters })));
const ClusterDeployment = React.lazy(() => import('./pages/ClusterDeployment').then(m => ({ default: m.ClusterDeployment })));
const LdapSettings = React.lazy(() => import('./pages/LdapSettings').then(m => ({ default: m.LdapSettings })));
const SchemaRegistry = React.lazy(() => import('./pages/SchemaRegistry').then(m => ({ default: m.SchemaRegistry })));
const KafkaConnect = React.lazy(() => import('./pages/KafkaConnect').then(m => ({ default: m.KafkaConnect })));
const UserManagement = React.lazy(() => import('./pages/UserManagement'));
const ClusterNodes = React.lazy(() => import('./pages/ClusterNodes').then(m => ({ default: m.ClusterNodes })));
const JobsList = React.lazy(() => import('./pages/JobsList').then(m => ({ default: m.JobsList })));
const JobStatusPage = React.lazy(() => import('./pages/JobStatusPage').then(m => ({ default: m.JobStatusPage })));
const ClusterSecurity = React.lazy(() => import('./pages/ClusterSecurity').then(m => ({ default: m.ClusterSecurity })));

/** Guard component: redirects External clusters away from Deployment Logs */
function DeploymentLogsGuard() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [checked, setChecked] = useState(false);
  const [allowed, setAllowed] = useState(false);

  useEffect(() => {
    if (!id) { Promise.resolve().then(() => { setChecked(true); setAllowed(false); }); return; }
    fetch(`/api/v1/ui/clusters/`+id)
      .then(res => res.json())
      .then(data => {
        if (data.mode === 'EXTERNAL') {
          navigate(`/clusters/`+id+`/overview`, { replace: true });
        } else {
          setAllowed(true);
        }
      })
      .catch(() => setAllowed(true))
      .finally(() => setChecked(true));
  }, [id, navigate]);

  if (!checked) return null;
  if (!allowed) return null;
  return <DeploymentLogs />;
}

function App() {
  const { isAdmin } = usePermissions();

  return (
    <ClusterProvider>
      <Router>
        <div className="app-container flex-col">
          <TopNavbar />
          <div className="app-body">
            <Sidebar />
            <main className="main-content">
              <Suspense fallback={<div className="animate-fade-in glass-panel" style={{ padding: '2rem', textAlign: 'center' }}><h2>Loading...</h2></div>}>
                <Routes>
                  <Route path="/" element={<Navigate to="/dashboard" replace />} />
                  <Route path="/login" element={<Navigate to="/dashboard" replace />} />
                  <Route path="/dashboard" element={<Dashboard />} />
                  <Route path="/hosts" element={<Hosts />} />
                  <Route path="/clusters" element={<Clusters />} />
                  <Route path="/cluster-deployment" element={<ClusterDeployment />} />
                  <Route path="/external-clusters" element={<ExternalClusters />} />
                  <Route path="/clusters/:id" element={<ClusterDetails />}>
                    <Route path="overview" element={<ClusterOverview />} />
                    <Route path="nodes" element={<ClusterNodes />} />
                    <Route path="brokers" element={<Brokers />} />
                    <Route path="partitions" element={<Partitions />} />
                    <Route path="topics" element={<Topics />} />
                    <Route path="topics/:topicName" element={<TopicDetails />} />
                    <Route path="consumers" element={<Consumers />} />
                    <Route path="config" element={<ConfigEditor />} />
                    <Route path="actions" element={<ClusterActions />} />
                    <Route path="logs" element={<DeploymentLogsGuard />} />
                    <Route path="security" element={<ClusterSecurity />} />
                    <Route path="schema-registry" element={<SchemaRegistry />} />
                    <Route path="kafka-connect" element={<KafkaConnect />} />
                  </Route>
                  <Route path="/artifacts" element={<Artifacts />} />
                  <Route path="/jobs" element={<JobsList />} />
                  <Route path="/jobs/:id" element={<JobStatusPage />} />
                  <Route path="/monitoring" element={<Monitoring />} />
                  <Route path="/alerts" element={<Alerts />} />
                  <Route path="/audit" element={<AuditLogs />} />
                  <Route path="/ldap-settings" element={<LdapSettings />} />
                  <Route path="/user-management" element={isAdmin ? <UserManagement /> : <Navigate to="/dashboard" replace />} />
                  {/* Fallback routes for pages not fully fleshed out yet */}
                  <Route path="*" element={
                    <div className="animate-fade-in glass-panel" style={{ padding: '2rem', textAlign: 'center' }}>
                      <h2>Page Under Construction</h2>
                      <p style={{ color: 'var(--text-secondary)', marginTop: '1rem' }}>
                        This view is being built in the background.
                      </p>
                    </div>
                  } />
                </Routes>
              </Suspense>
            </main>
          </div>
          <GlobalConfirmDialog />
        </div>
      </Router>
    </ClusterProvider>
  );
}

export default App;
"@;
[System.IO.File]::WriteAllText("d:\AIRTEL PAYMENTS BANK - KAFKA - TANTOR\Tantor_kafka\tantor-ui\src\App.tsx", $appContent, [System.Text.UTF8Encoding]::new($false))
