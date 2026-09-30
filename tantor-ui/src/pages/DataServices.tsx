import { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { Database, Activity, Box, Server, Settings, Layers, HardDrive, CheckCircle2, X } from 'lucide-react';
import { notifyAction } from '../components/confirmUtils';
import './DataServices.css';

interface DataServiceHost {
  id: string;
  hostname?: string;
  ipAddresses?: string;
  status?: string;
}

export function DataServices() {
  const [step, setStep] = useState(1);
  const [selectedService, setSelectedService] = useState<string | null>(null);
  const [hosts, setHosts] = useState<DataServiceHost[]>([]);
  const [isHostModalOpen, setIsHostModalOpen] = useState(false);
  const [selectedHostId, setSelectedHostId] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/v1/ui/hosts')
      .then(res => res.json())
      .then(data => setHosts(data))
      .catch(err => console.error(err));
  }, []);

  const availableServices = [
    { id: 'atlas', name: 'Atlas', desc: 'Metadata management and governance services', icon: Database },
    { id: 'cruise', name: 'Cruise Control', desc: 'Simplifies the operation of Kafka clusters by automating workload rebalancing', icon: Activity },
    { id: 'flink', name: 'Flink', desc: 'Distributed processing engine for stateful computations over unbounded streams', icon: Layers },
    { id: 'hbase', name: 'HBase', desc: 'Highly scalable, highly resilient NoSQL database', icon: Database },
    { id: 'hdfs', name: 'HDFS', desc: 'Hadoop Distributed File System for reliable storage', icon: HardDrive },
    { id: 'hive', name: 'Hive', desc: 'SQL based data warehouse system', icon: Box },
    { id: 'kafka', name: 'Kafka', desc: 'Distributed event streaming platform for high-performance data pipelines', icon: Activity },
    { id: 'ranger', name: 'Ranger KMS', desc: 'Key Management Service for Hadoop ecosystem', icon: Settings },
    { id: 'zookeeper', name: 'Zookeeper', desc: 'Centralized service for maintaining configuration information', icon: Server },
  ];

  return (
    <div className="wizard-container animate-fade-in">
      <header className="page-header cloudera-header">
        <h1>Add Service to Tantor Runtime</h1>
      </header>

      <div className="wizard-layout">
        {/* Left Sidebar Steps */}
        <div className="wizard-steps-sidebar">
          <ul>
            <li className={step === 1 ? 'active' : step > 1 ? 'completed' : ''}>
              <span className="step-circle">{step > 1 ? <CheckCircle2 size={16}/> : 1}</span> 
              Select Dependencies
            </li>
            <li className={step === 2 ? 'active' : step > 2 ? 'completed' : ''}>
              <span className="step-circle">{step > 2 ? <CheckCircle2 size={16}/> : 2}</span> 
              Assign Roles
            </li>
            <li className={step === 3 ? 'active' : step > 3 ? 'completed' : ''}>
              <span className="step-circle">{step > 3 ? <CheckCircle2 size={16}/> : 3}</span> 
              Setup Database
            </li>
            <li className={step === 4 ? 'active' : step > 4 ? 'completed' : ''}>
              <span className="step-circle">{step > 4 ? <CheckCircle2 size={16}/> : 4}</span> 
              Review Changes
            </li>
            <li className={step === 5 ? 'active' : ''}>
              <span className="step-circle">5</span> 
              Command Details
            </li>
          </ul>
        </div>

        {/* Main Content Area */}
        <div className="wizard-content">
          {step === 1 && (
            <div className="step-pane">
              <h2>Select the type of service you want to add.</h2>
              
              <div className="service-selection-table">
                <div className="table-header">
                  <div></div>
                  <div>Service Type</div>
                  <div>Description</div>
                </div>
                {availableServices.map((svc) => (
                  <label key={svc.id} className={`table-row ${selectedService === svc.id ? 'selected' : ''}`}>
                    <div className="radio-col">
                      <input 
                        type="radio" 
                        name="service_selection" 
                        checked={selectedService === svc.id}
                        onChange={() => setSelectedService(svc.id)}
                      />
                    </div>
                    <div className="name-col">
                      <svc.icon size={16} style={{ color: '#1967D2', marginRight: '8px' }} />
                      {svc.name}
                    </div>
                    <div className="desc-col">{svc.desc}</div>
                  </label>
                ))}
              </div>

              <div className="wizard-footer">
                <button className="btn-secondary" onClick={() => window.history.back()}>Back</button>
                <button 
                  className="btn-primary" 
                  disabled={!selectedService}
                  onClick={() => setStep(2)}
                >
                  Continue
                </button>
              </div>
            </div>
          )}

          {step === 2 && (
            <div className="step-pane">
              <h2>Assign Roles</h2>
              <p>Select which hosts will run the {availableServices.find(s => s.id === selectedService)?.name} roles.</p>
              
              <div className="role-assignment-box">
                <div className="role-header">
                  <strong>Master Node</strong>
                  <button className="btn-small" onClick={() => setIsHostModalOpen(true)}>Select Host</button>
                </div>
                <div className="role-body" style={selectedHostId ? { color: '#222', fontStyle: 'normal' } : {}}>
                  {selectedHostId ? (
                    <span>
                      <Server size={16} style={{ color: '#1967D2', verticalAlign: 'middle', marginRight: '8px' }}/>
                      <strong>{hosts.find(h => h.id === selectedHostId)?.hostname || selectedHostId}</strong> assigned to Master Node.
                    </span>
                  ) : (
                    'No host selected. Click "Select Host" to assign this role.'
                  )}
                </div>
              </div>

              <div className="wizard-footer">
                <button className="btn-secondary" onClick={() => setStep(1)}>Back</button>
                <button className="btn-primary" onClick={() => setStep(3)}>Continue</button>
              </div>
            </div>
          )}

          {step === 3 && (
            <div className="step-pane">
              <h2>Setup Database (Optional)</h2>
              <p>Configure database connections if this service requires external storage.</p>
              
              {['kafka', 'zookeeper', 'cruise', 'flink', 'hdfs'].includes(selectedService || '') ? (
                <div className="role-assignment-box" style={{ background: '#F8F9FA', marginTop: '2rem' }}>
                  <div className="role-body" style={{ color: '#0F9D58', fontStyle: 'normal', fontSize: '1.1rem' }}>
                    <CheckCircle2 size={24} style={{ verticalAlign: 'middle', marginRight: '8px' }}/>
                    <strong>{availableServices.find(s => s.id === selectedService)?.name}</strong> does not require an external relational database.
                    <br/><span style={{ fontSize: '0.9rem', color: '#5E6470', marginLeft: '32px' }}>You can safely skip this step and continue to deployment.</span>
                  </div>
                </div>
              ) : (
                <div className="form-grid">
                  <div className="form-group">
                    <label>Database Type</label>
                    <select className="form-control"><option>MySQL</option><option>PostgreSQL</option></select>
                  </div>
                  <div className="form-group">
                    <label>Database Hostname</label>
                    <input type="text" className="form-control" placeholder="db.translab.local:3306" />
                  </div>
                  <div className="form-group">
                    <label>Database Name</label>
                    <input type="text" className="form-control" placeholder="rangerkms" />
                  </div>
                  <div className="form-group">
                    <label>Username</label>
                    <input type="text" className="form-control" placeholder="admin" />
                  </div>
                </div>
              )}

              <div className="wizard-footer">
                <button className="btn-secondary" onClick={() => setStep(2)}>Back</button>
                <button className="btn-primary" onClick={() => setStep(4)}>Continue</button>
              </div>
            </div>
          )}

          {step > 3 && (
            <div className="step-pane">
              <h2>Review &amp; Deploy</h2>
              <p>You are about to deploy <strong>{availableServices.find(s => s.id === selectedService)?.name}</strong>.</p>
              
              <div className="role-assignment-box" style={{ marginTop: '20px' }}>
                <div className="role-header" style={{ background: '#E8F0FE', color: '#1967D2' }}>
                  <strong>Deployment Summary</strong>
                </div>
                <div className="role-body" style={{ textAlign: 'left', fontStyle: 'normal' }}>
                  <p><strong>Service:</strong> {availableServices.find(s => s.id === selectedService)?.name}</p>
                  <p><strong>Target Host:</strong> {hosts.find(h => h.id === selectedHostId)?.hostname || 'None selected'}</p>
                  <p><strong>Action:</strong> Backend will send deployment artifact and configuration tasks to the Tantor Agent on the target host.</p>
                </div>
              </div>

              <div className="wizard-footer">
                <button className="btn-secondary" onClick={() => setStep(3)}>Back</button>
                <button 
                  className="btn-primary" 
                  onClick={async () => {
                    notifyAction('Deployment initialized! In a real scenario, this would trigger /api/v1/ui/clusters/deploy with the selected host ' + selectedHostId);
                    window.location.href = '/';
                  }}
                >
                  Deploy Service
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Host Selection Modal — portal to escape scroll container */}
      {isHostModalOpen ? createPortal(
        <div className="modal-overlay">
          <div className="modal-content cloudera-modal">
            <div className="modal-header">
              <h3>Select Host for Master Node</h3>
              <button className="icon-btn" onClick={() => setIsHostModalOpen(false)}>
                <X size={20} />
              </button>
            </div>
            <div className="modal-body">
              {hosts.length === 0 ? (
                <p>No hosts available. Please add hosts first.</p>
              ) : (
                <div className="host-list">
                  {hosts.map(host => (
                    <label key={host.id} className={`host-row ${selectedHostId === host.id ? 'selected' : ''}`}>
                      <input 
                        type="radio" 
                        name="host_select" 
                        checked={selectedHostId === host.id}
                        onChange={() => setSelectedHostId(host.id)}
                      />
                      <span className="host-name">{host.hostname}</span>
                      <span className="host-ip">{host.ipAddresses}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
            <div className="modal-footer">
              <button className="btn-secondary" onClick={() => setIsHostModalOpen(false)}>Cancel</button>
              <button className="btn-primary" onClick={() => setIsHostModalOpen(false)}>Select</button>
            </div>
          </div>
        </div>
      , document.body) : null}
    </div>
  );
}
