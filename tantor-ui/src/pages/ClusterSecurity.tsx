import { useParams } from 'react-router-dom';
import SecurityManager from '../components/SecurityManager';

export function ClusterSecurity() {
  const { id } = useParams<{ id: string }>();

  if (!id) return null;

  return (
    <div className="cluster-security-page animate-fade-in" style={{ padding: '1rem' }}>
      <SecurityManager clusterId={id} />
    </div>
  );
}
