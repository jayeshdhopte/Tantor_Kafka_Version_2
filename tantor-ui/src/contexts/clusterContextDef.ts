import { createContext } from 'react';

export interface Cluster {
  id: string;
  name: string;
  status: string;
  kafkaVersion: string;
  mode: string;
  [key: string]: unknown;
}

export interface ClusterContextProps {
  clusters: Cluster[];
  activeClusterId: string | null;
  setActiveClusterId: (id: string | null) => void;
  activeClusterMode: string | null;
  isExternalCluster: boolean;
  loading: boolean;
}

export const ClusterContext = createContext<ClusterContextProps>({
  clusters: [],
  activeClusterId: null,
  setActiveClusterId: () => {},
  activeClusterMode: null,
  isExternalCluster: false,
  loading: true,
});
