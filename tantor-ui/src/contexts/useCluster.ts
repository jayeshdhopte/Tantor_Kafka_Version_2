import { useContext } from 'react';
import { ClusterContext } from './clusterContextDef';

export const useCluster = () => useContext(ClusterContext);
