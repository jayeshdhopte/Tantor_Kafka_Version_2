interface DeploymentJob { clusterId: string; type: string; operation?: string }
interface DeploymentCluster { id: string; mode: string; status: string; deploymentInProgress?: boolean }

export function isDeploymentJob(job: Pick<DeploymentJob, 'type' | 'operation'>) {
  return job.type === 'DEPLOYMENT' && job.operation !== 'ADD_SERVICE';
}

export function isClusterDeploying(cluster: DeploymentCluster, jobs: DeploymentJob[] = []) {
  if (cluster.mode === 'EXTERNAL') return false;
  return Boolean(cluster.deploymentInProgress)
    || jobs.some(job => job.clusterId === cluster.id && isDeploymentJob(job))
    || ['PENDING', 'RUNNING', 'IN_PROGRESS', 'VALIDATING', 'DEPLOYING'].includes(cluster.status.toUpperCase());
}
