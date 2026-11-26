import './ClusterLoadingState.css';

/** Avoid flashing loading text on fast cluster navigation. */
export function ClusterLoadingState({ overview = false }: { overview?: boolean }) {
  return <div className="cluster-loading-state" role="status" aria-label={overview ? 'Loading cluster overview' : 'Loading cluster'}>
    <div className="cluster-loading-placeholder" aria-hidden="true">
      <div className="cluster-loading-line" />
      <div className="cluster-loading-line cluster-loading-line-short" />
      <div className="cluster-loading-panels"><span /><span /><span /></div>
    </div>
  </div>;
}
