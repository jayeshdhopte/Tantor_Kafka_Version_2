CREATE UNIQUE INDEX IF NOT EXISTS ux_kf_clusters_active_name_ci
    ON kf_clusters (LOWER(BTRIM(cluster_name)))
    WHERE status IS DISTINCT FROM 'DELETED'
      AND BTRIM(cluster_name) <> '';
