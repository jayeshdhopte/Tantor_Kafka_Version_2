ALTER TABLE tasks
    ADD COLUMN cluster_id UUID REFERENCES clusters(id) ON DELETE SET NULL;

UPDATE tasks t
SET cluster_id = h.cluster_id
FROM hosts h
WHERE t.host_id = h.id
  AND t.cluster_id IS NULL;

CREATE INDEX idx_tasks_cluster ON tasks(cluster_id);

ALTER TABLE clusters DROP CONSTRAINT IF EXISTS clusters_name_key;

CREATE UNIQUE INDEX ux_clusters_active_name
    ON clusters(name)
    WHERE COALESCE(status, 'PENDING') <> 'DELETED';
