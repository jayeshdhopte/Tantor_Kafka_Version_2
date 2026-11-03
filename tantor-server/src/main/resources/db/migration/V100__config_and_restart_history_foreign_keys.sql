-- Enforce configuration/restart history links without changing existing rows.
-- Deferred NO ACTION checks preserve atomic multi-table writes and history.
-- A validation error aborts this migration; no orphan is deleted or repaired.

ALTER TABLE public.kf_config_change ADD CONSTRAINT uq_cfg_change_cluster UNIQUE (change_id,cluster_id);
ALTER TABLE public.kf_config_change_detail ADD CONSTRAINT uq_cfg_detail_owner UNIQUE (change_id,detail_id);
ALTER TABLE public.kf_config_change_target_node ADD CONSTRAINT uq_cfg_target_owner UNIQUE (change_id,target_id);
ALTER TABLE public.kf_rolling_restart_operation ADD CONSTRAINT uq_cfg_operation_owner UNIQUE (change_id,restart_operation_id);

-- Individual links also protect populated references when other nullable fields are absent.
ALTER TABLE public.kf_config_change ADD CONSTRAINT fk_cfg_change_cluster
    FOREIGN KEY (cluster_id) REFERENCES public.kf_clusters (id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_change_detail ADD CONSTRAINT fk_cfg_detail_change
    FOREIGN KEY (change_id) REFERENCES public.kf_config_change (change_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_change_detail ADD CONSTRAINT fk_cfg_detail_catalog
    FOREIGN KEY (config_catalog_id) REFERENCES public.kf_config_catalog (config_catalog_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_change_target_node ADD CONSTRAINT fk_cfg_target_change
    FOREIGN KEY (change_id) REFERENCES public.kf_config_change (change_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_change_node_value ADD CONSTRAINT fk_cfg_value_change
    FOREIGN KEY (change_id) REFERENCES public.kf_config_change (change_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_change_node_value ADD CONSTRAINT fk_cfg_value_detail
    FOREIGN KEY (detail_id) REFERENCES public.kf_config_change_detail (detail_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_change_node_value ADD CONSTRAINT fk_cfg_value_target
    FOREIGN KEY (target_id) REFERENCES public.kf_config_change_target_node (target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_execution_attempt ADD CONSTRAINT fk_cfg_attempt_change
    FOREIGN KEY (change_id) REFERENCES public.kf_config_change (change_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_execution_attempt ADD CONSTRAINT fk_cfg_attempt_detail
    FOREIGN KEY (detail_id) REFERENCES public.kf_config_change_detail (detail_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_execution_attempt ADD CONSTRAINT fk_cfg_attempt_target
    FOREIGN KEY (target_id) REFERENCES public.kf_config_change_target_node (target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_backup ADD CONSTRAINT fk_cfg_backup_change
    FOREIGN KEY (change_id) REFERENCES public.kf_config_change (change_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_backup ADD CONSTRAINT fk_cfg_backup_detail
    FOREIGN KEY (detail_id) REFERENCES public.kf_config_change_detail (detail_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_backup ADD CONSTRAINT fk_cfg_backup_target
    FOREIGN KEY (target_id) REFERENCES public.kf_config_change_target_node (target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_cluster_operation_lock ADD CONSTRAINT fk_cfg_lock_cluster
    FOREIGN KEY (cluster_id) REFERENCES public.kf_clusters (id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_cluster_operation_lock ADD CONSTRAINT fk_cfg_lock_change
    FOREIGN KEY (change_id) REFERENCES public.kf_config_change (change_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_health_check ADD CONSTRAINT fk_cfg_health_change
    FOREIGN KEY (change_id) REFERENCES public.kf_config_change (change_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_health_check ADD CONSTRAINT fk_cfg_health_target
    FOREIGN KEY (target_id) REFERENCES public.kf_config_change_target_node (target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_rolling_restart_operation ADD CONSTRAINT fk_cfg_restart_change
    FOREIGN KEY (change_id) REFERENCES public.kf_config_change (change_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_rolling_restart_operation ADD CONSTRAINT fk_cfg_restart_current
    FOREIGN KEY (current_target_id) REFERENCES public.kf_config_change_target_node (target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_rolling_restart_node_step ADD CONSTRAINT fk_cfg_restart_step_change
    FOREIGN KEY (change_id) REFERENCES public.kf_config_change (change_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_rolling_restart_node_step ADD CONSTRAINT fk_cfg_restart_step_operation
    FOREIGN KEY (restart_operation_id) REFERENCES public.kf_rolling_restart_operation (restart_operation_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_rolling_restart_node_step ADD CONSTRAINT fk_cfg_restart_step_target
    FOREIGN KEY (target_id) REFERENCES public.kf_config_change_target_node (target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_rolling_restart_table ADD CONSTRAINT fk_cfg_standalone_cluster
    FOREIGN KEY (cluster_id) REFERENCES public.kf_clusters (id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_change_node_value ADD CONSTRAINT fk_cfg_value_detail_owner
    FOREIGN KEY (change_id,detail_id) REFERENCES public.kf_config_change_detail (change_id,detail_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_change_node_value ADD CONSTRAINT fk_cfg_value_target_owner
    FOREIGN KEY (change_id,target_id) REFERENCES public.kf_config_change_target_node (change_id,target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_execution_attempt ADD CONSTRAINT fk_cfg_attempt_detail_owner
    FOREIGN KEY (change_id,detail_id) REFERENCES public.kf_config_change_detail (change_id,detail_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_execution_attempt ADD CONSTRAINT fk_cfg_attempt_target_owner
    FOREIGN KEY (change_id,target_id) REFERENCES public.kf_config_change_target_node (change_id,target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_backup ADD CONSTRAINT fk_cfg_backup_detail_owner
    FOREIGN KEY (change_id,detail_id) REFERENCES public.kf_config_change_detail (change_id,detail_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_config_backup ADD CONSTRAINT fk_cfg_backup_target_owner
    FOREIGN KEY (change_id,target_id) REFERENCES public.kf_config_change_target_node (change_id,target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_health_check ADD CONSTRAINT fk_cfg_health_target_owner
    FOREIGN KEY (change_id,target_id) REFERENCES public.kf_config_change_target_node (change_id,target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_rolling_restart_operation ADD CONSTRAINT fk_cfg_restart_current_owner
    FOREIGN KEY (change_id,current_target_id) REFERENCES public.kf_config_change_target_node (change_id,target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_rolling_restart_node_step ADD CONSTRAINT fk_cfg_restart_step_operation_owner
    FOREIGN KEY (change_id,restart_operation_id) REFERENCES public.kf_rolling_restart_operation (change_id,restart_operation_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_rolling_restart_node_step ADD CONSTRAINT fk_cfg_restart_step_target_owner
    FOREIGN KEY (change_id,target_id) REFERENCES public.kf_config_change_target_node (change_id,target_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE public.kf_cluster_operation_lock ADD CONSTRAINT fk_cfg_lock_owner
    FOREIGN KEY (change_id,cluster_id) REFERENCES public.kf_config_change (change_id,cluster_id)
    ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID;

-- Reuse existing non-partial indexes whose leading columns already cover the FK.
DO $$
DECLARE item RECORD;
BEGIN
    FOR item IN SELECT * FROM (VALUES
        ('kf_config_change', ARRAY['cluster_id']::text[], 'ix_fk_cfg_change_cluster'),
        ('kf_config_change_detail', ARRAY['change_id']::text[], 'ix_fk_cfg_detail_change'),
        ('kf_config_change_detail', ARRAY['config_catalog_id']::text[], 'ix_fk_cfg_detail_catalog'),
        ('kf_config_change_target_node', ARRAY['change_id']::text[], 'ix_fk_cfg_target_change'),
        ('kf_config_change_node_value', ARRAY['change_id']::text[], 'ix_fk_cfg_value_change'),
        ('kf_config_change_node_value', ARRAY['detail_id']::text[], 'ix_fk_cfg_value_detail'),
        ('kf_config_change_node_value', ARRAY['target_id']::text[], 'ix_fk_cfg_value_target'),
        ('kf_config_execution_attempt', ARRAY['change_id']::text[], 'ix_fk_cfg_attempt_change'),
        ('kf_config_execution_attempt', ARRAY['detail_id']::text[], 'ix_fk_cfg_attempt_detail'),
        ('kf_config_execution_attempt', ARRAY['target_id']::text[], 'ix_fk_cfg_attempt_target'),
        ('kf_config_backup', ARRAY['change_id']::text[], 'ix_fk_cfg_backup_change'),
        ('kf_config_backup', ARRAY['detail_id']::text[], 'ix_fk_cfg_backup_detail'),
        ('kf_config_backup', ARRAY['target_id']::text[], 'ix_fk_cfg_backup_target'),
        ('kf_cluster_operation_lock', ARRAY['cluster_id']::text[], 'ix_fk_cfg_lock_cluster'),
        ('kf_cluster_operation_lock', ARRAY['change_id']::text[], 'ix_fk_cfg_lock_change'),
        ('kf_health_check', ARRAY['change_id']::text[], 'ix_fk_cfg_health_change'),
        ('kf_health_check', ARRAY['target_id']::text[], 'ix_fk_cfg_health_target'),
        ('kf_rolling_restart_operation', ARRAY['change_id']::text[], 'ix_fk_cfg_restart_change'),
        ('kf_rolling_restart_operation', ARRAY['current_target_id']::text[], 'ix_fk_cfg_restart_current'),
        ('kf_rolling_restart_node_step', ARRAY['change_id']::text[], 'ix_fk_cfg_restart_step_change'),
        ('kf_rolling_restart_node_step', ARRAY['restart_operation_id']::text[], 'ix_fk_cfg_restart_step_operation'),
        ('kf_rolling_restart_node_step', ARRAY['target_id']::text[], 'ix_fk_cfg_restart_step_target'),
        ('kf_rolling_restart_table', ARRAY['cluster_id']::text[], 'ix_fk_cfg_standalone_cluster'),
        ('kf_config_change_node_value', ARRAY['change_id','detail_id']::text[], 'ix_fk_cfg_value_detail_owner'),
        ('kf_config_change_node_value', ARRAY['change_id','target_id']::text[], 'ix_fk_cfg_value_target_owner'),
        ('kf_config_execution_attempt', ARRAY['change_id','detail_id']::text[], 'ix_fk_cfg_attempt_detail_owner'),
        ('kf_config_execution_attempt', ARRAY['change_id','target_id']::text[], 'ix_fk_cfg_attempt_target_owner'),
        ('kf_config_backup', ARRAY['change_id','detail_id']::text[], 'ix_fk_cfg_backup_detail_owner'),
        ('kf_config_backup', ARRAY['change_id','target_id']::text[], 'ix_fk_cfg_backup_target_owner'),
        ('kf_health_check', ARRAY['change_id','target_id']::text[], 'ix_fk_cfg_health_target_owner'),
        ('kf_rolling_restart_operation', ARRAY['change_id','current_target_id']::text[], 'ix_fk_cfg_restart_current_owner'),
        ('kf_rolling_restart_node_step', ARRAY['change_id','restart_operation_id']::text[], 'ix_fk_cfg_restart_step_operation_owner'),
        ('kf_rolling_restart_node_step', ARRAY['change_id','target_id']::text[], 'ix_fk_cfg_restart_step_target_owner'),
        ('kf_cluster_operation_lock', ARRAY['change_id','cluster_id']::text[], 'ix_fk_cfg_lock_owner')
    ) AS references_to_index(table_name, columns, index_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_index i WHERE i.indrelid = ('public.' || item.table_name)::regclass
                AND i.indisvalid AND i.indpred IS NULL AND i.indnkeyatts >= cardinality(item.columns)
                AND (SELECT array_agg(a.attname::text ORDER BY position)
                     FROM unnest(i.indkey) WITH ORDINALITY AS keys(attnum, position)
                     JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = keys.attnum
                     WHERE position <= cardinality(item.columns)) = item.columns
        ) THEN
            EXECUTE format('CREATE INDEX %I ON public.%I (%s)', item.index_name, item.table_name,
                (SELECT string_agg(quote_ident(value), ',') FROM unnest(item.columns) AS value));
        END IF;
    END LOOP;
END;
$$;

ALTER TABLE public.kf_config_change VALIDATE CONSTRAINT fk_cfg_change_cluster;
ALTER TABLE public.kf_config_change_detail VALIDATE CONSTRAINT fk_cfg_detail_change;
ALTER TABLE public.kf_config_change_detail VALIDATE CONSTRAINT fk_cfg_detail_catalog;
ALTER TABLE public.kf_config_change_target_node VALIDATE CONSTRAINT fk_cfg_target_change;
ALTER TABLE public.kf_config_change_node_value VALIDATE CONSTRAINT fk_cfg_value_change;
ALTER TABLE public.kf_config_change_node_value VALIDATE CONSTRAINT fk_cfg_value_detail;
ALTER TABLE public.kf_config_change_node_value VALIDATE CONSTRAINT fk_cfg_value_target;
ALTER TABLE public.kf_config_execution_attempt VALIDATE CONSTRAINT fk_cfg_attempt_change;
ALTER TABLE public.kf_config_execution_attempt VALIDATE CONSTRAINT fk_cfg_attempt_detail;
ALTER TABLE public.kf_config_execution_attempt VALIDATE CONSTRAINT fk_cfg_attempt_target;
ALTER TABLE public.kf_config_backup VALIDATE CONSTRAINT fk_cfg_backup_change;
ALTER TABLE public.kf_config_backup VALIDATE CONSTRAINT fk_cfg_backup_detail;
ALTER TABLE public.kf_config_backup VALIDATE CONSTRAINT fk_cfg_backup_target;
ALTER TABLE public.kf_cluster_operation_lock VALIDATE CONSTRAINT fk_cfg_lock_cluster;
ALTER TABLE public.kf_cluster_operation_lock VALIDATE CONSTRAINT fk_cfg_lock_change;
ALTER TABLE public.kf_health_check VALIDATE CONSTRAINT fk_cfg_health_change;
ALTER TABLE public.kf_health_check VALIDATE CONSTRAINT fk_cfg_health_target;
ALTER TABLE public.kf_rolling_restart_operation VALIDATE CONSTRAINT fk_cfg_restart_change;
ALTER TABLE public.kf_rolling_restart_operation VALIDATE CONSTRAINT fk_cfg_restart_current;
ALTER TABLE public.kf_rolling_restart_node_step VALIDATE CONSTRAINT fk_cfg_restart_step_change;
ALTER TABLE public.kf_rolling_restart_node_step VALIDATE CONSTRAINT fk_cfg_restart_step_operation;
ALTER TABLE public.kf_rolling_restart_node_step VALIDATE CONSTRAINT fk_cfg_restart_step_target;
ALTER TABLE public.kf_rolling_restart_table VALIDATE CONSTRAINT fk_cfg_standalone_cluster;
ALTER TABLE public.kf_config_change_node_value VALIDATE CONSTRAINT fk_cfg_value_detail_owner;
ALTER TABLE public.kf_config_change_node_value VALIDATE CONSTRAINT fk_cfg_value_target_owner;
ALTER TABLE public.kf_config_execution_attempt VALIDATE CONSTRAINT fk_cfg_attempt_detail_owner;
ALTER TABLE public.kf_config_execution_attempt VALIDATE CONSTRAINT fk_cfg_attempt_target_owner;
ALTER TABLE public.kf_config_backup VALIDATE CONSTRAINT fk_cfg_backup_detail_owner;
ALTER TABLE public.kf_config_backup VALIDATE CONSTRAINT fk_cfg_backup_target_owner;
ALTER TABLE public.kf_health_check VALIDATE CONSTRAINT fk_cfg_health_target_owner;
ALTER TABLE public.kf_rolling_restart_operation VALIDATE CONSTRAINT fk_cfg_restart_current_owner;
ALTER TABLE public.kf_rolling_restart_node_step VALIDATE CONSTRAINT fk_cfg_restart_step_operation_owner;
ALTER TABLE public.kf_rolling_restart_node_step VALIDATE CONSTRAINT fk_cfg_restart_step_target_owner;
ALTER TABLE public.kf_cluster_operation_lock VALIDATE CONSTRAINT fk_cfg_lock_owner;

COMMENT ON COLUMN public.kf_config_change_target_node.node_id IS 'Historical inventory UUID snapshot, nullable if inventory was unresolved. Intentionally not an FK to ephemeral kf_nodes: cluster purge retires live inventory; frozen roles and job/task payloads retain historical identity.';
COMMENT ON COLUMN public.kf_config_change.cluster_id IS 'Validated FK to kf_clusters.id; referenced cluster parents are retained as DELETED when live inventory is retired.';
COMMENT ON COLUMN public.kf_rolling_restart_table.cluster_id IS 'Validated FK to kf_clusters.id; standalone restart history retains its original cluster parent.';
