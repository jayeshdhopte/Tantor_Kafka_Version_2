-- One parent owns both candidate roles. Existing controller-only rows remain valid.
ALTER TABLE kf_external_controller_operations ADD COLUMN broker_node_id integer CHECK(broker_node_id>0 AND broker_node_id<>node_id);
ALTER TABLE kf_external_controller_operations ADD COLUMN published_broker_node_id uuid;
CREATE FUNCTION guard_external_separate_identity() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE plan jsonb; key text;
BEGIN
 IF TG_OP='UPDATE' AND NEW.broker_node_id IS DISTINCT FROM OLD.broker_node_id THEN RAISE EXCEPTION 'Frozen broker identity cannot change'; END IF;
 SELECT p.plan::jsonb INTO plan FROM kf_external_controller_previews p WHERE p.id=NEW.id;
 IF plan->>'layout'='SEPARATE' THEN
   IF NEW.broker_node_id IS NULL OR NEW.broker_node_id IS DISTINCT FROM (plan->'processes'->1->>'nodeId')::integer THEN RAISE EXCEPTION 'Both frozen process identities must be reserved'; END IF;
 ELSE
   IF NEW.broker_node_id IS NOT NULL THEN RAISE EXCEPTION 'Controller-only operation cannot reserve a broker'; END IF;
 END IF;
 IF NEW.broker_node_id IS NOT NULL AND NEW.state<>'CANCELLED_SAFE' THEN
   key='node:'||NEW.cluster_id||':'||NEW.broker_node_id;
   INSERT INTO kf_external_add_node_resources AS r VALUES(key,NEW.id,true)
     ON CONFLICT(resource_key) DO UPDATE SET permanent=true WHERE r.operation_id=EXCLUDED.operation_id;
   IF NOT FOUND THEN RAISE EXCEPTION 'External broker identity is retained: %',key; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER external_separate_identity BEFORE INSERT OR UPDATE ON kf_external_controller_operations FOR EACH ROW EXECUTE FUNCTION guard_external_separate_identity();
