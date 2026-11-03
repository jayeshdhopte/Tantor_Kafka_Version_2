package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.regex.Pattern;

/** Additional ER projections. Existing jobs, commands, checkpoints and locks stay authoritative. */
@Service
@RequiredArgsConstructor
public class ConfigChangeMappingService {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private static final Set<JobType> TYPES = Set.of(JobType.TARGETED_CONFIG_CHANGE, JobType.CONFIG_CHANGE, JobType.ROLLING_CONFIG_UPDATE);
    private static final Set<String> JSON = Set.of("catalog_snapshot", "requested_payload", "response_payload", "metrics", "old_config", "new_config", "validation_result");
    private static final Pattern BACKUP = Pattern.compile("(?m)^(?:\\[[^\\]\\r\\n]+\\]\\s*)?(?:Rollback checkpoint \\(7-day retention\\): |Configuration backup \\(7-day retention\\): |Live dynamic property checkpoint: |Disk checkpoint \\(7-day retention\\): |Dynamic checkpoint \\(7-day retention\\): |Existing config backed up to |Checkpoint on [^\\r\\n]+?: )(/[^\\r\\n]+)$");

    public static boolean supports(Job job) { return job != null && job.getType() != null && TYPES.contains(job.getType()); }

    @Transactional
    public void record(Job job, List<JobStep> steps) {
        if (!supports(job)) return;
        Map<String,Object> request = read(job.getPayload());
        UUID cluster = cluster(job);
        boolean restart = steps.stream().anyMatch(s -> isStatic(normalize(job,s)));
        String classification = text(request.get("classification"));
        if (classification == null) classification = restart ? "ROLLING_RESTART_REQUIRED" : "DYNAMIC_ALLOWED";
        insert("kf_config_change", "change_id", fields("change_id",job.getId(),"cluster_id",cluster,
                "title",request.get("key"),"target_role",request.get("role"),"target_scope",request.get("scope"),
                "execution_type",classification,"status",job.getStatus().name(),"requested_by",job.getRequestedBy(),
                "requested_at",time(job.getCreatedAt()),"created_at",time(job.getCreatedAt()),"updated_at",time(job.getUpdatedAt())));
        if (request.get("configVersionId") != null) {
            jdbc.update("UPDATE kf_config_change SET version_no=v.config_version, approved_at=v.approved_at FROM kf_config_versions v WHERE change_id=? AND v.id=?::uuid",
                    job.getId(),request.get("configVersionId").toString());
        }
        Set<UUID> staticTargets = new LinkedHashSet<>();
        var sequenceByKey = new LinkedHashMap<String,Integer>();
        var desiredByKey = new LinkedHashMap<String,Set<Object>>();
        var catalogs = new HashMap<String,Long>();
        var writtenDetails = new HashSet<UUID>();
        var sensitiveKeys = new HashSet<String>();
        for (var step:steps) for (var target:targets(job,step,normalize(job,step)))
            changes(target).forEach((key,value)->{desiredByKey.computeIfAbsent(key,k->new LinkedHashSet<>()).add(value);if(sensitive(key,target))sensitiveKeys.add(key);});
        for (var step : steps) {
            Map<String,Object> payload = normalize(job,step);
            if (payload.isEmpty()) continue;
            for (Map<String,Object> target : targets(job,step,payload)) {
                UUID targetId = target(job,step,target);
                recordTargetedVersion(job, step, target);
                if (isStatic(target)) {
                    staticTargets.add(targetId);
                    insert("kf_rolling_restart_node_step","restart_step_id",fields("restart_step_id",targetId,
                            "restart_operation_id",job.getId(),"change_id",job.getId(),"target_id",targetId,
                            "planned_order",step.getStepOrder(),"step_status",step.getStatus().name(),
                            "created_at",time(step.getCreatedAt()),"updated_at",time(step.getUpdatedAt())));
                }
                for (var change : changes(target).entrySet()) {
                    String key = change.getKey(); boolean secret = sensitiveKeys.contains(key);
                    String role = text(request.get("role"));
                    if (role == null) role = text(target.get("role"));
                    if (role != null && role.length()>16) role = isStatic(target)?null:"BROKER";
                    String policy = isStatic(target) ? "ROLLING_RESTART_REQUIRED" : "DYNAMIC_ALLOWED";
                    String context=key+":"+role+":"+policy;
                    Long catalog=catalogs.get(context);
                    if (catalog==null) {catalog=catalog(key,role,policy,secret);catalogs.put(context,catalog);}
                    Set<Object> desired=desiredByKey.get(key);
                    Object common=desired.size()==1?desired.iterator().next():null;
                    String operation=desired.contains(null)?(desired.size()==1?"REMOVE":null):"SET";
                    UUID detail = id(job.getId(),"detail:"+key);
                    if (writtenDetails.add(detail)) insert("kf_config_change_detail","detail_id",fields("detail_id",detail,"change_id",job.getId(),
                            "config_catalog_id",catalog,"config_key",key,"operation",operation,
                            "desired_value",secret?null:text(common),"is_sensitive",secret,"derived_execution_type",policy,
                            "sequence_no",sequenceByKey.computeIfAbsent(key,k->sequenceByKey.size()+1),"catalog_snapshot",fields("config_key",key,"applicable_role",role,"execution_policy",policy),
                            "created_at",time(job.getCreatedAt())));
                    var previous = previous(target,key);
                    insert("kf_config_change_node_value","node_value_id",fields("node_value_id",id(targetId,"value:"+key),
                            "change_id",job.getId(),"detail_id",detail,"target_id",targetId,"is_sensitive",secret,
                            "value_before",secret?null:previous.get("value"),"source_before",previous.get("source"),
                            "before_present",previous.get("present"),"desired_value",secret?null:text(change.getValue()),
                            "created_at",time(job.getCreatedAt()),"updated_at",time(job.getUpdatedAt())));
                }
            }
        }
        if (!staticTargets.isEmpty()) insert("kf_rolling_restart_operation","restart_operation_id",fields(
                "restart_operation_id",job.getId(),"change_id",job.getId(),"restart_status",job.getStatus().name(),
                "target_count",staticTargets.size(),"completed_count",0,"created_at",time(job.getCreatedAt()),"updated_at",time(job.getUpdatedAt())));
        synchronizeOperation(job);
    }

    @Transactional
    public void status(Job job) {
        if (!supports(job)) return;
        if (job.getType()==JobType.TARGETED_CONFIG_CHANGE) {
            String state=switch(job.getStatus()) {
                case FAILED, PARTIAL_SUCCESS, ROLLBACK_FAILED -> "FAILED";
                case ROLLED_BACK -> "SUPERSEDED";
                default -> null;
            };
            if(state!=null) jdbc.update("UPDATE kf_config_versions SET status=? WHERE job_id=? AND validation_result->>'targetedChangeHistory'='true' AND status IN ('VALIDATED','APPLYING')",state,job.getId().toString());
        }
        jdbc.update("UPDATE kf_config_change SET status=?, execution_started_at=COALESCE(execution_started_at,?), completed_at=?, updated_at=? WHERE change_id=?",
                job.getStatus().name(),time(job.getStartTime()),time(job.getEndTime()),now(),job.getId());
        synchronizeOperation(job);
        jdbc.update("UPDATE kf_rolling_restart_operation SET restart_status=?, started_at=COALESCE(started_at,?), completed_at=?, updated_at=? WHERE change_id=?",
                job.getStatus().name(),time(job.getStartTime()),time(job.getEndTime()),now(),job.getId());
    }

    private void synchronizeOperation(Job job) {
        jdbc.queryForObject("SELECT sync_cluster_operation_lock(?,?,?,?)",UUID.class,job.getId().toString(),
                job.getStatus().name(),Objects.requireNonNullElse(job.getRetryCount(),0),
                job.getEndTime()==null?now():time(job.getEndTime()));
    }

    @Transactional
    public void step(JobStep step) {
        Job job = step.getJob();
        if (!supports(job) || !exists(job.getId())) return;
        var payload = normalize(job,step);
        var selected = targets(job,step,payload);
        if (selected.isEmpty()) attempt(step,null,null,payload);
        for (var target : selected) {
            UUID targetId = targetId(job,target);
            jdbc.update("UPDATE kf_config_change_target_node SET target_status=?, started_at=COALESCE(started_at,?), completed_at=?, updated_at=? WHERE target_id=?",
                    step.getStatus().name(),time(step.getStartTime()),time(step.getEndTime()),now(),targetId);
            var changed = changes(target);
            if (changed.isEmpty()) attempt(step,targetId,null,target);
            for (String key : changed.keySet()) attempt(step,targetId,key,target);
            jdbc.update("UPDATE kf_rolling_restart_node_step SET attempt_count=(SELECT count(DISTINCT (phase,attempt_number,idempotency_key))::integer FROM kf_config_execution_attempt WHERE target_id=? AND started_at IS NOT NULL) WHERE restart_step_id=?",targetId,targetId);
            if (isStatic(target) && (isStatic(payload) && !job.getType().equals(JobType.ROLLING_CONFIG_UPDATE) || step.getName().startsWith("RESTART_SERVICE"))) {
                jdbc.update("UPDATE kf_rolling_restart_node_step SET step_status=?, completed_at=?, last_error=?, updated_at=? WHERE restart_step_id=?",
                        step.getStatus().name(),time(step.getEndTime()),failed(step)?safeLog(step,target):null,now(),targetId);
                jdbc.update("UPDATE kf_rolling_restart_operation SET current_target_id=?, completed_count=(SELECT count(*) FROM kf_rolling_restart_node_step WHERE change_id=? AND step_status IN ('SUCCESS','ROLLED_BACK','SKIPPED')), updated_at=? WHERE change_id=?",
                        step.getStatus()==JobStepStatus.IN_PROGRESS?targetId:null,job.getId(),now(),job.getId());
            }
            captureCheckpoint(step,target,targetId);
            syncTargetedVersion(step,target);
            if (selected.size()==1 || !payload.containsKey("previousByBroker")) capturePaths(step,target,targetId,step.getLogs());
            if (step.getStatus()==JobStepStatus.SUCCESS && step.getLogs()!=null && step.getLogs().contains("Active config values verified")) {
                jdbc.update("UPDATE kf_config_change_node_value SET value_after=CASE WHEN is_sensitive THEN NULL ELSE desired_value END, source_after='FILE', verification_status='SUCCESS', verified_at=?, updated_at=? WHERE target_id=? AND verification_status IS NULL",now(),now(),targetId);
            }
        }
    }

    /** Partial property history, never a standalone file-apply/restore document. */
    private void recordTargetedVersion(Job job, JobStep step, Map<String,Object> payload) {
        if(job.getType()!=JobType.TARGETED_CONFIG_CHANGE) return;
        UUID version=id(targetId(job,payload),"config-version");
        String host=text(payload.get("hostId")), role=text(payload.get("role")), path=text(payload.get("configPath"));
        // V65 mirrors external inventory into kf_clusters using the same UUID.
        // Reuse that real row for the existing FK, but never manufacture a mirror
        // or fail the accepted config job when inventory is incomplete.
        if(host!=null && host.startsWith("external:") && !Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM kf_clusters WHERE id=?)",Boolean.class,cluster(job)))) return;
        if(host==null || role==null || path==null || job.getRequestedBy()==null)
            throw new IllegalStateException("Targeted config version requires the accepted plan's host, role, config path and requester");
        jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtext(?))",Object.class,"targeted-config-version:"+cluster(job)+":"+host+":"+role+":"+path);
        if(Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM kf_config_versions WHERE id=?)",Boolean.class,version))) return;
        Integer next=jdbc.queryForObject("SELECT COALESCE(MAX(config_version),0)+1 FROM kf_config_versions WHERE cluster_id=? AND host_id=? AND component=? AND config_file_name=?",Integer.class,cluster(job),host,role,path);
        var before=new LinkedHashMap<String,Object>();var desired=new LinkedHashMap<String,Object>();
        var sources=new LinkedHashMap<String,Object>();var redacted=new ArrayList<String>();
        changes(payload).forEach((key,value)->{
            boolean secret=sensitive(key,payload);var previous=previous(payload,key);
            before.put(key,secret?null:previous.get("value"));desired.put(key,secret?null:value);
            sources.put(key,previous.get("source"));if(secret)redacted.add(key);
        });
        // service_id deliberately remains NULL: these revisions must not become the
        // active full-file version for a service or enter its stale-file comparison.
        insert("kf_config_versions","id",fields("id",version,"cluster_id",cluster(job),"host_id",host,"component",role,
                "config_file_name",path,"config_version",next,"old_config",before,"new_config",desired,
                "status","VALIDATED","approval_required",false,"created_by",job.getRequestedBy(),"job_id",job.getId().toString(),
                "created_at",time(job.getCreatedAt()),"validation_result",fields("targetedChangeHistory",true,
                        "scope","CHANGED_PROPERTIES","executionType",isStatic(payload)?"STATIC":"DYNAMIC",
                        "nodeId",payload.getOrDefault("nodeId",payload.get("brokerId")),"stepId",step.getId(),
                        "sourcesBefore",sources,"redactedKeys",redacted)));
    }

    private void syncTargetedVersion(JobStep step,Map<String,Object> payload) {
        if(step.getJob().getType()!=JobType.TARGETED_CONFIG_CHANGE) return;
        UUID version=id(targetId(step.getJob(),payload),"config-version");
        String state=switch(step.getStatus()) {
            case SUCCESS -> "APPLIED";
            case FAILED, ROLLBACK_FAILED -> "FAILED";
            case ROLLED_BACK -> "SUPERSEDED";
            case IN_PROGRESS -> "APPLYING";
            default -> "VALIDATED";
        };
        jdbc.update("UPDATE kf_config_versions SET status=?, applied_at=CASE WHEN ?='APPLIED' THEN COALESCE(applied_at,?) ELSE applied_at END WHERE id=? AND validation_result->>'targetedChangeHistory'='true'",
                state,state,time(step.getEndTime()),version);
        // Runtime checkpoints are more precise than preview values, especially
        // for dynamic source/presence. Never overwrite the requested new values.
        jdbc.update("""
                UPDATE kf_config_versions SET old_config=v.before_values, new_config=v.desired_values,
                    validation_result=validation_result || jsonb_build_object('sourcesBefore',v.sources,'redactedKeys',v.redacted_keys)
                FROM (SELECT jsonb_object_agg(d.config_key,n.value_before) AS before_values,
                             jsonb_object_agg(d.config_key,n.desired_value) AS desired_values,
                             jsonb_object_agg(d.config_key,n.source_before) AS sources,
                             COALESCE(jsonb_agg(d.config_key) FILTER (WHERE n.is_sensitive),'[]'::jsonb) AS redacted_keys
                      FROM kf_config_change_node_value n JOIN kf_config_change_detail d ON d.detail_id=n.detail_id
                      WHERE n.target_id=?) v
                WHERE id=? AND v.before_values IS NOT NULL AND validation_result->>'targetedChangeHistory'='true'
                """,targetId(step.getJob(),payload),version);
    }

    @Transactional
    public void verified(JobStep step, String key, Object value, String source) {
        if (!supports(step.getJob()) || !exists(step.getJob().getId())) return;
        var payload = normalize(step.getJob(),step);
        verifiedTarget(step,payload,key,value,source);
    }

    @Transactional
    public void verifiedTarget(JobStep step, Map<String,Object> payload, String key, Object value, String source) {
        if (!supports(step.getJob()) || !exists(step.getJob().getId())) return;
        UUID target = targetId(step.getJob(),payload);
        jdbc.update("UPDATE kf_config_change_node_value SET value_after=CASE WHEN is_sensitive THEN NULL ELSE ? END, source_after=?, verification_status='SUCCESS', verified_at=?, updated_at=? WHERE target_id=? AND detail_id=?",
                text(value),source,now(),now(),target,id(step.getJob().getId(),"detail:"+key));
    }

    @Transactional
    public void health(Job job, JobStep step, String phase, Map<String,Object> metrics, boolean healthy, String message) {
        if (!supports(job) || !exists(job.getId())) return;
        var selected = step==null?List.<Map<String,Object>>of():targets(job,step,normalize(job,step));
        if (selected.size()==1 && metrics.get("controllerId") instanceof Number controller && controller.intValue()>=0
                && !Boolean.TRUE.equals(selected.get(0).get("configMutationStarted"))) {
            Integer node=number(selected.get(0).getOrDefault("nodeId",selected.get(0).get("brokerId")));
            if (node!=null) {
                UUID target=targetId(job,selected.get(0));
                jdbc.update("UPDATE kf_config_change_target_node SET was_active_controller=? WHERE target_id=? AND was_active_controller IS NULL",node==controller.intValue(),target);
                jdbc.update("UPDATE kf_rolling_restart_node_step SET was_active_controller=? WHERE target_id=? AND was_active_controller IS NULL",node==controller.intValue(),target);
            }
        }
        insert("kf_health_check","health_check_id",fields("health_check_id",UUID.randomUUID(),"change_id",job.getId(),
                "target_id",selected.size()==1?targetId(job,selected.get(0)):null,"phase",phase,"check_type","CLUSTER_HEALTH",
                "result",healthy?"PASS":"FAIL","observed_at",now(),"metrics",metrics,"message",message));
    }

    @Transactional
    public void backup(JobStep step,Map<String,Object> metadata,Map<String,Object> previous,String output) {
        if (!supports(step.getJob()) || !exists(step.getJob().getId())) return;
        var payload=new LinkedHashMap<>(normalize(step.getJob(),step));payload.putAll(metadata);
        if (!previous.isEmpty()) payload.put("recoveryCheckpoint",Map.of("dynamic",previous));
        UUID target=targetId(step.getJob(),payload);
        captureCheckpoint(step,payload,target);capturePaths(step,payload,target,output);
    }

    private UUID target(Job job, JobStep step, Map<String,Object> payload) {
        UUID target = targetId(job,payload);
        Integer number = number(payload.getOrDefault("nodeId",payload.get("brokerId")));
        String host = text(payload.get("hostId"));
        List<Map<String,Object>> nodes = number != null
                ? jdbc.queryForList("SELECT id,role FROM kf_nodes WHERE cluster_id=? AND node_id=?",cluster(job),number)
                : host != null ? jdbc.queryForList("SELECT id,role FROM kf_nodes WHERE cluster_id=? AND host_id=? AND role=?",cluster(job),host,text(payload.get("role"))) : List.of();
        Map<String,Object> node = nodes.size()==1?nodes.get(0):Map.of();
        String role = Objects.toString(node.get("role"),Objects.toString(payload.get("role"),"")).toLowerCase(Locale.ROOT);
        insert("kf_config_change_target_node","target_id",fields("target_id",target,"change_id",job.getId(),"node_id",node.get("id"),
                "frozen_broker_role",role.isBlank()?null:role.contains("broker"),"frozen_controller_role",role.isBlank()?null:role.contains("controller"),
                "planned_order",step.getStepOrder(),"target_status",step.getStatus().name(),"created_at",time(step.getCreatedAt()),"updated_at",time(step.getUpdatedAt())));
        return target;
    }

    private void attempt(JobStep step, UUID target, String key, Map<String,Object> payload) {
        UUID detail=key==null?null:id(step.getJob().getId(),"detail:"+key);
        String phase = step.getJob().getStatus()==JobStatus.ROLLING_BACK || step.getStatus()==JobStepStatus.ROLLED_BACK || step.getStatus()==JobStepStatus.ROLLBACK_FAILED?"ROLLBACK":"APPLY";
        int number = Math.max(Objects.requireNonNullElse(step.getRetryCount(),0),Objects.requireNonNullElse(step.getJob().getRetryCount(),0))+1;
        UUID attempt = id(step.getId(),phase+":"+number+":"+target+":"+detail);
        var safeChanges=new LinkedHashMap<String,Object>();changes(payload).forEach((k,v)->{if(key==null||key.equals(k))safeChanges.put(k,sensitive(k,payload)?null:v);});
        if (phase.equals("ROLLBACK")) {
            var checkpoint=object(payload.get("recoveryCheckpoint"));
            var file=object(checkpoint.get("file"));var dynamic=object(checkpoint.get("dynamic"));
            if (!file.isEmpty()) {safeChanges.clear();file.forEach((k,v)->{if(key==null||key.equals(k))safeChanges.put(k,sensitive(k,payload)?null:v);});}
            else if (!dynamic.isEmpty()) {safeChanges.clear();dynamic.forEach((k,v)->{if(key==null||key.equals(k)){var before=object(v);safeChanges.put(k,sensitive(k,payload)?null:"DYNAMIC_BROKER_CONFIG".equals(before.get("source"))?before.get("value"):null);}});}
        }
        Map<String,Object> requested = fields("stepId",step.getId(),"hostId",payload.get("hostId"),"nodeId",payload.getOrDefault("nodeId",payload.get("brokerId")),"operation",payload.get("operation"),"changes",safeChanges);
        insert("kf_config_execution_attempt","attempt_id",fields("attempt_id",attempt,"change_id",step.getJob().getId(),
                "target_id",target,"detail_id",detail,"phase",phase,"attempt_number",number,"idempotency_key",step.getId(),
                "attempt_status",step.getStatus().name(),"requested_payload",requested,"started_at",time(step.getStartTime()),"created_at",now(),"updated_at",now()));
        jdbc.update("UPDATE kf_config_execution_attempt SET attempt_status=?, started_at=COALESCE(started_at,?), completed_at=?, error_message=?, response_payload=?::jsonb, updated_at=? WHERE attempt_id=?",
                step.getStatus().name(),time(step.getStartTime()),time(step.getEndTime()),failed(step)?safeLog(step,payload):null,
                json(fields("status",step.getStatus().name(),"agentTaskId",step.getAgentTaskId())),now(),attempt);
    }

    private void captureCheckpoint(JobStep step, Map<String,Object> payload, UUID target) {
        Map<String,Object> checkpoint = object(payload.get("recoveryCheckpoint"));
        for (String kind : List.of("file","dynamic")) {
            Map<String,Object> entries = object(checkpoint.get(kind));
            for (var entry : entries.entrySet()) {
                String key = entry.getKey(); boolean secret = sensitive(key,payload) || Boolean.TRUE.equals(object(entry.getValue()).get("isSensitive"));
                var previous = observed(entry.getValue(),kind.equals("file"));
                UUID detail = id(step.getJob().getId(),"detail:"+key);
                if (secret) {
                    jdbc.update("UPDATE kf_config_change_detail SET is_sensitive=TRUE, desired_value=NULL WHERE detail_id=?",detail);
                    jdbc.update("UPDATE kf_config_change_node_value SET is_sensitive=TRUE,value_before=NULL,value_after=NULL,desired_value=NULL WHERE detail_id=?",detail);
                    jdbc.update("UPDATE kf_config_backup SET is_sensitive=TRUE,value_before=NULL WHERE detail_id=?",detail);
                    if(step.getJob().getType()==JobType.TARGETED_CONFIG_CHANGE) jdbc.update("""
                            UPDATE kf_config_versions SET old_config=jsonb_set(old_config,ARRAY[?]::text[],'null'::jsonb),
                                new_config=jsonb_set(new_config,ARRAY[?]::text[],'null'::jsonb)
                            WHERE job_id=? AND validation_result->>'targetedChangeHistory'='true'
                            AND (jsonb_exists(old_config,?) OR jsonb_exists(new_config,?))
                            """,key,key,step.getJob().getId().toString(),key,key);
                }
                String rollback=previous.get("present") instanceof Boolean present?(present?"SET":"REMOVE"):null;
                jdbc.update("UPDATE kf_config_change_node_value SET is_sensitive=?, value_before=?, source_before=?, before_present=?, rollback_operation=?, desired_value=CASE WHEN ? THEN NULL ELSE desired_value END, updated_at=? WHERE target_id=? AND detail_id=?",
                        secret,secret?null:previous.get("value"),previous.get("source"),previous.get("present"),rollback,secret,now(),target,detail);
                insert("kf_config_backup","backup_id",fields("backup_id",id(target,"checkpoint:"+kind+":"+key),"change_id",step.getJob().getId(),
                        "target_id",target,"detail_id",detail,"backup_type",kind.equals("file")?"FILE_CHECKPOINT":"DYNAMIC_SNAPSHOT",
                        "config_key",key,"source_before",previous.get("source"),"before_present",previous.get("present"),
                        "value_before",secret?null:previous.get("value"),"is_sensitive",secret,"source_path",payload.get("configPath"),"captured_at",now()));
            }
        }
    }

    private void capturePaths(JobStep step, Map<String,Object> payload, UUID target, String output) {
        if (output==null) return;
        Set<String> reportedPaths = new HashSet<>();
        var matcher = BACKUP.matcher(output);
        while (matcher.find()) {
            String path = matcher.group(1).trim();
            reportedPaths.add(path);
            boolean dynamic=matcher.group().contains("Live dynamic property checkpoint") || matcher.group().contains("Dynamic checkpoint");
            // Chunk membership/full file contents are not reported in these text lines: link the artifact to its node only.
            insert("kf_config_backup","backup_id",fields("backup_id",id(target,"path:"+path),"change_id",step.getJob().getId(),
                    "target_id",target,"backup_type",dynamic?"DYNAMIC_SNAPSHOT":"FILE_BACKUP",
                    "source_path",dynamic?null:payload.get("configPath"),"backup_uri",path));
        }
        var receipts = Pattern.compile("(?m)^TANTOR_CONFIG_BACKUP_VERIFIED (\\{[^\\r\\n]*\\})$").matcher(output);
        while (receipts.find()) {
            Map<String,Object> receipt;
            OffsetDateTime verified;
            try {
                receipt = mapper.readValue(receipts.group(1), new TypeReference<Map<String,Object>>() {});
                verified = OffsetDateTime.parse(Objects.toString(receipt.get("verifiedAt"), ""));
            } catch (Exception invalid) { continue; }
            String path = text(receipt.get("path")), checksum = text(receipt.get("checksumSha256"));
            if (!reportedPaths.contains(path) || checksum == null || !checksum.matches("[a-f0-9]{64}")
                    || !"SOURCE_COPY_MATCH".equals(receipt.get("method")) || verified.isAfter(now().plusMinutes(5))
                    || step.getJob().getCreatedAt() != null && verified.toInstant().isBefore(step.getJob().getCreatedAt().minusSeconds(300))) continue;
            jdbc.update("""
                    UPDATE kf_config_backup SET checksum_sha256=COALESCE(checksum_sha256,?),
                        verified_at=CASE WHEN verification_method IS NULL THEN ? ELSE verified_at END,
                        integrity_status='VERIFIED',last_checked_at=COALESCE(last_checked_at,?),
                        verification_method=COALESCE(verification_method,'SOURCE_COPY_MATCH')
                    WHERE change_id=? AND target_id=? AND backup_uri=?
                        AND (checksum_sha256 IS NULL OR checksum_sha256=?)
                    """,checksum,verified,verified,step.getJob().getId(),target,path,checksum);
        }
    }

    private Long catalog(String key, String role, String policy, boolean sensitive) {
        var rows = jdbc.queryForList("SELECT config_catalog_id FROM kf_config_catalog WHERE config_key=? AND applicable_role IS NOT DISTINCT FROM ? AND execution_policy IS NOT DISTINCT FROM ? ORDER BY config_catalog_id LIMIT 1",key,role,policy);
        if (!rows.isEmpty()) return ((Number)rows.get(0).get("config_catalog_id")).longValue();
        jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtext(?))",Object.class,"config-catalog-observations");
        rows=jdbc.queryForList("SELECT config_catalog_id FROM kf_config_catalog WHERE config_key=? AND applicable_role IS NOT DISTINCT FROM ? AND execution_policy IS NOT DISTINCT FROM ? ORDER BY config_catalog_id LIMIT 1",key,role,policy);
        if (!rows.isEmpty()) return ((Number)rows.get(0).get("config_catalog_id")).longValue();
        return jdbc.queryForObject("INSERT INTO kf_config_catalog (config_key,applicable_role,execution_policy,is_sensitive,created_at,updated_at) VALUES (?,?,?,?,?,?) RETURNING config_catalog_id",Long.class,key,role,policy,sensitive?Boolean.TRUE:null,now(),now());
    }

    private Map<String,Object> normalize(Job job, JobStep step) {
        Map<String,Object> result = read(step.getPayload());
        Map<String,Object> request = read(job.getPayload());
        if (job.getType()==JobType.ROLLING_CONFIG_UPDATE && request.get("changes") instanceof List<?> changes) {
            for (Object raw : changes) {
                Map<String,Object> change = object(raw);
                if (!Objects.equals(change.get("host"),step.getTargetId())) continue;
                result.put("hostId",change.get("host")); result.put("nodeId",change.get("nodeId")); result.put("role",request.get("role"));
                result.put("configPath",change.get("configFilePath")); result.put("operation","STATIC");
                var desired = new LinkedHashMap<String,Object>(); var before = new LinkedHashMap<String,Object>();
                if (change.get("properties") instanceof List<?> properties) for (Object value : properties) {
                    var property = object(value); String key = text(property.get("key"));
                    if (key!=null) {Object desiredValue=property.get("newValue");desired.put(key,"".equals(desiredValue)?null:desiredValue);before.put(key,property.get("oldValue"));}
                }
                result.put("changes",desired); result.put("expectedValues",before); result.put("legacyEmptyBeforeAmbiguous",true); break;
            }
        }
        if (!result.containsKey("changes") && result.get("propertiesTemplate") instanceof String desired
                && result.get("previousPropertiesTemplate") instanceof String before) {
            var newValues=properties(desired);var oldValues=properties(before);var changed=new LinkedHashMap<String,Object>();var expected=new LinkedHashMap<String,Object>();
            var keys=new LinkedHashSet<>(oldValues.keySet());keys.addAll(newValues.keySet());
            for (String key:keys) if (!Objects.equals(newValues.get(key),oldValues.get(key))) {changed.put(key,newValues.get(key));expected.put(key,oldValues.get(key));}
            result.put("changes",changed);result.put("expectedValues",expected);
        }
        return result;
    }
    private List<Map<String,Object>> targets(Job job,JobStep step,Map<String,Object> payload) {
        if (payload.isEmpty()) return List.of();
        if (payload.get("previousByBroker") instanceof Map<?,?> brokers && !brokers.isEmpty()) {
            var result = new ArrayList<Map<String,Object>>();
            brokers.forEach((broker,value) -> {var copy=new LinkedHashMap<>(payload);copy.put("brokerId",broker);copy.put("role","BROKER");copy.put("expectedOldValue",value);result.add(copy);});
            return result;
        }
        return changes(payload).isEmpty() ? List.of() : List.of(payload);
    }
    private Map<String,Object> changes(Map<String,Object> payload) {
        if (payload.get("changes") instanceof Map<?,?>) return object(payload.get("changes"));
        String key=text(payload.getOrDefault("key",payload.get("configKey")));
        if (key==null) return Map.of();
        return fields(key,payload.containsKey("value")?payload.get("value"):payload.get("configValue"));
    }
    private Map<String,Object> previous(Map<String,Object> payload,String key) {
        Map<String,Object> expected=object(payload.getOrDefault("expectedValues",payload.get("expected")));
        if (expected.containsKey(key)) {
            if (Boolean.TRUE.equals(payload.get("legacyEmptyBeforeAmbiguous")) && "".equals(expected.get(key))) return fields("value",null,"source","FILE","present",null);
            return observed(expected.get(key),isStatic(payload));
        }
        if (payload.containsKey("expectedOldValue")) return fields("value",payload.get("expectedOldValue"),"source",null,"present",null);
        return Map.of();
    }
    private Map<String,Object> observed(Object raw,boolean file) {
        if (raw instanceof Map<?,?> entry) return fields("value",entry.get("value"),"source",entry.get("source"),
                "present",entry.get("source")==null?null:"DYNAMIC_BROKER_CONFIG".equals(entry.get("source")));
        return fields("value",text(raw),"source",file?"FILE":null,"present",file?raw!=null:null);
    }
    private UUID targetId(Job job,Map<String,Object> payload) {
        Object node=payload.getOrDefault("nodeId",payload.get("brokerId"));
        return id(job.getId(),"target:"+(node!=null?node:Objects.toString(payload.get("hostId"),"unknown")+":"+payload.get("role")));
    }
    private boolean exists(UUID id) { return Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM kf_config_change WHERE change_id=?)",Boolean.class,id)); }
    private static boolean isStatic(Map<String,Object> payload) {return "STATIC".equals(payload.get("operation")) || "patch_service".equals(payload.get("operation")) || Boolean.TRUE.equals(payload.get("checkpointRecovery")) || Boolean.TRUE.equals(payload.get("restart"));}
    private static boolean sensitive(String key,Map<String,Object> payload) {return key.matches("(?i).*(password|secret|token|credential|jaas|private.key|sasl).*") || Boolean.TRUE.equals(payload.get("isSensitive")) || Boolean.TRUE.equals(object(object(object(payload.get("recoveryCheckpoint")).get("dynamic")).get(key)).get("isSensitive"));}
    private String safeLog(JobStep step,Map<String,Object> payload) {return changes(payload).keySet().stream().anyMatch(k->sensitive(k,payload))?null:step.getLogs();}
    private static boolean failed(JobStep step) {return step.getStatus()==JobStepStatus.FAILED || step.getStatus()==JobStepStatus.ROLLBACK_FAILED;}
    private UUID cluster(Job job) {var value=read(job.getPayload()).get("clusterId");return UUID.fromString(String.valueOf(value));}
    private static UUID id(UUID parent,String purpose) {return UUID.nameUUIDFromBytes((parent+":"+purpose).getBytes(StandardCharsets.UTF_8));}
    private static Integer number(Object value) {try{return value==null?null:Integer.valueOf(value.toString());}catch(NumberFormatException ignored){return null;}}
    private static String text(Object value) {return value==null?null:value.toString();}
    private static OffsetDateTime now() {return OffsetDateTime.now(ZoneOffset.UTC);}
    private static OffsetDateTime time(Instant value) {return value==null?null:value.atOffset(ZoneOffset.UTC);}
    private static Map<String,Object> fields(Object... pairs) {var result=new LinkedHashMap<String,Object>();for(int i=0;i<pairs.length;i+=2)result.put((String)pairs[i],pairs[i+1]);return result;}
    @SuppressWarnings("unchecked") private static Map<String,Object> object(Object value) {return value instanceof Map<?,?> map?(Map<String,Object>)map:Map.of();}
    private Map<String,Object> read(String value) {if(value==null||value.isBlank())return new LinkedHashMap<>();try{return mapper.readValue(value,new TypeReference<LinkedHashMap<String,Object>>(){});}catch(Exception e){throw new IllegalStateException("Invalid stored configuration mapping payload",e);}}
    private static Map<String,Object> properties(String text) {try {var p=new Properties();p.load(new java.io.StringReader(text));var result=new LinkedHashMap<String,Object>();p.stringPropertyNames().stream().sorted().forEach(k->result.put(k,p.getProperty(k)));return result;}catch(java.io.IOException e){throw new IllegalStateException(e);}}
    private String json(Object value) {try{return mapper.writeValueAsString(value);}catch(Exception e){throw new IllegalStateException("Cannot serialize configuration mapping",e);}}
    private int insert(String table,String primary,Map<String,Object> values) {
        var names=new ArrayList<>(values.keySet()); var placeholders=new ArrayList<String>();var parameters=new ArrayList<Object>();
        for(String name:names){Object value=values.get(name);placeholders.add(JSON.contains(name)?"?::jsonb":"?");parameters.add(JSON.contains(name)&&value!=null?json(value):value);}
        return jdbc.update("INSERT INTO "+table+" ("+String.join(",",names)+") VALUES ("+String.join(",",placeholders)+") ON CONFLICT ("+primary+") DO NOTHING",parameters.toArray());
    }
}
