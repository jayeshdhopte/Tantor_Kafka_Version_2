package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Job;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/** Durable, append-only observations. A storage failure stops the workflow before its next mutation. */
@Service
@RequiredArgsConstructor
public class RollingRestartEvidenceService {
    static final String VERSION="restart-checks-v1";
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    public record Context(UUID policyId, UUID jobId, int attempt) {}
    public record Check(String outcome, Map<String,Object> metrics, String message) {}

    @Transactional
    public Context begin(Job job, Map<String,Object> settings) {
        int attempt=Objects.requireNonNullElse(job.getRetryCount(),0)+1;
        UUID policy=UUID.randomUUID();
        int inserted=jdbc.update("""
            INSERT INTO kf_rolling_restart_policy(policy_id,job_id,attempt_number,policy_version,definition_version,settings)
            VALUES (?,?,?,'standalone-restart-v1',?,?::jsonb) ON CONFLICT(job_id,attempt_number) DO NOTHING
            """,policy,job.getId().toString(),attempt,VERSION,json(settings));
        if(inserted==0) {
            var row=jdbc.queryForMap("SELECT policy_id,settings,definition_version FROM kf_rolling_restart_policy WHERE job_id=? AND attempt_number=?",job.getId().toString(),attempt);
            if(!VERSION.equals(row.get("definition_version")) || !read(row.get("settings").toString()).equals(read(json(settings))))
                throw new IllegalStateException("Restart policy changed within an attempt; resume using a new retry attempt.");
            policy=(UUID)row.get("policy_id");
        }
        return new Context(policy,job.getId(),attempt);
    }

    @Transactional
    public void sample(Context context, UUID step, String phase, Map<String,Check> checks) {
        UUID sample=UUID.randomUUID();
        OffsetDateTime observed=OffsetDateTime.now(ZoneOffset.UTC);
        for(var check:checks.entrySet()) jdbc.update("""
            INSERT INTO kf_rolling_restart_check_result(result_id,policy_id,definition_version,check_key,step_id,phase,sample_id,outcome,observed_at,metrics,message)
            VALUES (?,?,?,?,?,?,?,?,?,?::jsonb,?)
            """,UUID.randomUUID(),context.policyId(),VERSION,check.getKey(),step,phase,sample,check.getValue().outcome(),observed,json(check.getValue().metrics()),check.getValue().message());
    }
    @Transactional
    public void check(Context context, UUID step, String phase, String key, String outcome, Map<String,Object> metrics, String message) {
        sample(context,step,phase,Map.of(key,new Check(outcome,metrics,message)));
    }
    private String json(Object value) {try{return mapper.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("Invalid restart evidence",e);}}
    private Map<String,Object> read(String value) {try{return mapper.readValue(value,new TypeReference<>(){});}catch(Exception e){throw new IllegalArgumentException("Invalid saved restart policy",e);}}
}
