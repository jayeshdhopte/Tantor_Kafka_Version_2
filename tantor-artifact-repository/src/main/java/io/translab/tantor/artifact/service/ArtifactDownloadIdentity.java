package io.translab.tantor.artifact.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.util.*;

/** Resolve an opaque capability to server-persisted task metadata, never to a client-supplied username. */
@Service
public class ArtifactDownloadIdentity {
    private final JdbcTemplate jdbc;
    public ArtifactDownloadIdentity(JdbcTemplate jdbc){this.jdbc=jdbc;}
    public record Identity(String actor,String executor,UUID taskId,String hostname,String hostIp) {}
    public Identity resolve(UUID artifact,String token,String authenticatedActor,String remote) {
        if(token==null || token.isBlank()) {
            String actor=authenticatedActor==null || authenticatedActor.isBlank()?"unknown":authenticatedActor;
            return new Identity(actor,"repository-client",null,null,remote);
        }
        if(!token.matches("[0-9a-fA-F-]{36}"))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid download context");
        var rows=jdbc.queryForList("""
            SELECT id,host_id,parameters->>'initiated_by' AS actor,
                   parameters->>'target_hostname' AS hostname,parameters->>'target_host_ip' AS host_ip
            FROM kf_tasks WHERE command='DISTRIBUTE_PARCEL'
              AND parameters->>'artifact_id'=? AND parameters->>'download_context_token'=?
              AND created_at>now()-interval '24 hours'
            """,artifact.toString(),token);
        if(rows.size()!=1)throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Invalid or expired download context");
        var row=rows.getFirst();String actor=Objects.toString(row.get("actor"),"");
        if(actor.isBlank())throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Download initiator not recorded");
        return new Identity(actor,"agent:"+row.get("host_id"),(UUID)row.get("id"),
                (String)row.get("hostname"),(String)row.get("host_ip"));
    }
}
