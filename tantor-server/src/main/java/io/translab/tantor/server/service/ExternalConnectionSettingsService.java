package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.dto.*;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.audit.AuditService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Service @RequiredArgsConstructor
public class ExternalConnectionSettingsService {
    private final ExternalClusterRepository clusters;
    private final ClusterRepository mirrors;
    private final KafkaAdminService kafka;
    private final DataServiceConnectionService connections;
    private final ObjectMapper mapper;
    private final AuditService audit;

    public Map<String,Object> get(UUID id) {
        var cluster = require(id);
        return Map.of("bootstrapServers", cluster.getBootstrapServers(), "settings", read(cluster.getConnectionSettings()),
                "schemaRegistry", connections.listConnections(id,"SCHEMA_REGISTRY"),
                "kafkaConnect", connections.listConnections(id,"KAFKA_CONNECT"));
    }

    public ExternalConnectionSettings read(String json) {
        if(json==null || json.isBlank()) return new ExternalConnectionSettings();
        try { return mapper.readValue(json,ExternalConnectionSettings.class); }
        catch(Exception e) {throw new IllegalStateException("External connection settings could not be read.",e);}
    }

    public void validate(ExternalConnectionSettings.Request request) {
        if(request==null || request.getSettings()==null) throw new IllegalArgumentException("Connection settings are required.");
        if(request.getBootstrapServers()!=null) normalizeBootstrap(request.getBootstrapServers());
        var metrics=request.getSettings().getMetrics();
        if(metrics!=null && metrics.isEnabled()) {
            port(metrics.getBrokerPort());port(metrics.getControllerPort());
        }
        var rules=request.getSettings().getMasking();
        if(rules==null || rules.size()>50) throw new IllegalArgumentException("Provide up to 50 masking rules.");
        for(var rule:rules) {
            if(rule==null || rule.getType()==null || !Set.of("MASK","REMOVE","REPLACE").contains(rule.getType())) throw new IllegalArgumentException("Invalid masking type.");
            if(rule.getFields()!=null)rule.setFields(rule.getFields().stream().filter(Objects::nonNull).map(String::trim).filter(s->!s.isEmpty()).toList());
            if(rule.getFields()==null || rule.getFields().size()>100 || rule.getFields().stream().anyMatch(s->s==null || s.isBlank() || s.length()>256)) throw new IllegalArgumentException("Invalid masking field names.");
            if(rule.getFields().isEmpty() && (rule.getFieldsPattern()==null || rule.getFieldsPattern().isBlank())) throw new IllegalArgumentException("Each masking rule needs a field or field-name pattern.");
            regex(rule.getFieldsPattern());regex(rule.getTopicKeysPattern());regex(rule.getTopicValuesPattern());
            if((rule.getTopicKeysPattern()==null || rule.getTopicKeysPattern().isBlank()) && (rule.getTopicValuesPattern()==null || rule.getTopicValuesPattern().isBlank())) throw new IllegalArgumentException("Select topic keys or values for each masking rule.");
            if(rule.getReplacement()!=null && rule.getReplacement().length()>256) throw new IllegalArgumentException("Mask replacement is too long.");
        }
        validateConnections(request.getSchemaRegistry());validateConnections(request.getKafkaConnect());
    }

    private void validateConnections(List<SaveConnectionRequest> requests) {
        if(requests==null || requests.size()>30) throw new IllegalArgumentException("Provide up to 30 service connections.");
        Set<String> names=new HashSet<>();
        for(var connection:requests) {
            if(connection==null || connection.getConnectionName()==null || connection.getConnectionName().isBlank() || !names.add(connection.getConnectionName().trim())) throw new IllegalArgumentException("Service connection names must be present and unique.");
            if(!Set.of("HTTP","HTTPS").contains(Objects.toString(connection.getProtocol(),"").toUpperCase(Locale.ROOT))) throw new IllegalArgumentException("Service connections require HTTP or HTTPS.");
            port(connection.getPort());
            if(connection.getHost()==null || !connection.getHost().matches("[a-zA-Z0-9.\\-:\\[\\]]+")) throw new IllegalArgumentException("Invalid service hostname.");
        }
    }
    private void regex(String value) {
        if(value==null || value.isBlank())return;
        if(value.length()>256)throw new IllegalArgumentException("Mask patterns must be at most 256 characters.");
        try{Pattern.compile(value);}catch(Exception e){throw new IllegalArgumentException("Invalid masking pattern.");}
    }
    private void port(Integer value) {if(value==null || value<1 || value>65535)throw new IllegalArgumentException("Ports must be between 1 and 65535.");}

    public static String normalizeBootstrap(String input) {
        if(input==null || input.isBlank())throw new IllegalArgumentException("At least one bootstrap server is required.");
        Set<String> endpoints=new LinkedHashSet<>();
        for(String part:input.split(",",-1)) {
            String value=part.trim();
            try {
                URI endpoint=URI.create("kafka://"+value);
                if(endpoint.getHost()==null || endpoint.getUserInfo()!=null || endpoint.getPort()<1 || endpoint.getPort()>65535 || !Objects.toString(endpoint.getPath(),"").isEmpty() || endpoint.getQuery()!=null || endpoint.getFragment()!=null || value.matches(".*\\s.*"))throw new IllegalArgumentException();
                endpoints.add(value);
            }catch(Exception e){throw new IllegalArgumentException("Bootstrap servers must use host:port (IPv6: [address]:port).");}
        }
        if(endpoints.size()>30)throw new IllegalArgumentException("Provide up to 30 bootstrap servers.");
        return String.join(",",endpoints);
    }

    @Transactional
    public Map<String,Object> save(UUID id, ExternalConnectionSettings.Request request) {
        validate(request);
        ExternalCluster cluster=require(id);
        String before=cluster.getConnectionSettings();
        boolean changed=false;
        if(request.getBootstrapServers()!=null) {
            String normalized=normalizeBootstrap(request.getBootstrapServers());
            if(!normalized.equals(cluster.getBootstrapServers())) {
                String actual=probe(id,normalized);
                if(cluster.getKafkaClusterId()!=null && !cluster.getKafkaClusterId().isBlank() && !"Unknown".equalsIgnoreCase(cluster.getKafkaClusterId()) && !cluster.getKafkaClusterId().equals(actual))throw new IllegalArgumentException("These bootstrap servers belong to a different Kafka cluster.");
                cluster.setBootstrapServers(normalized);changed=true;
                mirrors.findById(id).ifPresent(mirror->{mirror.setBootstrapServers(normalized);mirrors.save(mirror);});
            }
        }
        try {cluster.setConnectionSettings(mapper.writeValueAsString(request.getSettings()));}
        catch(Exception e){throw new IllegalArgumentException("Cannot save connection settings.");}
        clusters.save(cluster);
        for(var connection:request.getSchemaRegistry()) connections.saveConnection(id,"SCHEMA_REGISTRY",connection,io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
        for(var connection:request.getKafkaConnect()) connections.saveConnection(id,"KAFKA_CONNECT",connection,io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
        if(changed) {
            if(org.springframework.transaction.support.TransactionSynchronizationManager.isSynchronizationActive())
                org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(new org.springframework.transaction.support.TransactionSynchronization(){@Override public void afterCommit(){kafka.refreshAdminClient(id);}});
            else kafka.refreshAdminClient(id);
        }
        audit.record("CLUSTER_MANAGEMENT","EXTERNAL_CONNECTION_SETTINGS_UPDATED","CLUSTER",id.toString(),id,"SUCCESS",before,cluster.getConnectionSettings(),null,Map.of("schemaRegistryConnections",request.getSchemaRegistry().size(),"kafkaConnectConnections",request.getKafkaConnect().size()));
        return get(id);
    }

    String probe(UUID id,String bootstrap) {
        var properties=kafka.getKafkaClientProperties(id);
        properties.put("bootstrap.servers",bootstrap);properties.put("request.timeout.ms","10000");properties.put("default.api.timeout.ms","15000");
        try(var client=AdminClient.create(properties)){return client.describeCluster().clusterId().get(15,TimeUnit.SECONDS);}
        catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException("Bootstrap verification interrupted.");}
        catch(Exception e){throw new IllegalArgumentException("Could not verify the new bootstrap servers using this cluster's saved security settings.");}
    }
    private ExternalCluster require(UUID id){return clusters.findById(id).filter(c->!Set.of("DELETED","DELETING").contains(c.getStatus())).orElseThrow(()->new IllegalArgumentException("External cluster not found or is being deleted."));}
}
