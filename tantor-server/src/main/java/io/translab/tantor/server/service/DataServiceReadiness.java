package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;

/** Strict native REST readiness, distinct from prerequisites and connection reachability. No persistence during polling. */
@Service
public class DataServiceReadiness {
    private final ObjectMapper mapper;
    private final HttpClient client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).followRedirects(HttpClient.Redirect.NEVER).build();
    @Value("${tantor.data-services.readiness-timeout-seconds:300}") private long timeoutSeconds=300;
    @Value("${tantor.data-services.readiness-poll-millis:2000}") private long pollMillis=2000;
    public DataServiceReadiness(ObjectMapper mapper){this.mapper=mapper;}
    public void await(String role,String host,int port,String expectedKafkaClusterId) {
        String base;
        try {base=new URI("http",null,host,port,null,null,null).toString();}
        catch(java.net.URISyntaxException invalid){throw new IllegalArgumentException("Invalid service endpoint",invalid);}
        await(role,base,expectedKafkaClusterId,Duration.ofSeconds(Math.max(1,timeoutSeconds)),Duration.ofMillis(Math.max(1,pollMillis)));
    }
    void await(String role,String base,String cluster,Duration timeout,Duration poll) {
        long deadline=System.nanoTime()+timeout.toNanos();int stable=0;String reason="REST endpoint has not become ready";
        do {
            try {inspect(role,base,cluster,deadline);if(++stable>=2)return;}
            catch(RejectedEndpoint rejected){throw rejected;}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IllegalStateException("Service readiness interrupted",interrupted);}
            catch(Exception notReady){stable=0;reason=Objects.toString(notReady.getMessage(),notReady.getClass().getSimpleName());}
            long remaining=deadline-System.nanoTime();if(remaining<=0)break;
            try {java.util.concurrent.TimeUnit.NANOSECONDS.sleep(Math.min(remaining,poll.toNanos()));}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IllegalStateException("Service readiness interrupted",interrupted);}
        } while(System.nanoTime()<deadline);
        throw new IllegalStateException("Timed out waiting for "+role+" REST readiness at "+base+": "+reason);
    }
    private void inspect(String role,String base,String cluster,long deadline) throws Exception {
        if("schema_registry".equals(role)){if(!get(base+"/subjects",deadline).isArray())throw new IllegalStateException("Schema subjects response must be an array");return;}
        if(!"kafka_connect".equals(role))throw new IllegalArgumentException("Unsupported data service role");
        var root=get(base+"/",deadline);
        if(!root.isObject() || root.path("version").asText().isBlank())throw new IllegalStateException("Kafka Connect root lacks worker version");
        if(cluster!=null && !cluster.isBlank() && root.hasNonNull("kafka_cluster_id") && !cluster.equals(root.get("kafka_cluster_id").asText()))throw new RejectedEndpoint("Kafka Connect belongs to another Kafka cluster");
        if(!get(base+"/connectors",deadline).isArray())throw new IllegalStateException("Connector list is not ready");
        var plugins=get(base+"/connector-plugins",deadline);if(!plugins.isArray())throw new IllegalStateException("Connector plugins are not ready");
        Set<String> classes=new HashSet<>();plugins.forEach(plugin->classes.add(plugin.path("class").asText()));
        for(String name:List.of("org.apache.kafka.connect.file.FileStreamSourceConnector","org.apache.kafka.connect.file.FileStreamSinkConnector"))
            if(!classes.contains(name))throw new IllegalStateException("Required bundled plugin is not ready: "+name);
    }
    private static class RejectedEndpoint extends IllegalStateException {RejectedEndpoint(String message){super(message);}}
    private JsonNode get(String url,long deadline) throws Exception {
        long remaining=deadline-System.nanoTime();if(remaining<=0)throw new IllegalStateException("Readiness budget exhausted");
        var request=HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofNanos(Math.min(remaining,Duration.ofSeconds(5).toNanos()))).GET().build();
        var future=client.sendAsync(request,HttpResponse.BodyHandlers.ofByteArray());
        HttpResponse<byte[]> response;
        try {response=future.get(Math.min(remaining,Duration.ofSeconds(5).toNanos()),java.util.concurrent.TimeUnit.NANOSECONDS);}
        finally {if(!future.isDone())future.cancel(true);}
        if(response.statusCode()!=200)throw new IllegalStateException("REST endpoint returned HTTP "+response.statusCode());
        byte[] bytes=response.body();if(bytes.length>1024*1024)throw new IllegalStateException("REST response exceeds readiness limit");
        return mapper.readTree(bytes);
    }
}
