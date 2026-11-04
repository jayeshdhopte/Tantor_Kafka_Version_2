package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

class DataServiceReadinessTest {
    final DataServiceReadiness readiness=new DataServiceReadiness(new ObjectMapper());
    String base(HttpServer server){return "http://127.0.0.1:"+server.getAddress().getPort();}
    @Test void waitsForSchemaStartupRatherThanFailingTheFirstUnavailableResponse() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);var calls=new AtomicInteger();
        server.createContext("/subjects",exchange->{boolean ready=calls.incrementAndGet()>2;byte[] body="[]".getBytes();exchange.sendResponseHeaders(ready?200:503,body.length);try(var out=exchange.getResponseBody()){out.write(body);}});server.start();
        try{readiness.await("schema_registry",base(server),null,Duration.ofSeconds(2),Duration.ofMillis(10));assertThat(calls.get()).isGreaterThanOrEqualTo(4);}finally{server.stop(0);}
    }
    @Test void connectorPluginsAndClusterIdentityMustBeReadyBeforeSuccess() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);var plugins=new AtomicInteger();
        server.createContext("/",exchange->{String path=exchange.getRequestURI().getPath();String value=switch(path){
            case "/"->"{\"version\":\"4.3.0\",\"kafka_cluster_id\":\"expected\"}";
            case "/connectors"->"[]";
            default->plugins.incrementAndGet()<2?"[]":"[{\"class\":\"org.apache.kafka.connect.file.FileStreamSourceConnector\"},{\"class\":\"org.apache.kafka.connect.file.FileStreamSinkConnector\"}]";};
            byte[] body=value.getBytes();exchange.sendResponseHeaders(200,body.length);try(var out=exchange.getResponseBody()){out.write(body);}});server.start();
        try{
            readiness.await("kafka_connect",base(server),"expected",Duration.ofSeconds(2),Duration.ofMillis(10));assertThat(plugins.get()).isGreaterThanOrEqualTo(3);
            assertThatThrownBy(()->readiness.await("kafka_connect",base(server),"different",Duration.ofMillis(150),Duration.ofMillis(10))).hasMessageContaining("another Kafka cluster");
        }finally{server.stop(0);}
    }
    @Test void permanentlyRefusedEndpointFailsWithinItsBudgetAndNamesTheExactPort() throws Exception {
        var socket=new java.net.ServerSocket(0);int port=socket.getLocalPort();socket.close();long start=System.nanoTime();
        assertThatThrownBy(()->readiness.await("kafka_connect","http://127.0.0.1:"+port,null,Duration.ofMillis(150),Duration.ofMillis(10)))
                .hasMessageContaining("kafka_connect").hasMessageContaining(":"+port);
        assertThat(Duration.ofNanos(System.nanoTime()-start)).isLessThan(Duration.ofSeconds(2));
    }
    @Test void stalledResponseBodyCannotOutliveTheReadinessBudget() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/subjects",exchange->{exchange.sendResponseHeaders(200,100);try(var out=exchange.getResponseBody()){
            out.write('[');out.flush();try{Thread.sleep(500);}catch(InterruptedException e){Thread.currentThread().interrupt();}
        }catch(java.io.IOException ignored){}});server.start();long start=System.nanoTime();
        try {
            assertThatThrownBy(()->readiness.await("schema_registry",base(server),null,Duration.ofMillis(150),Duration.ofMillis(10))).hasMessageContaining("Timed out");
            assertThat(Duration.ofNanos(System.nanoTime()-start)).isLessThan(Duration.ofSeconds(1));
        }finally{server.stop(0);}
    }
    @Test void onlyPostStartReadinessTimeoutsQualifyForRecovery() {
        assertThat(ExternalServicesJobHandler.startupVerificationTimeout("schema-registry verification failed: connection refused (context deadline exceeded)")).isTrue();
        for(String failure:new String[]{"checksum mismatch","permission denied","schema-registry is already active","context deadline exceeded during download"})
            assertThat(ExternalServicesJobHandler.startupVerificationTimeout(failure)).isFalse();
    }
}
