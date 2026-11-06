package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;

/** References are declarations, not runtime proof. Every use still requires native identity checks. */
record KafkaProcessReference(String unit,String configPath) {
    static KafkaProcessReference resolve(Cluster cluster,ClusterServiceAssignment service,ObjectMapper mapper){
        try{
            var config=mapper.readTree(service.getConfigJson()==null||service.getConfigJson().isBlank()?"{}":service.getConfigJson());
            String unit=config.path("service_name").asText(config.path("systemd_service").asText(""));String path=config.path("config_path").asText("");
            // Older native installations persist their configuration directory on
            // the cluster and use the documented legacy role-based unit/file names.
            // Missing directory metadata has no guessed filesystem fallback.
            if(!"EXTERNAL".equalsIgnoreCase(cluster.getOriginType())){
                if(unit.isBlank())unit=RollingRestartPlan.unit(java.util.Set.of("broker,controller","controller,broker").contains(service.getRole())?"broker_controller":service.getRole());
                if(path.isBlank()&&cluster.getConfigDirectory()!=null&&!cluster.getConfigDirectory().isBlank()){
                    String file=switch(service.getRole()){case "broker"->"broker.properties";case "controller"->"controller.properties";case "broker_controller","broker,controller","controller,broker"->"server.properties";default->"";};
                    if(!file.isEmpty())path=cluster.getConfigDirectory().replaceFirst("/+$","")+"/"+file;
                }
            }
            if(!unit.isBlank()&&!unit.endsWith(".service"))unit+=".service";
            return new KafkaProcessReference(unit,path);
        }catch(Exception e){throw new IllegalArgumentException("Recorded Kafka process references are incomplete.",e);}
    }
}
