package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import io.translab.tantor.server.dto.ExternalConnectionSettings;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.regex.Pattern;

/** Redact responses on the server; Kafka records/offsets are never modified. */
@Service @RequiredArgsConstructor
public class MessageMaskingService {
    private final ExternalClusterRepository clusters;
    private final ObjectMapper mapper;

    public List<ExternalConnectionSettings.Mask> rules(UUID id) {
        return clusters.findById(id).map(cluster->{
            if(cluster.getConnectionSettings()==null || cluster.getConnectionSettings().isBlank()) return List.<ExternalConnectionSettings.Mask>of();
            try{return mapper.readValue(cluster.getConnectionSettings(),ExternalConnectionSettings.class).getMasking();}
            catch(Exception e){throw new IllegalStateException("Cannot safely read this cluster's masking settings.");}
        }).orElse(List.of());
    }
    public String apply(List<ExternalConnectionSettings.Mask> rules,String topic,boolean key,String text) {
        if(text==null || rules.isEmpty())return text;
        var applicable=rules.stream().filter(rule->matches(key?rule.getTopicKeysPattern():rule.getTopicValuesPattern(),topic)).toList();
        if(applicable.isEmpty())return text;
        JsonNode node;
        try{node=mapper.readTree(text);if(node==null || !node.isContainerNode())throw new IllegalArgumentException();}
        catch(Exception ignored){return "REMOVE".equals(applicable.get(0).getType())?null:replacement(applicable.get(0));}
        for(var rule:applicable) redact(node,rule);
        return node.toString();
    }
    private void redact(JsonNode node,ExternalConnectionSettings.Mask rule) {
        if(node.isObject()) {
            ObjectNode object=(ObjectNode)node;
            List<String> names=new ArrayList<>();object.fieldNames().forEachRemaining(names::add);
            for(String name:names) {
                if(rule.getFields().contains(name) || matches(rule.getFieldsPattern(),name)) {
                    if("REMOVE".equals(rule.getType())) object.remove(name);
                    else object.put(name,replacement(rule));
                } else redact(object.get(name),rule);
            }
        } else if(node.isArray())node.forEach(child->redact(child,rule));
    }
    private String replacement(ExternalConnectionSettings.Mask rule){return "REPLACE".equals(rule.getType())?Objects.toString(rule.getReplacement(),""):"***";}
    private boolean matches(String pattern,String text){return pattern!=null && !pattern.isBlank() && Pattern.compile(pattern).matcher(text).matches();}
}
