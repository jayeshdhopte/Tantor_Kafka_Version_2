package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.dto.ExternalConnectionSettings;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class MessageMaskingServiceTest {
    private final MessageMaskingService service=new MessageMaskingService(mock(ExternalClusterRepository.class),new ObjectMapper());
    private ExternalConnectionSettings.Mask rule(String type){var rule=new ExternalConnectionSettings.Mask();rule.setType(type);rule.setFields(List.of("password"));rule.setTopicValuesPattern("orders.*");return rule;}
    @Test void masksNestedObjectsAndArraysWithoutMutatingSourceOrOtherFields(){
        String source="{\"password\":\"secret\",\"items\":[{\"password\":42,\"id\":1}]}";
        String actual=service.apply(List.of(rule("MASK")),"orders-v1",false,source);
        assertThat(actual).doesNotContain("secret","42").contains("***","\"id\":1");assertThat(source).contains("secret");
    }
    @Test void filtersByTopicAndKeyValueScope(){
        String source="{\"password\":\"secret\"}";
        assertThat(service.apply(List.of(rule("MASK")),"other",false,source)).isEqualTo(source);
        assertThat(service.apply(List.of(rule("MASK")),"orders",true,source)).isEqualTo(source);
    }
    @Test void removesAndReplacesFieldsAndFailsClosedForNonJson(){
        var remove=rule("REMOVE");assertThat(service.apply(List.of(remove),"orders",false,"{\"password\":\"x\",\"id\":1}")).isEqualTo("{\"id\":1}");
        var replace=rule("REPLACE");replace.setFieldsPattern(".*Secret");replace.setReplacement("hidden");
        assertThat(service.apply(List.of(replace),"orders",false,"{\"apiSecret\":\"raw\"}")).contains("hidden").doesNotContain("raw");
        assertThat(service.apply(List.of(rule("MASK")),"orders",false,"unstructured secret")).isEqualTo("***");
        assertThat(service.apply(List.of(remove),"orders",false,"unstructured secret")).isNull();
    }
}
