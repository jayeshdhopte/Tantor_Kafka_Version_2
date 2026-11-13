package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.translab.tantor.server.config.KafkaExpansionProperties;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.repository.TaskRepository;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class KafkaExpansionPreviewServiceTest {
    final ObjectMapper mapper=new ObjectMapper().findAndRegisterModules();final KafkaExpansionPlanCollector collector=mock(KafkaExpansionPlanCollector.class);final TaskRepository tasks=mock(TaskRepository.class);final KafkaExpansionProperties policy=new KafkaExpansionProperties();final KafkaExpansionPreviewService service=new KafkaExpansionPreviewService(collector,tasks,policy,mapper);
    KafkaExpansionPlan.Frozen frozen;Task task;
    KafkaExpansionPreviewService.Review start()throws Exception{
        frozen=new KafkaExpansionPlanTest().plan().freeze(mapper);when(collector.collect(any(),any(),any())).thenReturn(new KafkaExpansionPlanCollector.Collected(frozen,Instant.now().plusSeconds(120)));
        when(tasks.saveAndFlush(any())).thenAnswer(call->{task=call.getArgument(0);task.setId(UUID.randomUUID());when(tasks.findById(task.getId())).thenReturn(Optional.of(task));return task;});
        return service.start(frozen.plan().clusterId(),new KafkaExpansionPlanCollector.Request(frozen.plan().candidateHostId(),frozen.plan().layout(),UUID.randomUUID(),List.of(UUID.randomUUID()),"",false));
    }
    ObjectNode success()throws Exception{task.setStatus("SUCCESS");var proof=mapper.createObjectNode();proof.put("schemaVersion","add-quorum-capability/v1");proof.put("operationId",frozen.plan().operationId().toString());proof.put("hostId",task.getHostId());proof.put("planDigest",frozen.digest());proof.put("contextHash",frozen.plan().contextHash());proof.put("observedAt",Instant.now().toString());proof.put("candidateReady",true);proof.put("payloadVerified",true);proof.put("rootMutationSupported",true);proof.putArray("supportedCommands").add(KafkaExpansionLedger.CANDIDATE).add(KafkaExpansionLedger.PARTICIPANT).add(KafkaExpansionLedger.VALIDATE);task.setLogOutput(mapper.writeValueAsString(proof));return proof;}
    @Test void previewQueuesOnlyReadOnlyValidationAndReleaseGateStillBlocksSubmission()throws Exception{var review=start();assertEquals("RUNNING",review.status());assertEquals(KafkaExpansionLedger.VALIDATE,task.getCommand());assertEquals("VALIDATE",mapper.readTree(task.getParameters()).path("action").asText());assertFalse(review.executionEnabled());success();var ready=service.review(frozen.plan().clusterId(),task.getId());assertEquals("PASS",ready.status());assertFalse(ready.executionEnabled());assertThrows(org.springframework.web.server.ResponseStatusException.class,()->service.requireReady(frozen.plan().clusterId(),task.getId(),ready.contextHash()));}
    @Test void changedContextCannotReuseSuccessfulCandidateEvidence()throws Exception{start();success();ObjectNode tree=(ObjectNode)mapper.readTree(frozen.raw());tree.put("contextHash","f".repeat(64));var changed=mapper.treeToValue(tree,KafkaExpansionPlan.class).freeze(mapper);when(collector.collect(any(),any(),any())).thenReturn(new KafkaExpansionPlanCollector.Collected(changed,Instant.now().plusSeconds(120)));assertEquals("STALE",service.review(frozen.plan().clusterId(),task.getId()).status());}
    @Test void wrongDigestOrMissingNativeCapabilityCannotBecomePass()throws Exception{start();for(String field:List.of("planDigest","candidateReady","rootMutationSupported")){var proof=success();if(field.equals("planDigest"))proof.put(field,"e".repeat(64));else proof.put(field,false);task.setLogOutput(mapper.writeValueAsString(proof));assertThrows(org.springframework.web.server.ResponseStatusException.class,()->service.review(frozen.plan().clusterId(),task.getId()),field);}}
    @Test void expiredPreviewDoesNotRecollectOrEnableExecution()throws Exception{start();success();var params=(ObjectNode)mapper.readTree(task.getParameters());params.put("preview_expires_at",Instant.now().minusSeconds(1).toString());task.setParameters(mapper.writeValueAsString(params));clearInvocations(collector);var result=service.review(frozen.plan().clusterId(),task.getId());assertEquals("EXPIRED",result.status());assertFalse(result.executionEnabled());verifyNoInteractions(collector);}
    @Test void failedCandidateShowsBoundedNativeReasonAndCannotEnableExecution()throws Exception{start();task.setStatus("FAILED");task.setErrorMsg("shared filesystem cannot cover all process reservations: required 40960 MiB\n"+"x".repeat(1000));var review=service.review(frozen.plan().clusterId(),task.getId());assertEquals("BLOCKED",review.status());assertFalse(review.executionEnabled());assertTrue(review.message().contains("required 40960 MiB"));assertFalse(review.message().contains("\n"));assertTrue(review.message().length()<600);}
}
