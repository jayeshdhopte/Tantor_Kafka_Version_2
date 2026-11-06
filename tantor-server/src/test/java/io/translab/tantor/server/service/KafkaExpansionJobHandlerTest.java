package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.KafkaExpansionProperties;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.time.Instant;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static io.translab.tantor.server.service.KafkaExpansionWorkflow.*;

class KafkaExpansionJobHandlerTest {
    final KafkaExpansionLedger ledger=mock(KafkaExpansionLedger.class);final JobService jobs=mock(JobService.class);final AgentTaskAwaiter awaiter=mock(AgentTaskAwaiter.class);final KafkaExpansionNativeEvidence evidence=mock(KafkaExpansionNativeEvidence.class);final KafkaExpansionLiveVerifier live=mock(KafkaExpansionLiveVerifier.class);final KafkaExpansionProperties policy=new KafkaExpansionProperties();final ObjectMapper mapper=new ObjectMapper().findAndRegisterModules();
    final KafkaExpansionJobHandler handler=new KafkaExpansionJobHandler(ledger,jobs,awaiter,evidence,live,policy,mapper);
    Job job(){var job=new Job();job.setId(UUID.randomUUID());return job;}
    JobStep step(UUID jobId){var step=new JobStep();step.setId(UUID.randomUUID());step.setStatus(JobStepStatus.PENDING);return step;}
    @Test void committedPublicationRepairsJobWithoutRepeatingAnyNativeAction(){
        var job=job();var op=new KafkaExpansionOperation();op.setState(KafkaExpansionState.SUCCEEDED);var completed=step(job.getId());completed.setStatus(JobStepStatus.SUCCESS);var unfinished=step(job.getId());
        when(ledger.forJob(job.getId())).thenReturn(op);when(jobs.getSteps(job.getId())).thenReturn(List.of(completed,unfinished));
        handler.execute(job);verify(jobs).completeStep(eq(unfinished.getId()),contains("committed publication"));verify(jobs,never()).completeStep(eq(completed.getId()),anyString());verifyNoInteractions(awaiter,evidence,live);verify(ledger,never()).frozen(any());
    }
    @Test void failedFinalBaselineRetainsOwnershipAndCannotPrepareOrFormat()throws Exception{
        var job=job();var frozen=new KafkaExpansionPlanTest().plan().freeze(mapper);var op=new KafkaExpansionOperation();op.setId(frozen.plan().operationId());op.setJobId(job.getId());op.setState(KafkaExpansionState.ACCEPTED);
        var steps=frozen.steps().stream().map(ignored->step(job.getId())).toList();
        when(ledger.forJob(job.getId())).thenReturn(op);when(ledger.frozen(op)).thenReturn(frozen);when(ledger.checkpoints(op)).thenReturn(new Checkpoints(frozen.digest(),frozen.steps(),Map.of()));when(jobs.getSteps(job.getId())).thenReturn(steps);
        doAnswer(call->{steps.getFirst().setStatus(JobStepStatus.IN_PROGRESS);return null;}).when(jobs).startStep(steps.getFirst().getId());
        doThrow(new IllegalStateException("quorum changed")).when(live).baseline(frozen.plan());
        assertThrows(IllegalStateException.class,()->handler.execute(job));verify(ledger).recovery(op.getId(),"quorum changed");verify(jobs).failStep(steps.getFirst().getId(),"quorum changed");verify(ledger,never()).dispatch(any(),any(),anyBoolean(),any());verify(ledger,never()).publish(any(),any());verifyNoInteractions(awaiter,evidence);
    }
    @Test void mismatchedFrozenWorkflowCannotExecuteAgentTasks()throws Exception{
        var job=job();var op=new KafkaExpansionOperation();when(ledger.forJob(job.getId())).thenReturn(op);when(ledger.frozen(op)).thenReturn(new KafkaExpansionPlanTest().plan().freeze(mapper));when(jobs.getSteps(job.getId())).thenReturn(List.of());
        assertThrows(IllegalStateException.class,()->handler.execute(job));verifyNoInteractions(awaiter,evidence,live);verify(ledger,never()).dispatch(any(),any(),anyBoolean(),any());
    }
    @ParameterizedTest @CsvSource({"STATIC,CONTROLLER","STATIC,SEPARATE","DYNAMIC,CONTROLLER","DYNAMIC,SEPARATE"})
    void orderedExecutionPublishesOnlyAfterAllStagesAndIndependentHealth(Mode mode,Layout layout)throws Exception{
        var tree=(com.fasterxml.jackson.databind.node.ObjectNode)mapper.readTree(new KafkaExpansionPlanTest().fixture());tree.put("quorumMode",mode.name());tree.put("layout",layout.name());tree.put("maintenanceAcknowledged",mode==Mode.STATIC);((com.fasterxml.jackson.databind.node.ObjectNode)tree.path("features")).put("kraft.version",mode==Mode.STATIC?0:1);if(layout==Layout.CONTROLLER)((com.fasterxml.jackson.databind.node.ArrayNode)tree.path("processes")).remove(1);
        var frozen=mapper.treeToValue(tree,KafkaExpansionPlan.class).freeze(mapper);var plan=frozen.plan();var job=job();var op=new KafkaExpansionOperation();op.setId(plan.operationId());op.setJobId(job.getId());op.setPlanDigest(frozen.digest());op.setState(KafkaExpansionState.EXECUTING);
        var checkpoints=new Checkpoints(frozen.digest(),frozen.steps(),Map.of());var steps=frozen.steps().stream().map(ignored->step(job.getId())).toList();var dispatched=new ArrayList<String>();var current=new Step[1];
        when(ledger.forJob(job.getId())).thenReturn(op);when(ledger.frozen(op)).thenReturn(frozen);when(ledger.checkpoints(op)).thenReturn(checkpoints);when(jobs.getSteps(job.getId())).thenReturn(steps);policy.setStableSamples(2);policy.setStabilityPollSeconds(1);
        doAnswer(call->{String id=call.getArgument(1);checkpoints.intent(id);current[0]=frozen.steps().stream().filter(s->s.id().equals(id)).findFirst().orElseThrow();return null;}).when(ledger).beginServerStep(eq(op.getId()),anyString());
        when(ledger.dispatch(eq(op.getId()),any(),eq(false),nullable(String.class))).thenAnswer(call->{UUID stepId=call.getArgument(1);int index=java.util.stream.IntStream.range(0,steps.size()).filter(i->steps.get(i).getId().equals(stepId)).findFirst().orElseThrow();current[0]=frozen.steps().get(index);if(current[0].kind().mutation())checkpoints.intent(current[0].id());dispatched.add(current[0].kind()+":"+current[0].target());var task=new Task();task.setId(UUID.randomUUID());return task;});
        when(ledger.inspect(eq(op.getId()),any(),anyString())).thenAnswer(call->{var task=new Task();task.setId(UUID.randomUUID());task.setParameters(call.getArgument(2));return task;});
        when(awaiter.await(any(),eq(180L))).thenAnswer(call->{var task=new Task();task.setId(call.getArgument(0));return task;});
        when(evidence.read(eq(frozen),anyString(),any(),any())).thenAnswer(call->{String target=call.getArgument(1);var receipt=mapper.createObjectNode();var state=mapper.createObjectNode();receipt.put("phase",current[0].kind()==Kind.PRE_MAINTENANCE?"STAGED":"STARTED");state.put("active",true);state.put("runtimeVerified",true);state.put("backupVerified",true);state.put("stageVerified",true);state.put("metadataDirectoryId","a".repeat(22));plan.participants().stream().filter(p->p.key().equals(target)).findFirst().ifPresent(p->{state.put("pid",p.pid());state.put("startTicks",p.startTicks());state.put("configSha256",p.configSha256());});return new KafkaExpansionNativeEvidence.Observation(receipt,state,Instant.now(),target);});
        when(evidence.expanded(any())).thenReturn(true);when(evidence.completed(eq(plan),any(),any(),anyBoolean())).thenAnswer(call->((Step)call.getArgument(1)).kind().requiredFacts());
        when(live.controller(eq(frozen),anyString(),anyBoolean(),anyBoolean(),anyBoolean())).thenAnswer(call->new KafkaExpansionLiveVerifier.ControllerProof(UUID.randomUUID(),op.getId(),frozen.digest(),plan.process("controller").nodeId(),"a".repeat(22),plan.voterDigest(),Instant.now(),true,call.getArgument(2)));
        doAnswer(call->{checkpoints.confirm(call.getArgument(1),Instant.now());return null;}).when(ledger).confirm(eq(op.getId()),any());
        doAnswer(call->{assertEquals(Kind.PUBLISH,checkpoints.next().orElseThrow().kind());verify(live,atLeast(2)).health(plan);return null;}).when(ledger).publish(eq(op.getId()),any());
        handler.execute(job);verify(ledger).publish(eq(op.getId()),any());verify(ledger,never()).recovery(any(),any());
        if(mode==Mode.DYNAMIC)assertTrue(dispatched.stream().noneMatch(s->s.startsWith("STOP:")||s.startsWith("APPLY_VOTERS:")||s.startsWith("STAGE_VOTERS:")));
        if(layout==Layout.SEPARATE)assertTrue(dispatched.indexOf("START:candidate:controller")<dispatched.indexOf("START:candidate:broker"));
    }
}
