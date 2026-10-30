package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStep;
import org.hibernate.Hibernate;
import org.hibernate.bytecode.internal.bytebuddy.BytecodeProviderImpl;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.pojo.bytebuddy.ByteBuddyProxyFactory;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

class TargetedConfigChangeDetachedJobTest {
    @Test
    void healthCheckDoesNotInitializeTheDetachedParentJob() throws Exception {
        var factory = new ByteBuddyProxyFactory(new BytecodeProviderImpl().getByteBuddyProxyHelper());
        factory.postInstantiate(Job.class.getName(), Job.class, Set.of(HibernateProxy.class),
                Job.class.getMethod("getId"), Job.class.getMethod("setId", UUID.class), null);
        Job detachedJob = (Job) factory.getProxy(UUID.randomUUID(), null);
        JobStep step = new JobStep();
        step.setId(UUID.randomUUID());
        step.setJob(detachedJob);
        var jobs = mock(JobService.class);
        var health = mock(RollingRestartHealthService.class);
        var snapshot = new RollingRestartHealthService.HealthSnapshot(true, 1, 101, 0, 0, 0, 0, Map.of());
        UUID clusterId = UUID.randomUUID();
        when(health.inspect(clusterId, 1, Set.of("host-1"))).thenReturn(snapshot);
        var handler = new TargetedConfigChangeJobHandler(jobs, mock(KafkaAdminService.class), health,
                mock(DeploymentService.class), mock(AgentTaskAwaiter.class), new ObjectMapper());

        assertThat(Hibernate.isInitialized(detachedJob)).isFalse();
        assertThatCode(() -> ReflectionTestUtils.invokeMethod(handler, "checkHealth", step, clusterId, 1, "host-1"))
                .doesNotThrowAnyException();
        assertThat(Hibernate.isInitialized(detachedJob)).isFalse();
        verify(jobs).recordConfigExecutionHealth(step.getId(), snapshot);
    }
}
