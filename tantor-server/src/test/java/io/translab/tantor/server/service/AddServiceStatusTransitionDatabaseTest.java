package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.ExternalDataService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Committing/concurrency tests: explicitly opt in using an isolated *_check_* database. */
@EnabledIfEnvironmentVariable(named = "TANTOR_STATUS_AUDIT_TEST_URL", matches = ".+")
class AddServiceStatusTransitionDatabaseTest {
    @Configuration @EnableTransactionManagement
    static class Config {}

    private final DriverManagerDataSource source = new DriverManagerDataSource(
            System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"), System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),
            System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD"));
    private final JdbcTemplate jdbc = new JdbcTemplate(source);
    private final DataSourceTransactionManager manager = new DataSourceTransactionManager(source);
    private final AuditService audit = mock(AuditService.class);

    private AnnotationConfigApplicationContext context() {
        assertThat(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL")).contains("_check_");
        var context = new AnnotationConfigApplicationContext();
        context.register(Config.class);
        context.registerBean("transactionManager", DataSourceTransactionManager.class, () -> manager);
        context.registerBean(AddServiceMappingService.class,
                () -> new AddServiceMappingService(jdbc, new ObjectMapper(), context));
        context.registerBean(AddServiceAuditListener.class, () -> new AddServiceAuditListener(jdbc, audit));
        context.refresh();
        return context;
    }

    private UUID create(AddServiceMappingService mapping, boolean external, String role, String status) {
        UUID id = UUID.randomUUID(), clusterId = UUID.randomUUID();
        if (external) {
            var service = new ExternalDataService(); service.setId(id); service.setClusterId(clusterId);
            service.setAgentId("audit-test-external"); service.setRole(role); service.setStatus(status); service.setConfigJson("{}");
            mapping.recordExternal(service, "creator");
        } else {
            var cluster = new Cluster(); cluster.setId(clusterId);
            var service = new ClusterServiceAssignment(); service.setId(id); service.setCluster(cluster);
            service.setHostId("audit-test-internal"); service.setRole(role); service.setStatus(status); service.setConfigJson("{}");
            mapping.recordManaged(service, "creator");
        }
        clearInvocations(audit);
        return id;
    }

    private List<org.mockito.invocation.Invocation> transitions(UUID id) {
        return mockingDetails(audit).getInvocations().stream()
                .filter(call -> call.getMethod().getName().equals("recordAs")
                        && call.getArgument(4).equals("SERVICE_STATUS_CHANGED")
                        && call.getArgument(6).equals(id.toString())).toList();
    }

    @Test void repeatedPhasesEmitOneEventForBothRolesAndBothDeploymentPaths() {
        try (var context = context()) {
            var mapping = context.getBean(AddServiceMappingService.class);
            for (boolean external : new boolean[]{false, true}) for (String role : List.of("schema_registry", "kafka_connect")) {
                UUID id = create(mapping, external, role, "PENDING");
                for (int i = 0; i < 5; i++) mapping.updateStatus(id, "DEPLOYING", "deployer-" + i);
                assertThat(transitions(id)).hasSize(1);
                assertThat(jdbc.queryForObject("SELECT updated_by FROM kf_add_service WHERE add_service_id=?", String.class, id))
                        .isEqualTo("deployer-0");
                for (int i = 0; i < 5; i++) mapping.updateStatus(id, "ONLINE", "online-user");
                assertThat(transitions(id).stream().map(call -> call.getArgument(8, String.class)))
                        .containsExactly("DEPLOYING", "ONLINE");
                // A real transition into an earlier phase is still a new event.
                mapping.updateStatus(id, "DEPLOYING", "retry-user");
                assertThat(transitions(id).stream().map(call -> call.getArgument(8, String.class)))
                        .containsExactly("DEPLOYING", "ONLINE", "DEPLOYING");
            }
        }
    }

    @Test void concurrentCallbacksForOneTransitionProduceOneCommittedEvent() throws Exception {
        try (var context = context(); var workers = Executors.newFixedThreadPool(5)) {
            var mapping = context.getBean(AddServiceMappingService.class);
            UUID id = create(mapping, true, "kafka_connect", "PENDING");
            var start = new CountDownLatch(1);
            var futures = java.util.stream.IntStream.range(0, 5).mapToObj(i -> workers.submit(() -> {
                if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Start timeout");
                mapping.updateStatus(id, "DEPLOYING", "parallel-user"); return true;
            })).toList();
            start.countDown();
            for (var future : futures) assertThat(future.get(20, TimeUnit.SECONDS)).isTrue();
            assertThat(transitions(id)).hasSize(1);
            assertThat(jdbc.queryForObject("SELECT status FROM kf_add_service WHERE add_service_id=?", String.class, id))
                    .isEqualTo("DEPLOYING");
        }
    }

    @Test void nullStatusesAreHandledAndRolledBackTransitionsNeverReachAudit() {
        try (var context = context()) {
            var mapping = context.getBean(AddServiceMappingService.class);
            UUID id = create(mapping, false, "schema_registry", null);
            mapping.updateStatus(id, null, "no-op");
            assertThat(transitions(id)).isEmpty();
            new TransactionTemplate(manager).executeWithoutResult(tx -> {
                mapping.updateStatus(id, "DEPLOYING", "rolled-back-user"); tx.setRollbackOnly();
            });
            assertThat(transitions(id)).isEmpty();
            assertThat(jdbc.queryForObject("SELECT status FROM kf_add_service WHERE add_service_id=?", String.class, id)).isNull();
            mapping.updateStatus(id, "DEPLOYING", "actual-user");
            mapping.updateStatus(id, "DEPLOYING", "duplicate-user");
            mapping.updateStatus(id, null, "clear-user");
            mapping.updateStatus(id, null, "duplicate-null-user");
            assertThat(transitions(id).stream().map(call -> call.getArgument(8, String.class)))
                    .containsExactly("DEPLOYING", null);
        }
    }
}
