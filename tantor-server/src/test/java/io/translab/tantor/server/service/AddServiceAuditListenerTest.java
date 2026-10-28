package io.translab.tantor.server.service;

import io.translab.tantor.server.audit.AuditService;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class AddServiceAuditListenerTest {
    @Configuration
    @EnableTransactionManagement
    static class Config {}

    @Test
    void committedMappingUsesTheExistingAuditServiceWithoutCredentials() {
        var jdbc = mock(JdbcTemplate.class); var audit = mock(AuditService.class);
        UUID id = UUID.randomUUID(), cluster = UUID.randomUUID();
        when(jdbc.queryForList(anyString(), eq(id))).thenReturn(List.of(Map.of("cluster_id", cluster,
                "host_id", "host-a", "process_role", "schema_registry")));
        try (var context = context(jdbc, audit)) {
            transaction().executeWithoutResult(tx -> context.publishEvent(new AddServiceAuditListener.Event(id, "SERVICE_ADDED", "PENDING", "alice")));
        }
        verify(audit).recordAs(eq("alice"), eq("MANAGEMENT_SERVER"), isNull(), eq("SERVICE_DEPLOYMENT"), eq("SERVICE_ADDED"),
                eq("DATA_SERVICE"), eq(id.toString()), eq(cluster), eq("PENDING"), isNull(), isNull(), isNull(),
                eq(Map.of("addServiceId", id.toString(), "hostId", "host-a", "processRole", "schema_registry", "serviceStatus", "PENDING")));
    }

    @Test
    void rolledBackMappingDoesNotCreateAnAuditEntry() {
        var jdbc = mock(JdbcTemplate.class); var audit = mock(AuditService.class);
        try (var context = context(jdbc, audit)) {
            transaction().executeWithoutResult(tx -> {
                context.publishEvent(new AddServiceAuditListener.Event(UUID.randomUUID(), "SERVICE_ADDED", "PENDING", "alice"));
                tx.setRollbackOnly();
            });
        }
        verifyNoInteractions(jdbc, audit);
    }

    private static AnnotationConfigApplicationContext context(JdbcTemplate jdbc, AuditService audit) {
        var context = new AnnotationConfigApplicationContext();
        context.register(Config.class);
        context.registerBean(AddServiceAuditListener.class, () -> new AddServiceAuditListener(jdbc, audit));
        context.refresh();
        return context;
    }

    private static TransactionTemplate transaction() {
        return new TransactionTemplate(new AbstractPlatformTransactionManager() {
            protected Object doGetTransaction() { return new Object(); }
            protected void doBegin(Object transaction, TransactionDefinition definition) {}
            protected void doCommit(DefaultTransactionStatus status) {}
            protected void doRollback(DefaultTransactionStatus status) {}
        });
    }
}
