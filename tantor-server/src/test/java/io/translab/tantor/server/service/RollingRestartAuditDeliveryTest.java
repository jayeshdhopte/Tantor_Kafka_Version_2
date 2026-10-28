package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.*;
import java.util.*;
import static org.mockito.Mockito.*;

class RollingRestartAuditDeliveryTest {
    @Configuration @EnableTransactionManagement static class Config {}

    @Test void committedEventReachesExistingAuditServiceWithItsExactOutcome() {
        var audit=mock(AuditService.class);var event=event();
        try(var context=context(audit)) {
            transaction().executeWithoutResult(tx -> context.publishEvent(event));
        }
        verify(audit).recordAs("bob","MANAGEMENT_SERVER",null,"RESTART",event.action(),"CLUSTER",
                event.clusterId().toString(),event.clusterId(),"FAILED",null,null,null,event.details());
    }
    @Test void rolledBackEventDoesNotWriteAnAuditEntry() {
        var audit=mock(AuditService.class);
        try(var context=context(audit)) {
            transaction().executeWithoutResult(tx -> {context.publishEvent(event());tx.setRollbackOnly();});
        }
        verifyNoInteractions(audit);
    }
    private static RollingRestartMappingService.AuditEvent event() {
        UUID id=UUID.randomUUID();
        return new RollingRestartMappingService.AuditEvent(id,UUID.randomUUID(),"bob","ROLLING_RESTART_PARTIAL_SUCCESS",
                "PARTIAL_SUCCESS",Map.of("jobId",id.toString(),"restartStatus","PARTIAL_SUCCESS"));
    }
    private static AnnotationConfigApplicationContext context(AuditService audit) {
        var context=new AnnotationConfigApplicationContext();context.register(Config.class);
        context.registerBean(RollingRestartMappingService.class,() -> new RollingRestartMappingService(mock(JdbcTemplate.class),
                new ObjectMapper(),mock(ExternalClusterRepository.class),context,audit));
        context.refresh();return context;
    }
    private static TransactionTemplate transaction() {
        return new TransactionTemplate(new AbstractPlatformTransactionManager() {
            protected Object doGetTransaction(){return new Object();}
            protected void doBegin(Object transaction,TransactionDefinition definition){}
            protected void doCommit(DefaultTransactionStatus status){}
            protected void doRollback(DefaultTransactionStatus status){}
        });
    }
}
