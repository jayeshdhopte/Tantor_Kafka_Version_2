package io.translab.tantor.server.service;

import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.DataServiceConnection;
import io.translab.tantor.server.dto.SaveConnectionRequest;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.security.EncryptionService;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.jpa.repository.support.JpaRepositoryFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.*;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

/** Real PostgreSQL partial-index and Hibernate-flush regression tests in a disposable schema. */
@EnabledIfEnvironmentVariable(named="TANTOR_TEST_PG_URL", matches=".+")
class DataServiceConnectionDefaultsPostgresTest {
    String schema;
    JdbcTemplate admin;
    LocalContainerEntityManagerFactoryBean factory;
    TransactionTemplate tx;
    DataServiceConnectionRepository repository;
    DataServiceConnectionService service;
    UUID cluster = UUID.randomUUID();

    @BeforeEach void setup() {
        var source = new DriverManagerDataSource(System.getenv("TANTOR_TEST_PG_URL"),
                System.getenv("TANTOR_TEST_PG_USER"), System.getenv("TANTOR_TEST_PG_PASSWORD"));
        admin = new JdbcTemplate(source);
        schema = "tantor_default_test_"+UUID.randomUUID().toString().replace("-", "");
        admin.execute("create schema "+schema);
        var properties = new Properties(); properties.setProperty("currentSchema", schema);
        source.setConnectionProperties(properties);
        factory = new LocalContainerEntityManagerFactoryBean();
        factory.setDataSource(source);
        factory.setManagedTypes(PersistenceManagedTypes.of(DataServiceConnection.class.getName()));
        factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        factory.setJpaPropertyMap(Map.of("hibernate.hbm2ddl.auto","create-drop","hibernate.default_schema",schema));
        factory.afterPropertiesSet();
        admin.execute("create unique index uidx_dsc_default on "+schema+".kf_data_service_connections(cluster_id,service_type) where is_default=true and is_active=true");
        var emf = factory.getObject();
        tx = new TransactionTemplate(new JpaTransactionManager(emf));
        repository = new JpaRepositoryFactory(SharedEntityManagerCreator.createSharedEntityManager(emf))
                .getRepository(DataServiceConnectionRepository.class);
        service = new DataServiceConnectionService(repository, mock(ExternalClusterRepository.class),
                mock(EncryptionService.class), mock(AuditService.class));
    }
    @AfterEach void cleanup() {
        if (factory != null) factory.destroy();
        if (admin != null && schema != null && schema.matches("tantor_default_test_[a-f0-9]+")) admin.execute("drop schema if exists "+schema+" cascade");
    }
    SaveConnectionRequest request(String name, Boolean makeDefault) {
        var r = new SaveConnectionRequest();r.setConnectionName(name);r.setHost("127.0.0.1");
        r.setProtocol("http");r.setPort(1);r.setIsDefault(makeDefault);return r;
    }
    io.translab.tantor.server.dto.ConnectionResponse save(SaveConnectionRequest r) {
        return tx.execute(status -> service.saveConnection(cluster,"SCHEMA_REGISTRY",r,"regression-test"));
    }
    List<DataServiceConnection> rows() {
        return tx.execute(status -> repository.findByClusterIdAndServiceTypeAndIsActiveTrueOrderByConnectionNameAsc(cluster,"SCHEMA_REGISTRY"));
    }
    @Test void additionalInstancePreservesDefaultAndExplicitInsertAndUpdateSwitchesFlushInOrder() {
        var first = save(request("first",null));
        var second = save(request("second",null));
        assertThat(rows().stream().filter(r->Boolean.TRUE.equals(r.getIsDefault())).toList()).singleElement()
                .satisfies(r->assertThat(r.getId()).isEqualTo(first.getId()));
        var third = save(request("third",true));
        assertThat(rows().stream().filter(r->Boolean.TRUE.equals(r.getIsDefault())).toList()).singleElement()
                .satisfies(r->assertThat(r.getId()).isEqualTo(third.getId()));
        var edit = request("second",true);edit.setId(second.getId());save(edit);
        assertThat(rows().stream().filter(r->Boolean.TRUE.equals(r.getIsDefault())).toList()).singleElement()
                .satisfies(r->assertThat(r.getId()).isEqualTo(second.getId()));
    }
    @Test void concurrentFirstConnectionsAndExplicitSwitchesKeepExactlyOneDefault() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        try {
            var start = new CountDownLatch(1);
            var a = pool.submit(()->{start.await();return save(request("a",null));});
            var b = pool.submit(()->{start.await();return save(request("b",null));});
            start.countDown();a.get(20,TimeUnit.SECONDS);b.get(20,TimeUnit.SECONDS);
            assertThat(rows()).hasSize(2);
            assertThat(rows().stream().filter(r->Boolean.TRUE.equals(r.getIsDefault())).count()).isEqualTo(1);
            var c = pool.submit(()->save(request("c",true)));
            var d = pool.submit(()->save(request("d",true)));
            c.get(20,TimeUnit.SECONDS);d.get(20,TimeUnit.SECONDS);
            assertThat(rows()).hasSize(4);
            assertThat(rows().stream().filter(r->Boolean.TRUE.equals(r.getIsDefault())).count()).isEqualTo(1);
        } finally { pool.shutdownNow(); }
    }
    @Test void retryReusesConnectionAndDeletingDefaultSafelyElectsReplacement() {
        var first = save(request("first",null));
        var second = save(request("second",null));
        assertThat(save(request("second",null)).getId()).isEqualTo(second.getId());
        assertThat(rows()).hasSize(2);
        tx.executeWithoutResult(status->service.deleteConnection(cluster,"SCHEMA_REGISTRY",first.getId(),"regression-test"));
        assertThat(rows()).singleElement().satisfies(r->{
            assertThat(r.getId()).isEqualTo(second.getId());assertThat(r.getIsDefault()).isTrue();
        });
    }
}
