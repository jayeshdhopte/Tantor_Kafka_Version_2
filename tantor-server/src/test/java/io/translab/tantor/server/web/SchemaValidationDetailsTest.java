package io.translab.tantor.server.web;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaValidationDetailsTest {
    @Test
    void extractsCompatibilityReasonAndAffectedField() {
        assertThat(DataServicesController.schemaValidationDetails("""
                {"error_code":409,"message":"Schema being registered is incompatible with an earlier schema",
                 "details":[{"description":"Field type changed from int to string","field":"age"}]}
                """))
                .containsEntry("validationReason", "Field type changed from int to string")
                .containsEntry("affectedField", "age");
        assertThat(DataServicesController.schemaValidationDetails("""
                {"error_code":409,"message":"Schema being registered is incompatible with an earlier schema"}
                """))
                .containsEntry("validationReason", "Schema being registered is incompatible with an earlier schema")
                .doesNotContainKey("affectedField");
    }

    @Test
    void doesNotExposeRawSchemaOrCredentialBearingErrors() {
        assertThat(DataServicesController.schemaValidationDetails("""
                {"message":"schema={\\"password\\":\\"private\\"}"}
                """)).isEmpty();
        assertThat(DataServicesController.schemaValidationDetails("""
                {"message":"Authorization token was rejected"}
                """)).isEmpty();
    }
}
