package dataservice

import (
	"strings"
	"testing"
)

func TestBasicSchemaDisablesOptionalExtensionsAndEnvironmentSecrets(t *testing.T) {
	task := taskFor()
	task.Parameters["install_dir"] = "/opt/tantor/schema-registry"
	settings, err := Parse(Schema, task)
	if err != nil {
		t.Fatal(err)
	}
	files := ConfigFiles(settings, "/opt/java")
	for _, key := range []string{"resource.extension.class=\n", "init.resource.extension.class=\n", "schema.registry.resource.extension.class=\n"} {
		if !strings.Contains(files[0].Content, key) {
			t.Fatal(files[0].Content)
		}
	}
	if !strings.Contains(files[2].Content, "UnsetEnvironment=METADATA_ENCODER_SECRET METADATA_ENCODER_OLD_SECRET") {
		t.Fatal(files[2].Content)
	}
	if strings.Contains(files[0].Content, "metadata.encoder.secret=") {
		t.Fatal("empty secret would still enable metadata encryption")
	}
}

func TestBasicSchemaRemovesContinuedOptionalPropertiesAndPreservesOtherSettings(t *testing.T) {
	current := "metadata.encoder.secret example\nresource.extension.class=first,\\\n second\nother=first,\\\n second\n"
	cleaned, err := basicSchemaProperties(current)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cleaned, "metadata.encoder") || strings.Contains(cleaned, "resource.extension") || cleaned != "other=first,\\\n second\n" {
		t.Fatal(cleaned)
	}
}

func TestBasicSchemaDoesNotInheritEncryptionSettings(t *testing.T) {
	current, err := basicSchemaProperties("# vendor config\nmetadata.encoder.secret=example\nmetadata.encoder.old.secret: old\nresource.extension.class=io.confluent.dekregistry.DekRegistryResourceExtension\nunrelated.setting=keep\n")
	if err != nil {
		t.Fatal(err)
	}
	merged, err := mergeConnectProperties(current, "resource.extension.class=\ninit.resource.extension.class=\nschema.registry.resource.extension.class=\n")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(merged, "metadata.encoder.secret") || strings.Contains(merged, "metadata.encoder.old.secret") || strings.Contains(merged, "DekRegistryResourceExtension") {
		t.Fatal(merged)
	}
	if !strings.Contains(merged, "unrelated.setting=keep") {
		t.Fatal(merged)
	}
}
