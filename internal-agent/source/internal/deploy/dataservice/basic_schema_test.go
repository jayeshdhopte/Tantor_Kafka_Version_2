package dataservice

import (
	"os"
	"os/exec"
	"runtime"
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
	current := "metadata.encoder.secret example\nresource.extension.class=io.confluent.dekregistry.DekRegistryResourceExtension,\\\n com.example.AuthExtension\nother=first,\\\n second\n"
	cleaned, err := basicSchemaProperties(current)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cleaned, "metadata.encoder") || strings.Contains(cleaned, "dekregistry") || cleaned != "resource.extension.class=com.example.AuthExtension\nother=first,\\\n second\n" {
		t.Fatal(cleaned)
	}
}

func TestBasicProfilePreservesSecurityAndOtherExtensionsAcrossTopologies(t *testing.T) {
	for _, peers := range []string{"", "broker-a:8081,broker-b:8081"} {
		for _, shared := range []bool{false, true} {
			task := taskFor()
			task.Parameters["sr_ha_peers"] = peers
			if shared {
				task.Parameters["install_dir"] = task.Parameters["kafka_install_dir"]
				if task.Parameters["install_dir"] == "" {
					task.Parameters["install_dir"] = "/opt/kafka"
					task.Parameters["kafka_install_dir"] = "/opt/kafka"
				}
				task.Parameters["config_dir"] = task.Parameters["install_dir"] + "/etc/schema-registry"
			} else {
				task.Parameters["install_dir"] = "/opt/tantor/schema-registry"
			}
			s, err := Parse(Schema, task)
			if err != nil {
				t.Fatal(err)
			}
			current := "resource.extension.class=com.example.AuthExtension,io.confluent.dekregistry.DekRegistryResourceExtension\nmetadata.encoder.secret=vendor\nkafkastore.security.protocol=SASL_SSL\nkafkastore.sasl.jaas.config=keep-private\nssl.keystore.location=/secure/keystore\n"
			cleaned, err := basicSchemaProperties(current)
			if err != nil {
				t.Fatal(err)
			}
			desired, err := preserveSchemaExtensions(cleaned, ConfigFiles(s, "/opt/java")[0].Content)
			if err != nil {
				t.Fatal(err)
			}
			merged, err := mergeConnectProperties(cleaned, desired)
			if err != nil {
				t.Fatal(err)
			}
			for _, keep := range []string{"resource.extension.class=com.example.AuthExtension", "kafkastore.security.protocol=SASL_SSL", "kafkastore.sasl.jaas.config=keep-private", "ssl.keystore.location=/secure/keystore"} {
				if !strings.Contains(merged, keep) {
					t.Fatal("security setting removed", keep)
				}
			}
			if strings.Contains(merged, "dekregistry") || strings.Contains(merged, "metadata.encoder.secret") {
				t.Fatal("optional encryption inherited")
			}
		}
	}
}

func TestBasicProfileStartupGuardRejectsOnlyOptionalOverrides(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux systemd shell")
	}
	shell := strings.TrimSuffix(strings.TrimPrefix(basicSchemaStartupCheck, "ExecStartPre=/bin/bash -c '"), "'\n")
	shell = strings.ReplaceAll(shell, "$$", "$")
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"", true}, {"-Djavax.net.ssl.trustStore=/secure/store -Dauth.enabled=true", true},
		{"-Dmetadata.encoder.secret=not-logged", false}, {"io.confluent.dekregistry.DekRegistryResourceExtension", false},
	} {
		cmd := exec.Command("bash", "-c", shell)
		cmd.Env = append(os.Environ(), "SCHEMA_REGISTRY_OPTS="+tc.value, "JAVA_TOOL_OPTIONS=", "_JAVA_OPTIONS=", "JDK_JAVA_OPTIONS=", "JAVA_OPTS=", "RESOURCE_EXTENSION_CLASS=", "SCHEMA_REGISTRY_RESOURCE_EXTENSION_CLASS=", "INIT_RESOURCE_EXTENSION_CLASS=", "SCHEMA_REGISTRY_INIT_RESOURCE_EXTENSION_CLASS=")
		out, err := cmd.CombinedOutput()
		if (err == nil) != tc.valid {
			t.Fatalf("guard valid=%v: %v", tc.valid, err)
		}
		if strings.Contains(string(out), "not-logged") {
			t.Fatal("secret leaked in diagnostic")
		}
	}
}

func TestExistingSchemaRetryPreservesItsEncryptionProfile(t *testing.T) {
	task := taskFor()
	task.Parameters["install_dir"] = "/opt/tantor/schema-registry"
	s, err := Parse(Schema, task)
	if err != nil {
		t.Fatal(err)
	}
	desired := ConfigFiles(s, "/usr")[0].Content
	current := strings.Replace(desired, "resource.extension.class=\n", "resource.extension.class=io.confluent.dekregistry.DekRegistryResourceExtension,com.example.AuthExtension\n", 1) + "metadata.encoder.secret=existing-secret\n"
	want, err := preserveSchemaExtensions(current, desired)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := portConfiguration(current, want, true)
	if err != nil {
		t.Fatal(err)
	}
	if updated != current {
		t.Fatal("retry changed existing encryption settings")
	}
}

func TestBasicSchemaBareAndEscapedKeys(t *testing.T) {
	cleaned, err := basicSchemaProperties("metadata.encoder.secret\nmetadata.encoder.old.secret\nresource.extension.class\n")
	if err != nil || cleaned != "resource.extension.class=\n" {
		t.Fatal(cleaned, err)
	}
	for _, input := range []string{`metadata.\u0065ncoder.secret=value`, `metadata.\u0065ncoder.secret`, "other=unfinished\\"} {
		if _, err := basicSchemaProperties(input); err == nil {
			t.Fatal("ambiguous configuration accepted")
		}
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
