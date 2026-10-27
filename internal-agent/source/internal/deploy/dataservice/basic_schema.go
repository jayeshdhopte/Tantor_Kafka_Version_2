package dataservice

import (
	"fmt"
	"strings"
)

var schemaExtensionKeys = map[string]bool{
	"resource.extension.class":                 true,
	"init.resource.extension.class":            true,
	"schema.registry.resource.extension.class": true,
}

// Fresh basic installations discard vendor encryption secrets and only optional
// DEK extensions. Authentication and other resource extensions remain intact.
// Existing services use a separate, non-migrating retry path.
func basicSchemaProperties(current string) (string, error) {
	lines := strings.Split(strings.ReplaceAll(current, "\r\n", "\n"), "\n")
	var result []string
	for i := 0; i < len(lines); i++ {
		first := lines[i]
		clean := strings.TrimSpace(first)
		if clean == "" || strings.HasPrefix(clean, "#") || strings.HasPrefix(clean, "!") {
			result = append(result, first)
			continue
		}
		logical := first
		original := []string{first}
		for {
			trimmed := strings.TrimRight(logical, " \t\r")
			slashes := len(trimmed) - len(strings.TrimRight(trimmed, `\`))
			if slashes%2 == 0 {
				break
			}
			if i+1 == len(lines) {
				return "", fmt.Errorf("Schema Registry properties end in an unfinished continuation")
			}
			i++
			original = append(original, lines[i])
			logical = strings.TrimSuffix(trimmed, `\`) + strings.TrimLeft(lines[i], " \t\r")
		}
		match := propertyLine.FindStringSubmatch(logical)
		if match == nil {
			bare := strings.TrimSpace(logical)
			if strings.Contains(bare, `\`) {
				return "", fmt.Errorf("escaped Schema Registry property names require an explicit configuration review")
			}
			if bare == "metadata.encoder.secret" || bare == "metadata.encoder.old.secret" {
				continue
			}
			if schemaExtensionKeys[bare] {
				result = append(result, bare+"=")
				continue
			}
			result = append(result, original...)
			continue
		}
		key, value := match[1], match[2]
		if strings.Contains(key, `\`) {
			return "", fmt.Errorf("escaped Schema Registry property names require an explicit configuration review")
		}
		if key == "metadata.encoder.secret" || key == "metadata.encoder.old.secret" {
			continue
		}
		if schemaExtensionKeys[key] {
			if strings.Contains(value, `\`) {
				return "", fmt.Errorf("escaped Schema Registry extension settings require an explicit configuration review")
			}
			var keep []string
			for _, class := range strings.Split(value, ",") {
				class = strings.TrimSpace(class)
				if class != "" && !strings.HasPrefix(class, "io.confluent.dekregistry.") {
					keep = append(keep, class)
				}
			}
			result = append(result, key+"="+strings.Join(keep, ","))
		} else {
			result = append(result, original...)
		}
	}
	return strings.Join(result, "\n"), nil
}

// Generated basic defaults must not erase existing authentication extensions.
func preserveSchemaExtensions(current, desired string) (string, error) {
	values := map[string]string{}
	for _, line := range strings.Split(current, "\n") {
		if match := propertyLine.FindStringSubmatch(line); match != nil && schemaExtensionKeys[match[1]] {
			if strings.Contains(match[2], `\`) {
				return "", fmt.Errorf("existing continued/escaped extension settings require a reviewed migration")
			}
			values[match[1]] = match[2]
		}
	}
	lines := strings.Split(desired, "\n")
	for i, line := range lines {
		if match := propertyLine.FindStringSubmatch(line); match != nil && schemaExtensionKeys[match[1]] {
			if value, ok := values[match[1]]; ok {
				lines[i] = match[1] + "=" + value
			}
		}
	}
	return strings.Join(lines, "\n"), nil
}

// systemd expands $$ to a literal $ before invoking bash. Only optional
// encryption overrides are refused; unrelated JVM/authentication options survive.
const basicSchemaStartupCheck = `ExecStartPre=/bin/bash -c 'case "$${SCHEMA_REGISTRY_OPTS-} $${JAVA_TOOL_OPTIONS-} $${_JAVA_OPTIONS-} $${JDK_JAVA_OPTIONS-} $${JAVA_OPTS-} $${SCHEMA_REGISTRY_RESOURCE_EXTENSION_CLASS-} $${RESOURCE_EXTENSION_CLASS-} $${SCHEMA_REGISTRY_INIT_RESOURCE_EXTENSION_CLASS-} $${INIT_RESOURCE_EXTENSION_CLASS-}" in *metadata.encoder.secret*|*metadata.encoder.old.secret*|*io.confluent.dekregistry.*) echo "Basic Schema Registry profile conflicts with inherited encryption/DEK options; review the service environment" >&2; exit 1;; esac'
`
