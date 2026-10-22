package kafka

import (
	"fmt"
	"regexp"
	"strings"
)

var recoveryCheckpointPattern = regexp.MustCompile(`^rollback-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
var recoveryNodePattern = regexp.MustCompile(`(?m)^\s*(?:node\.id|broker\.id)\s*[=:]\s*([0-9]+)\s*$`)

func validateRecoveryNode(parameters map[string]string, content string) error {
	if parameters["config_recovery"] != "true" {
		return nil
	}
	matches := recoveryNodePattern.FindAllStringSubmatch(content, -1)
	if len(matches) == 0 || parameters["node_id"] == "" {
		return fmt.Errorf("cannot verify recovery node identity")
	}
	for _, match := range matches {
		if match[1] != parameters["node_id"] {
			return fmt.Errorf("configuration node identity changed; recovery stopped")
		}
	}
	return nil
}

// ValidateConfigRecovery keeps recovery scoped to a job and a concrete property
// set. Nil means remove a previously absent override, never write an empty value.
func ValidateConfigRecovery(parameters map[string]string, changes, expected map[string]*string) error {
	if parameters["config_recovery"] != "true" {
		return nil
	}
	if !recoveryCheckpointPattern.MatchString(parameters["checkpoint_id"]) {
		return fmt.Errorf("job recovery checkpoint identity is required")
	}
	if len(changes) == 0 || len(changes) != len(expected) {
		return fmt.Errorf("recovery requires matching property and expected-value sets")
	}
	for key, value := range changes {
		if err := validatePatchKey(key); err != nil {
			return err
		}
		if _, ok := expected[key]; !ok {
			return fmt.Errorf("missing recovery expectation for %s", key)
		}
		if value != nil && strings.TrimSpace(*value) == "" {
			return fmt.Errorf("blank recovery value for %s; use null for absence", key)
		}
	}
	return nil
}
