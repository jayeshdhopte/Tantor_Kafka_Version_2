package dataservice

import (
	"strings"
)

// A fresh plaintext/basic installation must not inherit optional encryption
// secrets from the vendor's example configuration. Existing service reconfiguration
// stays a separate, explicit operation; existing topics are never deleted here.
func basicSchemaProperties(current string) (string, error) {
	var lines []string
	removingContinuation := false
	preservingContinuation := false
	for _, line := range strings.Split(current, "\n") {
		clean := strings.TrimSpace(line)
		trimmed := strings.TrimRight(line, " \t\r")
		slashes := len(trimmed) - len(strings.TrimRight(trimmed, `\`))
		continued := slashes%2 == 1
		if removingContinuation {
			removingContinuation = continued
			continue
		}
		if preservingContinuation {
			lines = append(lines, line)
			preservingContinuation = continued
			continue
		}
		if clean == "" || strings.HasPrefix(clean, "#") || strings.HasPrefix(clean, "!") {
			lines = append(lines, line)
			continue
		}
		if match := propertyLine.FindStringSubmatch(line); match != nil {
			switch match[1] {
			case "metadata.encoder.secret", "metadata.encoder.old.secret", "resource.extension.class", "init.resource.extension.class", "schema.registry.resource.extension.class":
				removingContinuation = continued
				continue
			}
		}
		lines = append(lines, line)
		preservingContinuation = continued
	}
	return strings.Join(lines, "\n"), nil
}
