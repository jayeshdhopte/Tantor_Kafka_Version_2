package kafka

// ApplyPropertyPatch validates expected values and retains unrelated properties.
func ApplyPropertyPatch(content string, changes, expected map[string]*string) (string, bool, error) {
	for key := range changes {
		if err := validatePatchKey(key); err != nil {
			return "", false, err
		}
	}
	return patchProperties(content, changes, expected)
}
func ReadableProperties(content string) (map[string]string, error) {
	return parseReadableProperties(content)
}
func WritePropertyPatch(path string, content []byte) error {
	return writePatchAtomically(path, content)
}
