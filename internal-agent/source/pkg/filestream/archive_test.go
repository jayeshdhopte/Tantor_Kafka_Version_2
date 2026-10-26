package filestream

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParcelConsistencyBeforeExtraction(t *testing.T) {
	for _, scenario := range []string{"bare-kafka", "canonical-only", "identical-copies", "mixed-builds", "truncated"} {
		t.Run(scenario, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "parcel.tgz")
			file, _ := os.Create(filename)
			gz := gzip.NewWriter(file)
			writer := tar.NewWriter(gz)
			files := map[string]string{"root/libs/kafka-runtime.jar": "runtime"}
			if scenario != "bare-kafka" {
				files["root/libs/connect-file-4.3.0.jar"] = "canonical"
			}
			if scenario == "identical-copies" {
				files["root/connect-plugins/tantor-filestream/connect-file-4.3.0.jar"] = "canonical"
			}
			if scenario == "mixed-builds" {
				files["root/connect-plugins/filestream-connectors/connect-file-8.1.1-ccs.jar"] = "vendor"
			}
			for name, body := range files {
				writer.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(body))})
				writer.Write([]byte(body))
			}
			writer.Close()
			gz.Close()
			file.Close()
			if scenario == "truncated" {
				os.Truncate(filename, 20)
			}
			err := ValidateArchive(filename)
			if scenario == "mixed-builds" {
				if err == nil || !strings.Contains(err.Error(), "inconsistent FileStream parcel") {
					t.Fatal(err)
				}
			} else if scenario == "truncated" {
				if err == nil {
					t.Fatal("truncated archive accepted")
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
