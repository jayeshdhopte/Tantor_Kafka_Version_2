package expansion

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"testing"
)

func TestApprovedLocalDistributionManifest(t *testing.T) {
	name := os.Getenv("TANTOR_EXPANSION_TEST_ARCHIVE")
	if name == "" {
		t.Skip("explicit local Kafka distribution not supplied")
	}
	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sum := sha256.New()
	if _, err = io.Copy(sum, f); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	checksum := hex.EncodeToString(sum.Sum(nil))
	m, err := Manifest(context.Background(), f, checksum, "4.3.0")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Kafka %s approved archive %s: %d runtime files verified", m.KafkaVersion, m.ArchiveSHA256, len(m.Files))
}

type runtimeFiles struct {
	files     map[string]string
	libraries []string
	fail      string
}

func (f runtimeFiles) SHA256(_ context.Context, p string) (string, error) {
	if p == f.fail {
		return "", errors.New("changed inode")
	}
	return f.files[p], nil
}
func (f runtimeFiles) Libraries(_ context.Context, p string) ([]string, error) {
	if p == f.fail {
		return nil, errors.New("unreadable")
	}
	return f.libraries, nil
}
func runtimeFixture() (ArtifactManifest, runtimeFiles, []string) {
	m := ArtifactManifest{ArchiveSHA256: strings.Repeat("a", 64), KafkaVersion: "4.3.0", Files: map[string]string{}}
	f := runtimeFiles{files: map[string]string{}}
	for _, name := range []string{"bin/kafka-run-class.sh", "bin/kafka-server-start.sh", "bin/kafka-storage.sh", "bin/kafka-metadata-quorum.sh", "libs/kafka_2.13-4.3.0.jar", "libs/kafka-clients-4.3.0.jar", "libs/utility.jar"} {
		m.Files[name] = Digest(name)
		f.files[path.Join("/opt/kafka", name)] = Digest(name)
		if strings.HasPrefix(name, "libs/") {
			f.libraries = append(f.libraries, path.Join("/opt/kafka", name))
		}
	}
	return m, f, []string{"/opt/java/bin/java", "-Xmx512m", "-cp", strings.Join(f.libraries, ":"), "kafka.Kafka", "/etc/kafka/controller.properties"}
}
func TestRuntimeProofChecksActualClasspathWithoutRequiringUnusedDistributionJars(t *testing.T) {
	m, f, args := runtimeFixture()
	m.Files["libs/connect-file-4.3.0.jar"] = Digest("unused")
	proof, err := VerifyRuntime(context.Background(), m, "/opt/kafka", args[len(args)-1], args, nil, f)
	if err != nil || proof.LibraryCount != 3 || !hash.MatchString(proof.ProofSHA256) {
		t.Fatal(proof, err)
	}
	args[3] = "/opt/kafka/libs/*"
	wildcard, err := VerifyRuntime(context.Background(), m, "/opt/kafka", args[len(args)-1], args, nil, f)
	if err != nil || wildcard != proof {
		t.Fatal(wildcard, err)
	}
}
func TestRuntimeRejectsForeignLibrariesModifiedScriptsAndUnreadableFiles(t *testing.T) {
	for _, change := range []func(*ArtifactManifest, *runtimeFiles, []string){func(_ *ArtifactManifest, f *runtimeFiles, _ []string) {
		f.files["/opt/kafka/libs/kafka-clients-4.3.0.jar"] = Digest("modified")
	}, func(_ *ArtifactManifest, f *runtimeFiles, _ []string) { f.fail = "/opt/kafka/bin/kafka-run-class.sh" }, func(_ *ArtifactManifest, _ *runtimeFiles, args []string) { args[3] += " :/tmp/evil.jar" }, func(_ *ArtifactManifest, _ *runtimeFiles, args []string) { args[3] = "/opt/kafka/libs/utility.jar" }, func(_ *ArtifactManifest, _ *runtimeFiles, args []string) { args[3] = "/opt/kafka/libs/*:" }} {
		m, f, args := runtimeFixture()
		change(&m, &f, args)
		if _, err := VerifyRuntime(context.Background(), m, "/opt/kafka", args[len(args)-1], args, nil, f); err == nil {
			t.Fatal("foreign runtime accepted")
		}
	}
}
func TestJavaClassLoadingOverridesAndDifferentMainClassAreRejected(t *testing.T) {
	for _, flag := range []string{"@/tmp/args", "-jar", "-javaagent:/tmp/evil.jar", "-agentpath:/tmp/evil.so", "-Xbootclasspath/a:/tmp/evil.jar", "-Djava.system.class.loader=evil.Loader", "--module-path=/tmp", "evil.Main"} {
		m, f, args := runtimeFixture()
		args = append(args[:1], append([]string{flag}, args[1:]...)...)
		if _, err := VerifyRuntime(context.Background(), m, "/opt/kafka", args[len(args)-1], args, nil, f); err == nil {
			t.Fatal("unsafe Java option accepted: " + flag)
		}
	}
}
func TestApprovedExporterMustMatchItsActualBytes(t *testing.T) {
	m, f, args := runtimeFixture()
	f.files["/opt/tantor/jmx.jar"] = Digest("jmx")
	args = append(args[:1], append([]string{"-javaagent:/opt/tantor/jmx.jar=7071:/etc/kafka/jmx.yml"}, args[1:]...)...)
	if _, err := VerifyRuntime(context.Background(), m, "/opt/kafka", args[len(args)-1], args, map[string]string{"/opt/tantor/jmx.jar": Digest("jmx")}, f); err != nil {
		t.Fatal(err)
	}
	f.files["/opt/tantor/jmx.jar"] = Digest("different")
	if _, err := VerifyRuntime(context.Background(), m, "/opt/kafka", args[len(args)-1], args, map[string]string{"/opt/tantor/jmx.jar": Digest("jmx")}, f); err == nil {
		t.Fatal("changed exporter accepted")
	}
}
func archiveFixture(t *testing.T, extra *tar.Header) []byte {
	t.Helper()
	var raw bytes.Buffer
	gz := gzip.NewWriter(&raw)
	tw := tar.NewWriter(gz)
	for _, name := range []string{"bin/kafka-run-class.sh", "bin/kafka-server-start.sh", "bin/kafka-storage.sh", "bin/kafka-metadata-quorum.sh", "libs/kafka_2.13-4.3.0.jar", "libs/kafka-clients-4.3.0.jar"} {
		content := []byte(name)
		if err := tw.WriteHeader(&tar.Header{Name: "kafka_2.13-4.3.0/" + name, Mode: 0644, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if extra != nil {
		if err := tw.WriteHeader(extra); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return raw.Bytes()
}
func TestManifestOnlyReadsBoundedCanonicalRegularDistributionFiles(t *testing.T) {
	raw := archiveFixture(t, nil)
	m, err := Manifest(context.Background(), bytes.NewReader(raw), Digest(string(raw)), "4.3.0")
	if err != nil || len(m.Files) != 6 {
		t.Fatal(m, err)
	}
	if _, err := Manifest(context.Background(), bytes.NewReader(raw), strings.Repeat("a", 64), "4.3.0"); err == nil {
		t.Fatal("incorrect archive checksum accepted")
	}
	for _, h := range []*tar.Header{{Name: "kafka_2.13-4.3.0/libs/link.jar", Typeflag: tar.TypeSymlink, Linkname: "/tmp/evil"}, {Name: "../evil", Typeflag: tar.TypeReg}, {Name: "other/libs/evil.jar", Typeflag: tar.TypeReg}, {Name: "kafka_2.13-4.3.0/bin/kafka-run-class.sh", Typeflag: tar.TypeReg}} {
		if _, err := Manifest(context.Background(), bytes.NewReader(archiveFixture(t, h)), strings.Repeat("a", 64), "4.3.0"); err == nil {
			t.Fatal("unsafe archive accepted", h.Name)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Manifest(ctx, bytes.NewReader(raw), strings.Repeat("a", 64), "4.3.0"); err == nil {
		t.Fatal("cancelled scan completed")
	}
}
