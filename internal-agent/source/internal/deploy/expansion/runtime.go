package expansion

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
)

// ArtifactManifest binds actual classpath bytes to an independently checksum-
// verified distribution. A version string in a directory name is not evidence.
type ArtifactManifest struct {
	ArchiveSHA256 string            `json:"archiveSha256"`
	KafkaVersion  string            `json:"kafkaVersion"`
	Files         map[string]string `json:"files"`
}
type RuntimeProof struct {
	SchemaVersion   string `json:"schemaVersion"`
	ArchiveSHA256   string `json:"archiveSha256"`
	KafkaVersion    string `json:"kafkaVersion"`
	RuntimeHome     string `json:"runtimeHome"`
	ClasspathDigest string `json:"classpathDigest"`
	ProofSHA256     string `json:"proofSha256"`
	LibraryCount    int    `json:"libraryCount"`
}

// Manifest reads only a bounded tar stream. It does not extract or execute it.
// The compressed bytes are checksum-verified on this same read, including the
// gzip trailer. A changed file between a preliminary hash and open cannot pass.
func Manifest(ctx context.Context, archive io.Reader, checksum, version string) (ArtifactManifest, error) {
	m := ArtifactManifest{ArchiveSHA256: checksum, KafkaVersion: version, Files: map[string]string{}}
	if !hash.MatchString(checksum) || (version != "4.1.0" && version != "4.1.1" && version != "4.1.2" && version != "4.3.0") {
		return m, fmt.Errorf("approved artifact identity required")
	}
	compressed := &io.LimitedReader{R: &contextReader{ctx: ctx, reader: archive}, N: 1024*1024*1024 + 1}
	archiveHash := sha256.New()
	verifiedReader := io.TeeReader(compressed, archiveHash)
	gzipReader, err := gzip.NewReader(verifiedReader)
	if err != nil {
		return m, err
	}
	defer gzipReader.Close()
	tr := tar.NewReader(gzipReader)
	root := ""
	seen := map[string]bool{}
	var total int64
	for count := 0; ; count++ {
		if err := ctx.Err(); err != nil {
			return m, err
		}
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return m, err
		}
		if count >= 20000 {
			return m, fmt.Errorf("archive entry limit exceeded")
		}
		name := strings.TrimSuffix(header.Name, "/")
		if name == "" || path.IsAbs(name) || path.Clean(name) != name || strings.ContainsAny(name, "\\\x00\r\n") || strings.HasPrefix(name, "../") || name == ".." || seen[name] {
			return m, fmt.Errorf("noncanonical or duplicate archive entry")
		}
		seen[name] = true
		parts := strings.SplitN(name, "/", 2)
		if root == "" {
			root = parts[0]
		}
		if root != parts[0] {
			return m, fmt.Errorf("single distribution root required")
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return m, fmt.Errorf("archive links and special files are unsupported")
		}
		if header.Size < 0 || header.Size > 256*1024*1024 || total > 2*1024*1024*1024-header.Size {
			return m, fmt.Errorf("archive size limit exceeded")
		}
		total += header.Size
		if len(parts) != 2 {
			return m, fmt.Errorf("unexpected archive root file")
		}
		relative := parts[1]
		if !strings.HasPrefix(relative, "bin/") && !strings.HasPrefix(relative, "libs/") {
			continue
		}
		sum := sha256.New()
		n, err := io.Copy(sum, &contextReader{ctx: ctx, reader: tr})
		if err != nil || n != header.Size {
			return m, fmt.Errorf("incomplete approved runtime file")
		}
		m.Files[relative] = hex.EncodeToString(sum.Sum(nil))
	}
	remaining, err := io.Copy(io.Discard, io.LimitReader(gzipReader, 2*1024*1024*1024-total+1))
	if err != nil || remaining > 2*1024*1024*1024-total {
		return m, fmt.Errorf("archive trailer or expanded size is invalid")
	}
	if _, err = io.Copy(io.Discard, verifiedReader); err != nil {
		return m, err
	}
	if compressed.N <= 0 || hex.EncodeToString(archiveHash.Sum(nil)) != checksum {
		return m, fmt.Errorf("approved archive checksum mismatch")
	}
	if m.Files["bin/kafka-run-class.sh"] == "" || m.Files["bin/kafka-server-start.sh"] == "" || m.Files["bin/kafka-storage.sh"] == "" || m.Files["bin/kafka-metadata-quorum.sh"] == "" || m.Files["libs/kafka_2.13-"+version+".jar"] == "" || m.Files["libs/kafka-clients-"+version+".jar"] == "" {
		return m, fmt.Errorf("approved distribution lacks required Kafka runtime")
	}
	return m, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(b)
}

// RuntimeFiles must reject symlinks, special files, changes while reading and
// unbounded data. Keeping that I/O at the adapter boundary enables fault tests.
type RuntimeFiles interface {
	Canonical(context.Context, string) (string, error)
	SHA256(context.Context, string) (string, error)
	Libraries(context.Context, string) ([]string, error)
}

func VerifyRuntime(ctx context.Context, m ArtifactManifest, home, config string, args []string, approvedAgents map[string]string, files RuntimeFiles) (RuntimeProof, error) {
	proof := RuntimeProof{SchemaVersion: "add-node-runtime/v1", ArchiveSHA256: m.ArchiveSHA256, KafkaVersion: m.KafkaVersion, RuntimeHome: home}
	if !safePath(home) || !safeConfig(config) || !hash.MatchString(m.ArchiveSHA256) || len(args) < 5 || len(args) > 4096 || path.Base(args[0]) != "java" {
		return proof, fmt.Errorf("bounded reviewed Java command required")
	}
	classpath := ""
	cpCount := 0
	mainIndex := -1
	agentPaths := map[string]bool{}
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == "-cp" || arg == "-classpath" || arg == "--class-path" {
			cpCount++
			if i+1 >= len(args) || mainIndex != -1 {
				return proof, fmt.Errorf("invalid Java classpath option")
			}
			i++
			classpath = args[i]
			continue
		}
		if arg == "kafka.Kafka" {
			if mainIndex != -1 {
				return proof, fmt.Errorf("duplicate Kafka main class")
			}
			mainIndex = i
			continue
		}
		if strings.HasPrefix(arg, "-javaagent:") {
			if mainIndex != -1 {
				return proof, fmt.Errorf("unexpected Kafka application argument")
			}
			agent := strings.SplitN(strings.TrimPrefix(arg, "-javaagent:"), "=", 2)[0]
			resolved, err := files.Canonical(ctx, agent)
			if err != nil {
				return proof, fmt.Errorf("Java agent path is unavailable")
			}
			expected := approvedAgents[resolved]
			if !safePath(resolved) || !hash.MatchString(expected) || agentPaths[resolved] {
				return proof, fmt.Errorf("unapproved or duplicate Java agent")
			}
			actual, err := files.SHA256(ctx, resolved)
			if err != nil || actual != expected {
				return proof, fmt.Errorf("Java agent bytes differ from approved artifact")
			}
			again, err := files.Canonical(ctx, agent)
			if err != nil || again != resolved {
				return proof, fmt.Errorf("Java agent path changed while inspecting")
			}
			agentPaths[resolved] = true
			continue
		}
		lower := strings.ToLower(arg)
		if mainIndex != -1 {
			if i != mainIndex+1 || arg != config {
				return proof, fmt.Errorf("unexpected Kafka application argument")
			}
			continue
		}
		if !strings.HasPrefix(arg, "-") || arg == "-jar" || arg == "-m" || strings.HasPrefix(arg, "--module") || arg == "-p" || strings.HasPrefix(lower, "-agent") || strings.HasPrefix(lower, "-xbootclasspath") || strings.HasPrefix(lower, "-djava.system.class.loader") || strings.HasPrefix(lower, "-djava.class.path") || strings.HasPrefix(lower, "-djava.ext.dirs") || strings.HasPrefix(lower, "-djava.endorsed.dirs") || strings.HasPrefix(lower, "--patch-module") || strings.HasPrefix(arg, "--class-path=") {
			return proof, fmt.Errorf("unsupported Java class-loading override")
		}
	}
	if cpCount != 1 || classpath == "" || mainIndex < 1 || mainIndex+2 != len(args) || args[mainIndex+1] != config {
		return proof, fmt.Errorf("Kafka command is not bound to one reviewed configuration")
	}
	for _, script := range []string{"bin/kafka-run-class.sh", "bin/kafka-server-start.sh", "bin/kafka-storage.sh", "bin/kafka-metadata-quorum.sh"} {
		expected := m.Files[script]
		actual, err := files.SHA256(ctx, path.Join(home, script))
		if expected == "" || err != nil || actual != expected {
			return proof, fmt.Errorf("Kafka startup script differs from approved artifact")
		}
	}
	seen := map[string]bool{}
	rows := []string{}
	hasCore, hasClient := false, false
	for _, entry := range strings.Split(classpath, ":") {
		if entry == "" {
			return proof, fmt.Errorf("working-directory classpath entries are unsupported")
		}
		paths := []string{entry}
		if strings.HasSuffix(entry, "/*") {
			dir, err := files.Canonical(ctx, strings.TrimSuffix(entry, "/*"))
			if err != nil {
				return proof, fmt.Errorf("classpath directory is unavailable")
			}
			if dir != path.Join(home, "libs") {
				return proof, fmt.Errorf("unapproved wildcard classpath")
			}
			paths, err = files.Libraries(ctx, dir)
			if err != nil || len(paths) == 0 || len(paths) > 4096 {
				return proof, fmt.Errorf("classpath enumeration unavailable")
			}
		}
		for _, file := range paths {
			original := file
			resolved, err := files.Canonical(ctx, file)
			if err != nil {
				return proof, fmt.Errorf("classpath library cannot be resolved")
			}
			file = resolved
			if len(seen) >= 4096 || !safePath(file) || path.Dir(file) != path.Join(home, "libs") || !strings.HasSuffix(file, ".jar") || seen[file] {
				return proof, fmt.Errorf("foreign or ambiguous classpath entry")
			}
			seen[file] = true
			relative := strings.TrimPrefix(file, home+"/")
			expected := m.Files[relative]
			actual, err := files.SHA256(ctx, file)
			if expected == "" || err != nil || expected != actual {
				return proof, fmt.Errorf("classpath library differs from approved distribution")
			}
			again, err := files.Canonical(ctx, original)
			if err != nil || again != file {
				return proof, fmt.Errorf("classpath path changed during inspection")
			}
			hasCore = hasCore || relative == "libs/kafka_2.13-"+m.KafkaVersion+".jar"
			hasClient = hasClient || relative == "libs/kafka-clients-"+m.KafkaVersion+".jar"
			rows = append(rows, relative+":"+actual)
		}
		if strings.HasSuffix(entry, "/*") {
			again, err := files.Canonical(ctx, strings.TrimSuffix(entry, "/*"))
			if err != nil || again != path.Join(home, "libs") {
				return proof, fmt.Errorf("classpath directory changed during inspection")
			}
		}
	}
	if !hasCore || !hasClient {
		return proof, fmt.Errorf("exact approved Kafka core and client libraries required")
	}
	sort.Strings(rows)
	proof.ClasspathDigest = Digest(strings.Join(rows, "\n"))
	proof.LibraryCount = len(rows)
	raw, _ := json.Marshal(proof)
	proof.ProofSHA256 = Digest(string(raw))
	return proof, nil
}
