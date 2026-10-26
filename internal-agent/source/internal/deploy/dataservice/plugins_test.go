package dataservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestBundledFileStreamCopiesSelectedRuntimeAndPreservesPlugins(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux deployment shell")
	}
	root := t.TempDir()
	source, plugins := filepath.Join(root, "distribution"), filepath.Join(root, "plugins")
	os.MkdirAll(filepath.Join(source, "libs"), 0755)
	os.MkdirAll(filepath.Join(source, "share", "filestream-connectors"), 0755)
	os.MkdirAll(plugins, 0755)
	os.WriteFile(filepath.Join(source, "libs", "connect-file-4.3.0.jar"), []byte("selected runtime"), 0644)
	os.WriteFile(filepath.Join(source, "share", "filestream-connectors", "connect-file-8.1.1-ccs.jar"), []byte("different runtime"), 0644)
	custom := filepath.Join(plugins, "custom.jar")
	os.WriteFile(custom, []byte("keep"), 0644)
	run := func() error { return exec.Command("bash", "-c", bundledFileStreamShell, "--", source, plugins).Run() }
	if run() == nil {
		t.Fatal("mixed vendor bundle accepted")
	}
	os.WriteFile(filepath.Join(source, "share", "filestream-connectors", "connect-file-8.1.1-ccs.jar"), []byte("selected runtime"), 0644)
	if err := exec.Command("bash", "-c", bundledFileStreamShell, "--", source, plugins, "check").Run(); err != nil {
		t.Fatal("read-only precheck", err)
	}
	if _, err := os.Stat(filepath.Join(plugins, "tantor-filestream")); !os.IsNotExist(err) {
		t.Fatal("precheck modified plugin directory", err)
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if err := run(); err != nil {
		t.Fatal("retry", err)
	}
	target := filepath.Join(plugins, "tantor-filestream", "connect-file-4.3.0.jar")
	if body, err := os.ReadFile(target); err != nil || string(body) != "selected runtime" {
		t.Fatal(string(body), err)
	}
	if body, _ := os.ReadFile(custom); string(body) != "keep" {
		t.Fatal("custom plugin changed")
	}
	os.WriteFile(target, []byte("unrelated existing plugin"), 0644)
	if run() == nil {
		t.Fatal("overwrote conflicting plugin")
	}
	os.Remove(filepath.Join(source, "libs", "connect-file-4.3.0.jar"))
	os.RemoveAll(filepath.Join(source, "share"))
	if run() == nil {
		t.Fatal("missing bundled plugin was accepted")
	}
}

func TestSharedRuntimePluginConflictIsDetectedWithoutChangingFiles(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux deployment shell")
	}
	root := t.TempDir()
	plugins := filepath.Join(root, "connect-plugins")
	os.MkdirAll(filepath.Join(root, "libs"), 0755)
	os.MkdirAll(filepath.Join(plugins, "filestream-connectors"), 0755)
	os.WriteFile(filepath.Join(root, "libs", "connect-file-4.3.0.jar"), []byte("apache"), 0644)
	conflict := filepath.Join(plugins, "filestream-connectors", "connect-file-8.1.1-ccs.jar")
	os.WriteFile(conflict, []byte("vendor"), 0644)
	for _, mode := range []string{"check", "install"} {
		if err := exec.Command("bash", "-c", bundledFileStreamShell, "--", root, plugins, mode).Run(); err == nil {
			t.Fatal("shared mixed bundle accepted", mode)
		}
		if body, _ := os.ReadFile(conflict); string(body) != "vendor" {
			t.Fatal("conflicting existing plugin was modified")
		}
	}
	// Corrected parcel supplies only the selected runtime JAR, including on retry.
	os.Remove(conflict)
	for _, mode := range []string{"check", "install", "install", "check"} {
		if out, err := exec.Command("bash", "-c", bundledFileStreamShell, "--", root, plugins, mode).CombinedOutput(); err != nil {
			t.Fatal(mode, string(out), err)
		}
	}
}

func TestBundledConnectHealthRequiresBothClasses(t *testing.T) {
	for _, tc := range []struct {
		payload string
		valid   bool
	}{
		{`[]`, false},
		{`<html>error</html>`, false},
		{`[{"class":"org.apache.kafka.connect.file.FileStreamSourceConnector"}]`, false},
		{`[{"class":"org.apache.kafka.connect.file.FileStreamSourceConnector"},{"class":"org.apache.kafka.connect.file.FileStreamSinkConnector"},{"class":"custom.Connector"}]`, true},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/connector-plugins" {
				t.Error(r.URL.Path)
			}
			w.Write([]byte(tc.payload))
		}))
		err := verifyBundledConnectPlugins(context.Background(), server.URL)
		server.Close()
		if (err == nil) != tc.valid {
			t.Fatalf("payload %s: %v", tc.payload, err)
		}
	}
}
