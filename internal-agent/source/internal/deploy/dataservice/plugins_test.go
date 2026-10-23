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
