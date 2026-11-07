//go:build linux

package brokeradd

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/internal/deploy/readiness"
	"io.translab/tantor-agent/internal/deploy/kafka"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPhase7IsolatedBrokerAdapter(t *testing.T) {
	path := os.Getenv("TANTOR_PHASE7_REQUEST")
	if path == "" {
		t.Skip("isolated broker request not supplied")
	}
	if !strings.HasPrefix(path, "/var/tmp/tantor-add-node-phase7-requests/") {
		t.Fatal("request outside QA")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Kind, HostID, Base string
		Parameters         map[string]string
		Task api.Task
	}
	if json.Unmarshal(raw, &r) != nil || !strings.HasPrefix(r.Base, "/var/tmp/tantor-add-node-live-") || !strings.HasPrefix(r.HostID, "phase7-qa-") {
		t.Fatal("invalid isolated broker scope")
	}
	p, err := readiness.ValidateBrokerPlan(r.Parameters["broker_plan"], r.HostID)
	if err != nil {
		t.Fatal(err)
	}
	if p.NodeID < 7000 || p.NodeID > 7999 || p.SchemaVersion != "add-broker/v2" {
		t.Fatal("broker identity outside QA")
	}
	for _, path := range append(storagePaths(p), p.LogPath) {
		if !strings.HasPrefix(path, r.Base+"/") {
			t.Fatal("broker path outside QA")
		}
	}
	command := r.Parameters["command"]
	if r.Kind=="broker"&&command != "PREPARE_ADD_BROKER" && command != "FORMAT_ADD_BROKER" && command != "START_ADD_BROKER" && command != "INSPECT_ADD_BROKER" {
		t.Fatal("unsupported QA broker action")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	e:=executor.New(executor.Options{PrivilegeMode:"direct"});var output string
	if r.Kind=="broker"{output,err=run(ctx,command,r.HostID,r.Base+"/agent-state",r.Parameters,e,"/etc/tantor-kafka/add-broker","/etc/systemd/system")}else{
	 cfg:=&config.Config{};cfg.Agent.HostID=r.HostID;cfg.Paths.DataDir=r.Base+"/agent-state";deployer:=kafka.NewDeployer(cfg,nil,e)
	 switch r.Kind{case "brokerpatch":if r.Task.Parameters["config_path"]!="/etc/tantor-kafka/add-broker/"+p.OperationID+"/broker.properties"{t.Fatal("patch outside frozen broker")};output,err=deployer.PatchConfig(ctx,&r.Task);case "brokercleanup":output,err=deployer.Clean(ctx,&r.Task);default:t.Fatal("unknown isolated broker action")}
	}
	response := map[string]any{"ok": err == nil, "output": output}
	if err != nil {
		response["error"] = err.Error()
	}
	encoded, _ := json.Marshal(response)
	fmt.Printf("TANTOR_PHASE7_RESULT=%s\n", encoded)
	if err != nil {
		t.Fail()
	}
}
