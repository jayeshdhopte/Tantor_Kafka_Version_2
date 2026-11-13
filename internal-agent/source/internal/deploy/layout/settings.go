// Package layout holds the explicitly approved candidate deployment layout.
package layout

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

type Settings struct {
	InstallPath            string   `json:"installPath"`
	ConfigDirectory        string   `json:"configDirectory"`
	JavaHome               string   `json:"javaHome"`
	RuntimeUser            string   `json:"runtimeUser"`
	BrokerDataPaths        []string `json:"brokerDataPaths"`
	BrokerMetadataPath     string   `json:"brokerMetadataPath"`
	BrokerLogPath          string   `json:"brokerLogPath"`
	BrokerHeapXmx          string   `json:"brokerHeapXmx"`
	ControllerDataPaths    []string `json:"controllerDataPaths"`
	ControllerMetadataPath string   `json:"controllerMetadataPath"`
	ControllerLogPath      string   `json:"controllerLogPath"`
	ControllerHeapXmx      string   `json:"controllerHeapXmx"`
}

func Approved(p string) bool {
	return regexp.MustCompile(`^/(opt|var|data|srv|app)/[A-Za-z0-9_./-]+$`).MatchString(p) && filepath.ToSlash(filepath.Clean(p)) == p && !strings.HasSuffix(p, "/")
}
func Overlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
func (s Settings) Validate() error {
	for _, p := range []string{s.InstallPath, s.ConfigDirectory, s.BrokerLogPath, s.ControllerMetadataPath, s.ControllerLogPath} {
		if !Approved(p) {
			return fmt.Errorf("candidate path outside approved roots: %s", p)
		}
	}
	if !Approved(s.JavaHome) && !regexp.MustCompile(`^/usr/lib/jvm/[A-Za-z0-9_.-]+$`).MatchString(s.JavaHome) {
		return fmt.Errorf("unapproved candidate Java home")
	}
	if !regexp.MustCompile(`^[a-z_][a-z0-9_-]*[$]?$`).MatchString(s.RuntimeUser) {
		return fmt.Errorf("invalid candidate runtime user")
	}
	for _, h := range []string{s.BrokerHeapXmx, s.ControllerHeapXmx} {
		if !regexp.MustCompile(`^[1-9][0-9]*[mMgG]$`).MatchString(h) {
			return fmt.Errorf("invalid candidate heap")
		}
	}
	storage := []string{}
	for _, paths := range [][]string{s.BrokerDataPaths, s.ControllerDataPaths} {
		if len(paths) < 1 || len(paths) > 16 {
			return fmt.Errorf("explicit bounded candidate storage required")
		}
		for _, p := range paths {
			if !Approved(p) {
				return fmt.Errorf("unapproved candidate storage")
			}
			storage = append(storage, p)
		}
	}
	if s.BrokerMetadataPath != "" {
		if !Approved(s.BrokerMetadataPath) {
			return fmt.Errorf("unapproved broker metadata")
		}
		found := false
		for _, p := range s.BrokerDataPaths {
			found = found || p == s.BrokerMetadataPath
		}
		if !found {
			storage = append(storage, s.BrokerMetadataPath)
		}
	}
	found := false
	for _, p := range s.ControllerDataPaths {
		found = found || p == s.ControllerMetadataPath
	}
	if !found {
		storage = append(storage, s.ControllerMetadataPath)
	}
	for i, p := range storage {
		for _, other := range storage[i+1:] {
			if Overlap(p, other) {
				return fmt.Errorf("candidate storage overlaps")
			}
		}
		for _, other := range []string{s.InstallPath, s.ConfigDirectory, s.JavaHome, s.BrokerLogPath, s.ControllerLogPath} {
			if Overlap(p, other) {
				return fmt.Errorf("candidate storage overlaps runtime/configuration/logs")
			}
		}
	}
	for _, log := range []string{s.BrokerLogPath, s.ControllerLogPath} {
		for _, p := range []string{s.InstallPath, s.ConfigDirectory, s.JavaHome} {
			if Overlap(log, p) {
				return fmt.Errorf("candidate log overlaps runtime/configuration")
			}
		}
	}
	if Overlap(s.BrokerLogPath, s.ControllerLogPath) || Overlap(s.JavaHome, s.InstallPath) || Overlap(s.JavaHome, s.ConfigDirectory) || Overlap(s.ConfigDirectory, s.InstallPath) && !strings.HasPrefix(s.ConfigDirectory, s.InstallPath+"/") {
		return fmt.Errorf("candidate runtime paths overlap")
	}
	for _, dir := range []string{"bin", "libs"} {
		if Overlap(s.ConfigDirectory, filepath.Join(s.InstallPath, dir)) {
			return fmt.Errorf("configuration overlaps Kafka executables")
		}
	}
	return nil
}
