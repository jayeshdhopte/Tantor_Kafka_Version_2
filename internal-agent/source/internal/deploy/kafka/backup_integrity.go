package kafka

import (
	"context"
	"io.translab/tantor-agent/pkg/api"
	bi "io.translab/tantor-agent/pkg/backupintegrity"
	"os"
)

func (d *Deployer) backupConfigVerified(ctx context.Context, task *api.Task, source string) (string, string, error) {
	original, err := os.ReadFile(source)
	if err != nil {
		return "", "", err
	}
	path, err := d.backupConfig(ctx, task, source)
	if err != nil {
		return "", "", err
	}
	receipt, err := bi.VerifyBackup(path, original)
	return path, receipt, err
}
