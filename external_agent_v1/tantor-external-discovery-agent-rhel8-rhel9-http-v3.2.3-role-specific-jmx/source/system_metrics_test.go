package main

import (
	"context"
	"syscall"
	"testing"
)

func TestCPUAndMemoryMetricsComeFromValidHostCounters(t *testing.T) {
	snapshot, err := readCPUSnapshot()
	if err != nil {
		t.Fatalf("read CPU snapshot: %v", err)
	}
	if snapshot.total == 0 || snapshot.idle > snapshot.total {
		t.Fatalf("invalid CPU snapshot: idle=%d total=%d", snapshot.idle, snapshot.total)
	}

	cpu := cpuUsage(context.Background())
	if cpu < 0 || cpu > 100 {
		t.Fatalf("CPU usage out of range: %f", cpu)
	}

	usedMB, totalMB := memoryUsage()
	if totalMB <= 0 || usedMB < 0 || usedMB > totalMB {
		t.Fatalf("invalid memory usage: used=%d MB total=%d MB", usedMB, totalMB)
	}
}

func TestDiskUsageMatchesDFUsedBlocksAndKeepsExactBytes(t *testing.T) {
	const blockSize = 4096
	stat := syscall.Statfs_t{
		Blocks: 1000,
		Bfree:  400,
		Bavail: 300,
		Bsize:  blockSize,
	}

	usedGB, totalGB, usedBytes, totalBytes := diskUsageFromStat(stat)
	wantUsed := int64((1000 - 400) * blockSize)
	wantTotal := int64(1000 * blockSize)

	if usedBytes != wantUsed || totalBytes != wantTotal {
		t.Fatalf("unexpected byte values: used=%d total=%d; want used=%d total=%d", usedBytes, totalBytes, wantUsed, wantTotal)
	}
	if usedGB != 0 || totalGB != 0 {
		t.Fatalf("sub-GiB values must truncate only in legacy GB fields: used=%d total=%d", usedGB, totalGB)
	}
}

func TestDiskUsageDoesNotCountReservedBlocksAsUsed(t *testing.T) {
	stat := syscall.Statfs_t{
		Blocks: 100,
		Bfree:  40,
		Bavail: 30,
		Bsize:  1,
	}

	_, _, usedBytes, _ := diskUsageFromStat(stat)
	if usedBytes != 60 {
		t.Fatalf("used bytes=%d; want 60 from Blocks-Bfree (not 70 from Blocks-Bavail)", usedBytes)
	}
}

func TestHostDiskMetricUsesRootFilesystem(t *testing.T) {
	if hostDiskMetricPath != "/" {
		t.Fatalf("host disk metric path=%q; want root filesystem /", hostDiskMetricPath)
	}
}
