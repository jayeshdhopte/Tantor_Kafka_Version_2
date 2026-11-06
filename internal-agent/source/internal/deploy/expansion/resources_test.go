package expansion

import "testing"

func capacityFixture(p Plan, bytes, inodes uint64) map[string]MountCapacity {
	out := map[string]MountCapacity{}
	for _, process := range p.Processes {
		paths := append([]string{}, process.DataPaths...)
		if process.MetadataPath != "" {
			paths = append(paths, process.MetadataPath)
		}
		paths = append(paths, process.LogPath)
		for _, path := range paths {
			out[path] = MountCapacity{Device: 1, AvailableBytes: bytes, AvailableInodes: inodes}
		}
	}
	return out
}
func TestSharedFilesystemCannotBeDoubleCountedForTwoJVMs(t *testing.T) {
	p := fixture()
	if err := ResourceBudget(p, 8*1024*1024*1024, capacityFixture(p, 30*1024*1024*1024, 30000)); err == nil {
		t.Fatal("same 30GB counted twice")
	}
	if err := ResourceBudget(p, 8*1024*1024*1024, capacityFixture(p, 50*1024*1024*1024, 15000)); err == nil {
		t.Fatal("same inode capacity counted twice")
	}
	if err := ResourceBudget(p, 8*1024*1024*1024, capacityFixture(p, 50*1024*1024*1024, 30000)); err != nil {
		t.Fatal(err)
	}
}
func TestCombinedMemoryAccountsForBothHeapsAndOSReserve(t *testing.T) {
	p := fixture()
	p.Processes[0].HeapXmx = "3g"
	p.Processes[1].HeapXmx = "3g"
	mounts := capacityFixture(p, 100*1024*1024*1024, 100000)
	if err := ResourceBudget(p, 7*1024*1024*1024, mounts); err == nil {
		t.Fatal("second heap or OS reserve omitted")
	}
	if err := ResourceBudget(p, 8*1024*1024*1024, mounts); err != nil {
		t.Fatal(err)
	}
}
func TestDistinctMountsAreReservedIndependentlyAndMissingPathsFail(t *testing.T) {
	p := fixture()
	mounts := capacityFixture(p, 25*1024*1024*1024, 15000)
	for _, path := range append(p.Processes[1].DataPaths, p.Processes[1].LogPath) {
		capacity := mounts[path]
		capacity.Device = 2
		mounts[path] = capacity
	}
	if err := ResourceBudget(p, 8*1024*1024*1024, mounts); err != nil {
		t.Fatal(err)
	}
	delete(mounts, p.Processes[0].DataPaths[0])
	if err := ResourceBudget(p, 8*1024*1024*1024, mounts); err == nil {
		t.Fatal("missing filesystem proof accepted")
	}
}
