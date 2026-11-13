package expansion

import "fmt"

type MountCapacity struct {
	Device          uint64
	AvailableBytes  uint64
	AvailableInodes uint64
}

// ResourceBudget charges one reservation per process per actual filesystem.
// Two paths on a single mount cannot both claim the same remaining capacity.
func ResourceBudget(p Plan, availableMemory uint64, mounts map[string]MountCapacity) error {
	requiredMemory := p.ResourcePolicy.OSReserveMB * 1024 * 1024
	for _, process := range p.Processes {
		heap, err := HeapBytes(process.HeapXmx)
		if err != nil {
			return err
		}
		if heap > ^uint64(0)-requiredMemory {
			return fmt.Errorf("combined memory requirement overflow")
		}
		requiredMemory += heap
	}
	minimum := p.ResourcePolicy.MinAvailableMemoryMB * 1024 * 1024
	if minimum > requiredMemory {
		requiredMemory = minimum
	}
	if availableMemory < requiredMemory {
		return fmt.Errorf("available memory cannot cover all planned heaps and the reviewed OS reserve: available %d MiB, required %d MiB", availableMemory/(1024*1024), requiredMemory/(1024*1024))
	}
	type budget struct {
		capacity  MountCapacity
		processes map[string]bool
	}
	byDevice := map[uint64]*budget{}
	for _, process := range p.Processes {
		paths := append([]string{}, process.DataPaths...)
		if process.MetadataPath != "" {
			paths = append(paths, process.MetadataPath)
		}
		paths = append(paths, process.LogPath)
		for _, path := range paths {
			capacity, exists := mounts[path]
			if !exists {
				return fmt.Errorf("candidate filesystem observation is incomplete")
			}
			entry := byDevice[capacity.Device]
			if entry == nil {
				entry = &budget{capacity: capacity, processes: map[string]bool{}}
				byDevice[capacity.Device] = entry
			} else if capacity.AvailableBytes < entry.capacity.AvailableBytes {
				entry.capacity.AvailableBytes = capacity.AvailableBytes
			}
			if capacity.AvailableInodes < entry.capacity.AvailableInodes {
				entry.capacity.AvailableInodes = capacity.AvailableInodes
			}
			entry.processes[process.Role] = true
		}
	}
	for _, entry := range byDevice {
		count := uint64(len(entry.processes))
		if entry.capacity.AvailableBytes < p.ResourcePolicy.MinDiskGB*1024*1024*1024*count || entry.capacity.AvailableInodes < p.ResourcePolicy.MinInodes*count {
			return fmt.Errorf("shared filesystem cannot cover all process reservations: available %d MiB and %d inodes; required %d MiB and %d inodes for %d processes", entry.capacity.AvailableBytes/(1024*1024), entry.capacity.AvailableInodes, p.ResourcePolicy.MinDiskGB*1024*count, p.ResourcePolicy.MinInodes*count, count)
		}
	}
	return nil
}
