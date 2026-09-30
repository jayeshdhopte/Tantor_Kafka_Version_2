package main

import "sync"

type ClusterStore struct {
	mu       sync.RWMutex
	clusters []DiscoveredCluster
}

func (s *ClusterStore) Set(clusters []DiscoveredCluster) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = append([]DiscoveredCluster(nil), clusters...)
}

func (s *ClusterStore) Get() []DiscoveredCluster {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]DiscoveredCluster(nil), s.clusters...)
}
