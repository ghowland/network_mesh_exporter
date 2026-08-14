// Package inventory holds host records and merges the sets produced by
// each provider into one canonical, sorted view.
package inventory

import (
	"sort"
	"sync"
	"time"
)

// HostRecord is one inventory entry. It carries no typed topology
// fields. All topology data is in Attributes, and zones are derived from
// Attributes by rule. This is what lets the Kubernetes provider work
// with any label set without a schema change.
type HostRecord struct {
	ID         string            `json:"id"`
	Address    string            `json:"address"`
	Attributes map[string]string `json:"attributes"`
	Source     string            `json:"source"`
	Healthy    bool              `json:"healthy"`
	Reason     string            `json:"reason"`
	SeenAt     time.Time         `json:"seen_at"`
	Priority   int               `json:"priority"`
}

// Attr returns an attribute value and whether it was present.
func (h HostRecord) Attr(key string) (string, bool) {
	v, ok := h.Attributes[key]
	return v, ok
}

// Clone returns a deep copy. Snapshots must not share the attribute map.
func (h HostRecord) Clone() HostRecord {
	cp := h
	cp.Attributes = make(map[string]string, len(h.Attributes))
	for k, v := range h.Attributes {
		cp.Attributes[k] = v
	}
	return cp
}

// Set is the complete host set produced by one provider.
type Set struct {
	Source    string
	Hosts     []HostRecord
	FetchAt   time.Time
	Stale     bool
	LastError string
}

// SourceStatus reports the state of one provider for the API.
type SourceStatus struct {
	Source    string    `json:"source"`
	Hosts     int       `json:"hosts"`
	FetchAt   time.Time `json:"fetch_at"`
	Stale     bool      `json:"stale"`
	LastError string    `json:"last_error"`
}

// Snapshot is an immutable merged inventory.
type Snapshot struct {
	Hosts      []HostRecord
	ByID       map[string]int
	Generation uint64
	TakenAt    time.Time
	Collisions int
	Sources    map[string]SourceStatus
}

// Get returns a host by ID.
func (s *Snapshot) Get(id string) (HostRecord, bool) {
	i, ok := s.ByID[id]
	if !ok {
		return HostRecord{}, false
	}
	return s.Hosts[i], true
}

// Len returns the host count.
func (s *Snapshot) Len() int { return len(s.Hosts) }

// Filter returns hosts that satisfy the predicate, in canonical order.
func (s *Snapshot) Filter(pred func(HostRecord) bool) []HostRecord {
	out := make([]HostRecord, 0, len(s.Hosts))
	for _, h := range s.Hosts {
		if pred(h) {
			out = append(out, h)
		}
	}
	return out
}

// Store merges provider sets into one inventory. It is safe for
// concurrent use.
type Store struct {
	mu   sync.RWMutex
	sets map[string]Set
	gen  uint64
	snap *Snapshot
}

// NewStore creates an empty Store.
func NewStore() *Store {
	return &Store{sets: make(map[string]Set)}
}

// Replace installs the complete host set for one source, atomically. A
// provider never applies a partial update, so a slow or failing source
// cannot produce a half-applied topology.
func (s *Store) Replace(set Set) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sets[set.Source] = set
	s.gen++
	s.snap = nil
}

// Remove deletes all hosts from one source, used when a provider is
// disabled by a reload.
func (s *Store) Remove(source string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sets[source]; !ok {
		return
	}
	delete(s.sets, source)
	s.gen++
	s.snap = nil
}

// Generation increases on every Replace or Remove.
func (s *Store) Generation() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gen
}

// Snapshot returns an immutable view of the merged inventory.
func (s *Store) Snapshot() *Snapshot {
	s.mu.RLock()
	if s.snap != nil {
		snap := s.snap
		s.mu.RUnlock()
		return snap
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snap != nil {
		return s.snap
	}
	hosts, collisions := merge(s.sets)
	byID := make(map[string]int, len(hosts))
	for i, h := range hosts {
		byID[h.ID] = i
	}
	sources := make(map[string]SourceStatus, len(s.sets))
	for name, set := range s.sets {
		sources[name] = SourceStatus{
			Source:    name,
			Hosts:     len(set.Hosts),
			FetchAt:   set.FetchAt,
			Stale:     set.Stale,
			LastError: set.LastError,
		}
	}
	s.snap = &Snapshot{
		Hosts:      hosts,
		ByID:       byID,
		Generation: s.gen,
		TakenAt:    time.Now(),
		Collisions: collisions,
		Sources:    sources,
	}
	return s.snap
}

// merge resolves collisions by the higher Priority value and counts
// them. The result is sorted by host ID, which is the canonical order
// used by every deterministic scan in the system.
func merge(sets map[string]Set) ([]HostRecord, int) {
	byID := make(map[string]HostRecord)
	collisions := 0
	for _, set := range sets {
		for _, h := range set.Hosts {
			prev, exists := byID[h.ID]
			if !exists {
				byID[h.ID] = h.Clone()
				continue
			}
			collisions++
			if h.Priority > prev.Priority {
				byID[h.ID] = h.Clone()
			}
		}
	}
	out := make([]HostRecord, 0, len(byID))
	for _, h := range byID {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, collisions
}

