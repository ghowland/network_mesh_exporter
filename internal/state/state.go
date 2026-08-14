// Package state holds the persisted slot assignment. The authoritative
// copy is in memory; the JSON file is a durable copy written on a
// debounce, so that continuous churn does not produce continuous writes.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/example/mesh/internal/slot"
)

// Version is the state file schema version. A mismatch is treated as a
// corrupt file: the state is discarded and assignment restarts.
const Version = 1

// Slot is one measurement container. The slot index is stable; the hosts
// inside it are replaceable. Clearing one side never clears the other.
type Slot struct {
	Index     int       `json:"index"`
	Class     string    `json:"class"`
	HostA     string    `json:"host_a"`
	HostB     string    `json:"host_b"`
	RankA     int       `json:"rank_a"`
	RankB     int       `json:"rank_b"`
	AssignedA time.Time `json:"assigned_a"`
	AssignedB time.Time `json:"assigned_b"`

	// SuperTarget names the side B host this super slot was created for.
	// It is empty for anchor and diverse slots.
	SuperTarget string `json:"super_target,omitempty"`
}

// Filled reports whether both sides hold a host.
func (s Slot) Filled() bool { return s.HostA != "" && s.HostB != "" }

// Host returns the host on one side.
func (s Slot) Host(side slot.Side) string {
	if side == slot.SideB {
		return s.HostB
	}
	return s.HostA
}

// SetHost assigns one side without touching the other.
func (s *Slot) SetHost(side slot.Side, id string, rank slot.Rank, now time.Time) {
	if side == slot.SideB {
		s.HostB, s.RankB, s.AssignedB = id, int(rank), now
		return
	}
	s.HostA, s.RankA, s.AssignedA = id, int(rank), now
}

// Rank returns the fill rank of one side.
func (s Slot) Rank(side slot.Side) slot.Rank {
	if side == slot.SideB {
		return slot.Rank(s.RankB)
	}
	return slot.Rank(s.RankA)
}

// Pairing is the slot table of one zone pairing.
type Pairing struct {
	Key      string    `json:"key"`
	ZoneA    string    `json:"zone_a"`
	ZoneB    string    `json:"zone_b"`
	Intra    bool      `json:"intra"`
	Slots    []Slot    `json:"slots"`
	RemoveAt time.Time `json:"remove_at,omitempty"`
}

// Clone returns a deep copy of one pairing.
func (p *Pairing) Clone() *Pairing {
	cp := *p
	cp.Slots = make([]Slot, len(p.Slots))
	copy(cp.Slots, p.Slots)
	return &cp
}

// State is the persisted assignment. It holds no measurements.
type State struct {
	Version     int                 `json:"version"`
	ZoneRule    string              `json:"zone_rule"`
	SlotsConfig string              `json:"slots_config"`
	UpdatedAt   time.Time           `json:"updated_at"`
	Pairings    map[string]*Pairing `json:"pairings"`
	SuperHosts  map[string][]string `json:"super_hosts"`
}

// New returns an empty State stamped with the current fingerprints.
func New(zoneFP, slotsFP string) *State {
	return &State{
		Version:     Version,
		ZoneRule:    zoneFP,
		SlotsConfig: slotsFP,
		Pairings:    make(map[string]*Pairing),
		SuperHosts:  make(map[string][]string),
	}
}

// Clone returns a deep copy, used to keep the reconcile pure.
func (s *State) Clone() *State {
	if s == nil {
		return nil
	}
	cp := &State{
		Version:     s.Version,
		ZoneRule:    s.ZoneRule,
		SlotsConfig: s.SlotsConfig,
		UpdatedAt:   s.UpdatedAt,
		Pairings:    make(map[string]*Pairing, len(s.Pairings)),
		SuperHosts:  make(map[string][]string, len(s.SuperHosts)),
	}
	for k, p := range s.Pairings {
		cp.Pairings[k] = p.Clone()
	}
	for k, v := range s.SuperHosts {
		hosts := make([]string, len(v))
		copy(hosts, v)
		cp.SuperHosts[k] = hosts
	}
	return cp
}

// Counts returns the slot totals, used by the reconcile statistics.
func (s *State) Counts() (total, filled int) {
	for _, p := range s.Pairings {
		if !p.RemoveAt.IsZero() {
			continue
		}
		for _, sl := range p.Slots {
			total++
			if sl.Filled() {
				filled++
			}
		}
	}
	return total, filled
}

// Load reads the state file. A missing file returns an empty state with
// ok false and no error, so a first start is not an error condition. A
// corrupt file or a version mismatch returns an empty state and a
// non-nil error, which the caller logs as a topology reset.
func Load(path string) (*State, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return New("", ""), false, nil
	}
	if err != nil {
		return New("", ""), false, fmt.Errorf("state: read %s: %w", path, err)
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return New("", ""), false, fmt.Errorf("state: parse %s: %w", path, err)
	}
	if st.Version != Version {
		return New("", ""), false, fmt.Errorf(
			"state: version %d is not the supported version %d", st.Version, Version)
	}
	if st.Pairings == nil {
		st.Pairings = make(map[string]*Pairing)
	}
	if st.SuperHosts == nil {
		st.SuperHosts = make(map[string][]string)
	}
	return &st, true, nil
}

// Save writes the state atomically: a temporary file in the same
// directory, fsync, then rename.
func Save(path string, st *State) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	st.Version = Version
	st.UpdatedAt = time.Now()
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

