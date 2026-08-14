package state

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/mesh/internal/config"
)

// Stats reports write activity for the metrics and the API.
type Stats struct {
	Writes     uint64    `json:"writes"`
	Failures   uint64    `json:"failures"`
	LastWrite  time.Time `json:"last_write"`
	Dirty      bool      `json:"dirty"`
	PendingFor string    `json:"pending_for"`
}

// Store holds the authoritative in-memory state and schedules writes.
// The API reads from here, not from the file, so a read inside the
// debounce window is still current.
type Store struct {
	cfg config.PersistConfig

	mu         sync.RWMutex
	cur        *State
	dirty      bool
	firstDirty time.Time

	kick chan struct{}

	writes   atomic.Uint64
	failures atomic.Uint64
	last     atomic.Int64
}

// NewStore creates a Store with an initial state.
func NewStore(cfg config.PersistConfig, initial *State) *Store {
	if initial == nil {
		initial = New("", "")
	}
	return &Store{cfg: cfg, cur: initial, kick: make(chan struct{}, 1)}
}

// Current returns a clone of the state for readers, so that a caller
// cannot mutate the authoritative copy.
func (s *Store) Current() *State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cur.Clone()
}

// Update replaces the state and marks it dirty. The debounce timer
// resets on every call, and MaxDelay caps the total postponement so that
// continuous churn cannot prevent a write.
func (s *Store) Update(st *State) {
	s.mu.Lock()
	s.cur = st
	if !s.dirty {
		s.dirty = true
		s.firstDirty = time.Now()
	}
	s.mu.Unlock()

	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Run performs the debounced writes until ctx is cancelled. On cancel it
// writes immediately when dirty.
func (s *Store) Run(ctx context.Context) error {
	debounce := s.cfg.Debounce.D()
	if debounce <= 0 {
		debounce = 60 * time.Second
	}
	maxDelay := s.cfg.MaxDelay.D()
	if maxDelay < debounce {
		maxDelay = 5 * debounce
	}

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	armed := false

	for {
		select {
		case <-ctx.Done():
			if _, err := s.Flush(); err != nil {
				slog.Error("final state write failed", "path", s.cfg.Path, "error", err)
			}
			return nil

		case <-s.kick:
			s.mu.RLock()
			first := s.firstDirty
			s.mu.RUnlock()

			// The cap wins over the reset, so a stream of changes still
			// reaches disk at a bounded interval.
			if armed && !first.IsZero() && time.Since(first) >= maxDelay {
				continue
			}
			if armed && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			wait := debounce
			if !first.IsZero() {
				if remain := maxDelay - time.Since(first); remain < wait {
					wait = remain
				}
			}
			if wait < 0 {
				wait = 0
			}
			timer.Reset(wait)
			armed = true

		case <-timer.C:
			armed = false
			if _, err := s.Flush(); err != nil {
				slog.Error("state write failed", "path", s.cfg.Path, "error", err)
				timer.Reset(debounce)
				armed = true
			}
		}
	}
}

// Flush writes now, if dirty, and reports whether a write occurred.
func (s *Store) Flush() (bool, error) {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return false, nil
	}
	snapshot := s.cur.Clone()
	s.dirty = false
	s.firstDirty = time.Time{}
	s.mu.Unlock()

	if err := Save(s.cfg.Path, snapshot); err != nil {
		s.failures.Add(1)
		s.mu.Lock()
		s.dirty = true
		if s.firstDirty.IsZero() {
			s.firstDirty = time.Now()
		}
		s.mu.Unlock()
		return false, err
	}
	s.writes.Add(1)
	s.last.Store(time.Now().UnixNano())
	return true, nil
}

// Stats reports write activity.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	dirty := s.dirty
	first := s.firstDirty
	s.mu.RUnlock()

	st := Stats{
		Writes:   s.writes.Load(),
		Failures: s.failures.Load(),
		Dirty:    dirty,
	}
	if ns := s.last.Load(); ns > 0 {
		st.LastWrite = time.Unix(0, ns)
	}
	if dirty && !first.IsZero() {
		st.PendingFor = time.Since(first).Truncate(time.Second).String()
	}
	return st
}

