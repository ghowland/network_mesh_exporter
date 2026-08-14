// Package health tracks host eligibility and applies the hysteresis that
// stops a short failure from rewriting slot assignments.
package health

import (
	"sort"
	"sync"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/inventory"
)

// State is the eligibility state of one host.
type State int

const (
	StateUnknown State = iota
	StateHealthy
	StateSuspect    // failing, but not yet past unhealthy_after
	StatePending    // marked unhealthy, inside release_hold
	StateUnhealthy  // release_hold expired, slot sides may be cleared
	StateCooldown   // held ineligible after flapping
	StateIneligible // the provider marked it ineligible, immediate
)

// String returns the metric label form of a state.
func (s State) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateSuspect:
		return "suspect"
	case StatePending:
		return "pending"
	case StateUnhealthy:
		return "unhealthy"
	case StateCooldown:
		return "cooldown"
	case StateIneligible:
		return "ineligible"
	default:
		return "unknown"
	}
}

// MarshalJSON writes the label form rather than the integer.
func (s State) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// Status is the full health record of one host.
type Status struct {
	HostID      string    `json:"host_id"`
	State       State     `json:"state"`
	Reason      string    `json:"reason"`
	Source      string    `json:"source"`
	FirstSeen   time.Time `json:"first_seen"`
	LastSeen    time.Time `json:"last_seen"`
	LastSuccess time.Time `json:"last_success"`
	LastFailure time.Time `json:"last_failure"`
	FailStreak  int       `json:"fail_streak"`
	OKStreak    int       `json:"ok_streak"`
	MarkedAt    time.Time `json:"marked_at"`
	ReleaseAt   time.Time `json:"release_at"`
	CooldownEnd time.Time `json:"cooldown_end"`
	Transitions int       `json:"transitions"`

	// internal, not exported to JSON
	present     bool
	missingAt   time.Time
	resolveFail time.Time
	flapMarks   []time.Time
	assignedAt  time.Time
}

// Eligible reports whether the host may hold a slot side. A host in
// StatePending is still eligible, because release_hold has not expired.
// This is the mechanism that stops a short network event from rewriting
// slot assignments.
func (s Status) Eligible() bool {
	switch s.State {
	case StateHealthy, StateSuspect, StatePending:
		return true
	case StateUnknown:
		// A host seen but never probed is eligible during the initial
		// grace, otherwise it could never be assigned in the first place.
		return true
	default:
		return false
	}
}

// Tracker holds the health state of every host and applies hysteresis.
// It is safe for concurrent use.
type Tracker struct {
	mu    sync.RWMutex
	cfg   config.HealthConfig
	hosts map[string]*Status
	clock func() time.Time
}

// New creates a Tracker. A nil clock uses time.Now.
func New(cfg config.HealthConfig, clock func() time.Time) *Tracker {
	if clock == nil {
		clock = time.Now
	}
	return &Tracker{cfg: cfg, hosts: make(map[string]*Status), clock: clock}
}

// SetConfig applies a reloaded configuration. Existing release timers are
// recomputed against the new hold value, so a shortened hold takes effect
// immediately rather than at the next failure.
func (t *Tracker) SetConfig(cfg config.HealthConfig) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cfg = cfg
	for _, st := range t.hosts {
		if st.State == StatePending && !st.MarkedAt.IsZero() {
			st.ReleaseAt = st.MarkedAt.Add(cfg.ReleaseHold.D())
		}
	}
}

// Observe applies a provider snapshot. Provider ineligibility is
// authoritative and immediate; it does not pass through hysteresis. A
// host absent from the snapshot becomes ineligible after missing_grace.
func (t *Tracker) Observe(snap *inventory.Snapshot) {
	now := t.clock()
	t.mu.Lock()
	defer t.mu.Unlock()

	seen := make(map[string]bool, snap.Len())
	for _, h := range snap.Hosts {
		seen[h.ID] = true
		st := t.hosts[h.ID]
		if st == nil {
			st = &Status{HostID: h.ID, FirstSeen: now, State: StateUnknown}
			t.hosts[h.ID] = st
		}
		st.Source = h.Source
		st.LastSeen = now
		st.present = true
		st.missingAt = time.Time{}

		if !h.Healthy {
			t.transition(st, StateIneligible, h.Reason, now)
			continue
		}
		// The provider reports the host as usable. A host that was
		// ineligible for a provider reason returns to the unknown state
		// and then earns its way back through the probe streak.
		if st.State == StateIneligible {
			t.transition(st, StateUnknown, "", now)
		}
	}

	for id, st := range t.hosts {
		if seen[id] {
			continue
		}
		if st.present {
			st.present = false
			st.missingAt = now
		}
		if !st.missingAt.IsZero() && now.Sub(st.missingAt) >= t.cfg.MissingGrace.D() {
			t.transition(st, StateIneligible, "absent from provider", now)
		}
	}
}

// Forget removes a host that no longer exists anywhere, so that the
// tracker does not grow without bound across long-lived processes.
func (t *Tracker) Forget(hostID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.hosts, hostID)
}

// RecordProbe applies one probe cycle outcome for one host.
// Probe-derived unhealth passes through unhealthy_after and then
// release_hold, so a brief failure never clears a slot side.
func (t *Tracker) RecordProbe(hostID string, ok bool) {
	now := t.clock()
	t.mu.Lock()
	defer t.mu.Unlock()

	st := t.hosts[hostID]
	if st == nil {
		st = &Status{HostID: hostID, FirstSeen: now, State: StateUnknown}
		t.hosts[hostID] = st
	}
	if ok {
		st.LastSuccess = now
		st.OKStreak++
		st.FailStreak = 0
		if st.State == StateIneligible || st.State == StateCooldown {
			return // a provider decision or a cooldown is not cleared by a probe
		}
		if st.OKStreak >= t.cfg.HealthyAfter {
			t.transition(st, StateHealthy, "", now)
		}
		return
	}

	st.LastFailure = now
	st.FailStreak++
	st.OKStreak = 0
	if st.State == StateIneligible || st.State == StateCooldown {
		return
	}
	if st.FailStreak < t.cfg.UnhealthyAfter {
		t.transition(st, StateSuspect, "probe failing", now)
		return
	}
	if st.State != StatePending && st.State != StateUnhealthy {
		t.transition(st, StatePending, "probe failed repeatedly", now)
		st.MarkedAt = now
		st.ReleaseAt = now.Add(t.cfg.ReleaseHold.D())
	}
}

// RecordResolve records a DNS resolution outcome, which is subject to
// dns_grace rather than to unhealthy_after.
func (t *Tracker) RecordResolve(hostID string, ok bool) {
	now := t.clock()
	t.mu.Lock()
	defer t.mu.Unlock()

	st := t.hosts[hostID]
	if st == nil {
		return
	}
	if ok {
		st.resolveFail = time.Time{}
		return
	}
	if st.resolveFail.IsZero() {
		st.resolveFail = now
		return
	}
	if now.Sub(st.resolveFail) >= t.cfg.DNSGrace.D() && st.Eligible() {
		t.transition(st, StateUnhealthy, "address does not resolve", now)
	}
}

// MarkAssigned records the moment a host took a slot side. The initial
// grace is measured from this moment, so a newly assigned host is not
// released before it has had a chance to be probed.
func (t *Tracker) MarkAssigned(hostID string) {
	now := t.clock()
	t.mu.Lock()
	defer t.mu.Unlock()
	if st := t.hosts[hostID]; st != nil && st.assignedAt.IsZero() {
		st.assignedAt = now
	}
}

// Eligible reports whether a host may hold a slot side now.
func (t *Tracker) Eligible(hostID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	st := t.hosts[hostID]
	if st == nil {
		return false
	}
	return st.Eligible()
}

// EligibleFunc returns a predicate bound to a copy of the current state,
// so that one reconcile sees one consistent view without holding a lock
// for its duration.
func (t *Tracker) EligibleFunc() func(string) bool {
	t.mu.RLock()
	elig := make(map[string]bool, len(t.hosts))
	for id, st := range t.hosts {
		elig[id] = st.Eligible()
	}
	t.mu.RUnlock()
	return func(id string) bool { return elig[id] }
}

// Status returns the record for one host.
func (t *Tracker) Status(hostID string) (Status, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	st := t.hosts[hostID]
	if st == nil {
		return Status{}, false
	}
	return *st, true
}

// All returns every record, sorted by host ID.
func (t *Tracker) All() []Status {
	t.mu.RLock()
	out := make([]Status, 0, len(t.hosts))
	for _, st := range t.hosts {
		out = append(out, *st)
	}
	t.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].HostID < out[j].HostID })
	return out
}

// Counts returns the host count by state, for the metrics.
func (t *Tracker) Counts() map[State]int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[State]int, 7)
	for _, st := range t.hosts {
		out[st.State]++
	}
	return out
}

// NextDeadline returns the earliest time at which a timer expires, so
// that the reconciler can wake exactly then instead of polling.
func (t *Tracker) NextDeadline() (time.Time, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var next time.Time
	consider := func(v time.Time) {
		if v.IsZero() {
			return
		}
		if next.IsZero() || v.Before(next) {
			next = v
		}
	}
	for _, st := range t.hosts {
		if st.State == StatePending {
			consider(st.ReleaseAt)
		}
		if st.State == StateCooldown {
			consider(st.CooldownEnd)
		}
		if !st.missingAt.IsZero() && st.State != StateIneligible {
			consider(st.missingAt.Add(t.cfg.MissingGrace.D()))
		}
	}
	return next, !next.IsZero()
}

// Tick advances timers and returns the hosts whose eligibility changed.
// The reconciler uses the returned list as a trigger reason.
func (t *Tracker) Tick(now time.Time) []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	var changed []string
	for id, st := range t.hosts {
		before := st.Eligible()

		switch st.State {
		case StatePending:
			if !st.ReleaseAt.IsZero() && !now.Before(st.ReleaseAt) {
				t.transition(st, StateUnhealthy, st.Reason, now)
			}
		case StateCooldown:
			if !st.CooldownEnd.IsZero() && !now.Before(st.CooldownEnd) {
				t.transition(st, StateUnknown, "", now)
				st.OKStreak = 0
				st.FailStreak = 0
			}
		}
		if !st.missingAt.IsZero() && st.State != StateIneligible &&
			now.Sub(st.missingAt) >= t.cfg.MissingGrace.D() {
			t.transition(st, StateIneligible, "absent from provider", now)
		}

		if st.Eligible() != before {
			changed = append(changed, id)
		}
	}
	sort.Strings(changed)
	return changed
}

// transition changes the state of one host and applies flap detection.
// The caller holds the lock.
func (t *Tracker) transition(st *Status, to State, reason string, now time.Time) {
	if st.State == to {
		if reason != "" {
			st.Reason = reason
		}
		return
	}

	// A change of eligibility counts toward the flap threshold. A host
	// that crosses the threshold is held ineligible for the cooldown, so
	// that an unstable host does not rewrite slot sides repeatedly.
	wasEligible := st.Eligible()
	st.State = to
	st.Reason = reason
	if to == StateHealthy {
		st.Reason = ""
		st.MarkedAt = time.Time{}
		st.ReleaseAt = time.Time{}
	}
	nowEligible := st.Eligible()

	if wasEligible != nowEligible {
		st.flapMarks = append(st.flapMarks, now)
		window := t.cfg.FlapWindow.D()
		keep := st.flapMarks[:0]
		for _, m := range st.flapMarks {
			if now.Sub(m) <= window {
				keep = append(keep, m)
			}
		}
		st.flapMarks = keep
		st.Transitions = len(keep)

		if t.cfg.FlapThreshold > 0 && len(keep) > t.cfg.FlapThreshold &&
			to != StateIneligible && to != StateCooldown {
			st.State = StateCooldown
			st.Reason = "flapping"
			st.CooldownEnd = now.Add(t.cfg.FlapCooldown.D())
		}
	}
}

