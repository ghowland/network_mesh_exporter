package reconcile

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/health"
	"github.com/example/mesh/internal/inventory"
	"github.com/example/mesh/internal/pairing"
	"github.com/example/mesh/internal/slot"
	"github.com/example/mesh/internal/state"
	"github.com/example/mesh/internal/zone"
)

// Trigger identifies why a reconcile ran. It becomes a metric label.
type Trigger string

const (
	TriggerProvider Trigger = "provider"
	TriggerConfig   Trigger = "config"
	TriggerHealth   Trigger = "health"
	TriggerTimer    Trigger = "timer"
	TriggerTick     Trigger = "tick"
	TriggerAPI      Trigger = "api"
	TriggerStart    Trigger = "start"
)

// Observer receives reconcile outcomes for the metrics.
type Observer interface {
	ReconcileDone(trigger Trigger, st Stats, d Delta, err error)
}

// NopObserver discards every outcome.
type NopObserver struct{}

func (NopObserver) ReconcileDone(Trigger, Stats, Delta, error) {}

// Deps holds the live components the loop reads on each run.
type Deps struct {
	Store   *inventory.Store
	Config  func() *config.Config
	Health  *health.Tracker
	State   *state.Store
	Metrics Observer

	// Rule, Filter, and Scanner are read on each run so that a
	// configuration reload takes effect without restarting the loop.
	Rule    func() *zone.Rule
	Filter  func() *pairing.Filter
	Scanner func() *slot.Scanner

	// OnDelta is called after each run that produced a change. It is
	// where the runner receives its new task set.
	OnDelta func(Delta, *state.State, *inventory.Snapshot)
}

// Loop serialises reconcile runs and coalesces triggers. Triggers that
// arrive during a run cause exactly one further run afterwards, not one
// run per trigger.
type Loop struct {
	deps Deps
	ch   chan Trigger

	mu       sync.RWMutex
	lastStat Stats
	lastDelta Delta
	lastAt   time.Time
	lastErr  error
	lastGen  uint64
	ready    bool
}

// NewLoop creates a Loop.
func NewLoop(d Deps) *Loop {
	if d.Metrics == nil {
		d.Metrics = NopObserver{}
	}
	return &Loop{deps: d, ch: make(chan Trigger, 1)}
}

// Trigger requests a run. It never blocks; a pending request absorbs the
// new one, which is the coalescing mechanism.
func (l *Loop) Trigger(t Trigger) {
	select {
	case l.ch <- t:
	default:
	}
}

// Ready reports whether at least one reconcile has completed.
func (l *Loop) Ready() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.ready
}

// Last returns the outcome of the most recent run.
func (l *Loop) Last() (Stats, Delta, time.Time) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastStat, l.lastDelta, l.lastAt
}

// LastError returns the error of the most recent run, if any.
func (l *Loop) LastError() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastErr
}

// Run executes reconciles until ctx is cancelled. It wakes on a trigger,
// on the periodic interval, and on the earliest health timer deadline,
// so a release hold expires on time without polling.
func (l *Loop) Run(ctx context.Context) error {
	cfg := l.deps.Config()
	interval := cfg.Reconcile.Interval.D()
	if interval <= 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.NewTimer(time.Hour)
	if !deadline.Stop() {
		<-deadline.C
	}
	armDeadline := func() {
		if !deadline.Stop() {
			select {
			case <-deadline.C:
			default:
			}
		}
		if at, ok := l.deps.Health.NextDeadline(); ok {
			d := time.Until(at)
			if d < 50*time.Millisecond {
				d = 50 * time.Millisecond
			}
			deadline.Reset(d)
		}
	}

	l.runOnce(TriggerStart)
	armDeadline()

	for {
		select {
		case <-ctx.Done():
			return nil

		case t := <-l.ch:
			l.runOnce(t)
			armDeadline()

		case <-ticker.C:
			// The periodic run also advances the health timers, so a
			// state change that no trigger reported is still applied.
			if changed := l.deps.Health.Tick(time.Now()); len(changed) > 0 {
				l.runOnce(TriggerHealth)
			} else if l.inventoryChanged() {
				l.runOnce(TriggerProvider)
			} else {
				l.runOnce(TriggerTick)
			}
			armDeadline()

		case <-deadline.C:
			if changed := l.deps.Health.Tick(time.Now()); len(changed) > 0 {
				slog.Debug("health timers expired", "hosts", len(changed))
				l.runOnce(TriggerTimer)
			}
			armDeadline()
		}
	}
}

// RunNow performs one synchronous reconcile and returns the outcome,
// used by the API endpoint.
func (l *Loop) RunNow(t Trigger) (Delta, Stats, error) {
	return l.runOnce(t)
}

// inventoryChanged reports whether the inventory generation advanced
// since the last run.
func (l *Loop) inventoryChanged() bool {
	l.mu.RLock()
	last := l.lastGen
	l.mu.RUnlock()
	return l.deps.Store.Generation() != last
}

// runOnce performs one reconcile and applies its result.
func (l *Loop) runOnce(t Trigger) (Delta, Stats, error) {
	cfg := l.deps.Config()
	snap := l.deps.Store.Snapshot()

	// The health tracker sees the snapshot first, so that provider
	// ineligibility is applied before the eligibility predicate is taken.
	l.deps.Health.Observe(snap)
	l.deps.Health.Tick(time.Now())

	in := Input{
		Snapshot: snap,
		Config:   cfg,
		Rule:     l.deps.Rule(),
		Filter:   l.deps.Filter(),
		Scanner:  l.deps.Scanner(),
		Eligible: l.deps.Health.EligibleFunc(),
		Current:  l.deps.State.Current(),
		Now:      time.Now(),
	}

	out, err := Reconcile(in)
	if err != nil {
		if errors.Is(err, pairing.ErrTooManyPairings) {
			slog.Error("reconcile aborted, keeping previous assignment",
				"trigger", t, "error", err)
		} else {
			slog.Error("reconcile failed", "trigger", t, "error", err)
		}
		l.mu.Lock()
		l.lastErr = err
		l.lastAt = time.Now()
		l.mu.Unlock()
		l.deps.Metrics.ReconcileDone(t, out.Stats, Delta{}, err)
		return Delta{}, out.Stats, err
	}

	if !out.Delta.Empty() {
		l.deps.State.Update(out.State)
		for _, c := range out.Delta.SidesChanged {
			if c.New != "" {
				l.deps.Health.MarkAssigned(c.New)
			}
		}
		if l.deps.OnDelta != nil {
			l.deps.OnDelta(out.Delta, out.State, snap)
		}
		slog.Info("assignment changed",
			"trigger", t,
			"pairings_added", len(out.Delta.PairingsAdded),
			"pairings_removed", len(out.Delta.PairingsRemoved),
			"sides_changed", len(out.Delta.SidesChanged),
			"slots_filled", out.Stats.SlotsFilled,
			"slots_unfilled", out.Stats.SlotsUnfilled)
	} else if l.deps.OnDelta != nil && !l.Ready() {
		// The first run publishes the task set even when nothing
		// changed, so a restart from a loaded state file starts probing.
		l.deps.OnDelta(out.Delta, out.State, snap)
	}

	l.mu.Lock()
	l.lastStat = out.Stats
	l.lastDelta = out.Delta
	l.lastAt = time.Now()
	l.lastErr = nil
	l.lastGen = snap.Generation
	l.ready = true
	l.mu.Unlock()

	l.deps.Metrics.ReconcileDone(t, out.Stats, out.Delta, nil)
	return out.Delta, out.Stats, nil
}

