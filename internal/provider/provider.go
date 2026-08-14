// Package provider defines the discovery source interface and the
// manager that starts, stops, and restarts sources as the configuration
// changes.
package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/inventory"
)

// Provider produces host records from one source.
type Provider interface {
	// Name returns the source identifier used in the inventory and in
	// metric labels.
	Name() string

	// Run polls or watches the source until ctx is cancelled. It writes
	// each complete host set into the sink. It returns an error only on
	// a condition that cannot be retried.
	Run(ctx context.Context, sink Sink) error
}

// Refresher is implemented by providers that accept an out-of-band poll
// request from the API.
type Refresher interface {
	Refresh()
}

// Sink receives complete host sets from a provider.
type Sink interface {
	Replace(set inventory.Set)
}

// Reporter records provider outcomes for the metrics and the API.
type Reporter interface {
	Success(source string, hosts int, at time.Time)
	Failure(source string, err error, at time.Time)
	CacheAge(source string, age time.Duration)
}

// NopReporter discards every outcome. It is used before the metrics
// registry exists.
type NopReporter struct{}

func (NopReporter) Success(string, int, time.Time)      {}
func (NopReporter) Failure(string, error, time.Time)    {}
func (NopReporter) CacheAge(string, time.Duration)      {}

// Factory builds a provider from a configuration. The manager holds one
// factory per source name so that a new source type does not change the
// manager.
type Factory func(cfg *config.Config) (Provider, error)

type running struct {
	prov   Provider
	cancel context.CancelFunc
	done   chan struct{}
	fp     string
}

// Manager starts, stops, and restarts providers as the configuration
// changes.
type Manager struct {
	store *inventory.Store
	rep   Reporter

	mu        sync.Mutex
	active    map[string]*running
	factories map[string]Factory
	enabled   func(cfg *config.Config, name string) bool
	fingers   func(cfg *config.Config, name string) string
}

// NewManager creates a Manager that writes into store.
func NewManager(store *inventory.Store, rep Reporter) *Manager {
	if rep == nil {
		rep = NopReporter{}
	}
	return &Manager{
		store:     store,
		rep:       rep,
		active:    make(map[string]*running),
		factories: make(map[string]Factory),
	}
}

// Register installs the factory for one source name, together with the
// predicate that reports whether the source is enabled and the function
// that fingerprints its configuration.
func (m *Manager) Register(name string, f Factory,
	enabled func(*config.Config) bool, sub func(*config.Config) any) {

	m.mu.Lock()
	defer m.mu.Unlock()
	m.factories[name] = f
	prevEnabled := m.enabled
	prevFinger := m.fingers
	m.enabled = func(cfg *config.Config, n string) bool {
		if n == name {
			return enabled(cfg)
		}
		if prevEnabled != nil {
			return prevEnabled(cfg, n)
		}
		return false
	}
	m.fingers = func(cfg *config.Config, n string) string {
		if n == name {
			return fingerprint(sub(cfg))
		}
		if prevFinger != nil {
			return prevFinger(cfg, n)
		}
		return ""
	}
}

// Apply reconciles the running provider set against the configuration. A
// provider whose configuration changed is stopped and restarted. A
// provider that is disabled has its hosts removed from the inventory.
func (m *Manager) Apply(ctx context.Context, cfg *config.Config) error {
	m.mu.Lock()
	names := make([]string, 0, len(m.factories))
	for n := range m.factories {
		names = append(names, n)
	}
	m.mu.Unlock()

	var firstErr error
	for _, name := range names {
		m.mu.Lock()
		want := m.enabled(cfg, name)
		fp := m.fingers(cfg, name)
		cur := m.active[name]
		m.mu.Unlock()

		switch {
		case !want && cur != nil:
			m.stop(name)
			m.store.Remove(name)
			slog.Info("provider disabled", "source", name)

		case want && cur != nil && cur.fp != fp:
			m.stop(name)
			if err := m.start(ctx, cfg, name, fp); err != nil && firstErr == nil {
				firstErr = err
			}
			slog.Info("provider restarted after configuration change", "source", name)

		case want && cur == nil:
			if err := m.start(ctx, cfg, name, fp); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (m *Manager) start(ctx context.Context, cfg *config.Config, name, fp string) error {
	m.mu.Lock()
	f := m.factories[name]
	m.mu.Unlock()
	if f == nil {
		return fmt.Errorf("provider: no factory for %s", name)
	}
	p, err := f(cfg)
	if err != nil {
		m.rep.Failure(name, err, time.Now())
		return fmt.Errorf("provider %s: %w", name, err)
	}
	pctx, cancel := context.WithCancel(ctx)
	r := &running{prov: p, cancel: cancel, done: make(chan struct{}), fp: fp}

	m.mu.Lock()
	m.active[name] = r
	m.mu.Unlock()

	go func() {
		defer close(r.done)
		if err := p.Run(pctx, sinkFunc(func(set inventory.Set) {
			set.Source = name
			m.store.Replace(set)
			if set.LastError != "" {
				m.rep.Failure(name, fmt.Errorf("%s", set.LastError), set.FetchAt)
			} else {
				m.rep.Success(name, len(set.Hosts), set.FetchAt)
			}
		})); err != nil && pctx.Err() == nil {
			slog.Error("provider stopped", "source", name, "error", err)
			m.rep.Failure(name, err, time.Now())
		}
	}()
	slog.Info("provider started", "source", name)
	return nil
}

func (m *Manager) stop(name string) {
	m.mu.Lock()
	r := m.active[name]
	delete(m.active, name)
	m.mu.Unlock()
	if r == nil {
		return
	}
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(10 * time.Second):
		slog.Warn("provider did not stop in time", "source", name)
	}
}

// Refresh forces an immediate poll of one source, or of all sources when
// source is empty.
func (m *Manager) Refresh(source string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	found := false
	for name, r := range m.active {
		if source != "" && name != source {
			continue
		}
		found = true
		if rf, ok := r.prov.(Refresher); ok {
			rf.Refresh()
		}
	}
	if !found {
		return fmt.Errorf("provider: no running source named %q", source)
	}
	return nil
}

// Status returns the current state of every provider.
func (m *Manager) Status() []inventory.SourceStatus {
	snap := m.store.Snapshot()
	out := make([]inventory.SourceStatus, 0, len(snap.Sources))
	for _, s := range snap.Sources {
		out = append(out, s)
	}
	return out
}

// Stop stops every provider and waits for exit.
func (m *Manager) Stop() {
	m.mu.Lock()
	names := make([]string, 0, len(m.active))
	for n := range m.active {
		names = append(names, n)
	}
	m.mu.Unlock()
	for _, n := range names {
		m.stop(n)
	}
}

// Backoff computes an exponential delay with jitter, bounded by max.
func Backoff(attempt int, min, max time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := min
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	jitter := time.Duration(rand.Int63n(int64(d/4) + 1))
	return d - d/8 + jitter
}

type sinkFunc func(inventory.Set)

func (f sinkFunc) Replace(set inventory.Set) { f(set) }

func fingerprint(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

