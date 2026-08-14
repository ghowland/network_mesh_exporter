package config

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Watcher reloads the configuration on SIGHUP and on file modification.
type Watcher struct {
	path string

	mu    sync.RWMutex
	cur   *Config
	mtime time.Time
	size  int64

	subsMu sync.Mutex
	subs   []chan *Config
}

// NewWatcher creates a Watcher for the given path with an initial value.
func NewWatcher(path string, initial *Config) *Watcher {
	w := &Watcher{path: path, cur: initial}
	if fi, err := os.Stat(path); err == nil {
		w.mtime, w.size = fi.ModTime(), fi.Size()
	}
	return w
}

// Current returns the active configuration. It is safe for concurrent use.
func (w *Watcher) Current() *Config {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cur
}

// Subscribe returns a channel that receives each accepted configuration.
// The channel has capacity 1 and drops intermediate values.
func (w *Watcher) Subscribe() <-chan *Config {
	ch := make(chan *Config, 1)
	w.subsMu.Lock()
	w.subs = append(w.subs, ch)
	w.subsMu.Unlock()
	return ch
}

// Run blocks until ctx is cancelled. A reload that fails validation is
// rejected and the previous value stays active.
func (w *Watcher) Run(ctx context.Context) error {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	defer signal.Stop(sig)

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sig:
			w.reload(true)
		case <-tick.C:
			w.reload(false)
		}
	}
}

// reload re-reads the file when it changed, or unconditionally when
// force is set.
func (w *Watcher) reload(force bool) {
	fi, err := os.Stat(w.path)
	if err != nil {
		if force {
			slog.Error("config reload failed", "path", w.path, "error", err)
		}
		return
	}
	w.mu.RLock()
	same := fi.ModTime().Equal(w.mtime) && fi.Size() == w.size
	w.mu.RUnlock()
	if same && !force {
		return
	}

	cfg, err := Load(w.path)
	if err != nil {
		slog.Error("config rejected, keeping previous", "path", w.path, "error", err)
		return
	}

	w.mu.Lock()
	prev := w.cur
	w.mtime, w.size = fi.ModTime(), fi.Size()
	if prev.Equal(cfg) {
		w.mu.Unlock()
		return
	}
	w.cur = cfg
	w.mu.Unlock()

	if cfg.TopologyChanged(prev) {
		slog.Warn("configuration changed the topology definition; the slot table will be rebuilt")
	} else {
		slog.Info("configuration reloaded")
	}
	w.publish(cfg)
}

func (w *Watcher) publish(cfg *Config) {
	w.subsMu.Lock()
	defer w.subsMu.Unlock()
	for _, ch := range w.subs {
		select {
		case ch <- cfg:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- cfg:
			default:
			}
		}
	}
}

