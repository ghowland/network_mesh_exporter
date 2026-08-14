// Command meshd runs the mesh network test system on one node. It
// discovers hosts, derives zones, assigns slots, probes its own
// direction of each slot it participates in, and exports the results.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/example/mesh/internal/api"
	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/health"
	"github.com/example/mesh/internal/inventory"
	"github.com/example/mesh/internal/metrics"
	"github.com/example/mesh/internal/pairing"
	"github.com/example/mesh/internal/probe"
	icmpsrc "github.com/example/mesh/internal/probe/icmp"
	tcpsrc "github.com/example/mesh/internal/probe/tcp"
	udpsrc "github.com/example/mesh/internal/probe/udp"
	"github.com/example/mesh/internal/provider"
	"github.com/example/mesh/internal/reconcile"
	"github.com/example/mesh/internal/responder"
	"github.com/example/mesh/internal/runner"
	"github.com/example/mesh/internal/slot"
	"github.com/example/mesh/internal/state"
	"github.com/example/mesh/internal/zone"
)

// Version is set at build time.
var Version = "dev"

// app holds every component and owns the shutdown order.
type app struct {
	cfgWatcher *config.Watcher
	store      *inventory.Store
	provs      *provider.Manager
	health     *health.Tracker
	state      *state.Store
	loop       *reconcile.Loop
	runner     *runner.Runner
	icmp       *icmpsrc.Client
	responder  *responder.Server
	metrics    *metrics.Registry
	api        *api.Server

	rule    atomic.Pointer[zone.Rule]
	filter  atomic.Pointer[pairing.Filter]
	scanner atomic.Pointer[slot.Scanner]
}

func main() {
	var (
		cfgPath = flag.String("config", "/etc/meshd/meshd.yaml", "configuration file path")
		showVer = flag.Bool("version", false, "print the version and exit")
		check   = flag.Bool("check", false, "validate the configuration and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Println("meshd", Version)
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	setupLogging(cfg.Log)

	if *check {
		fmt.Println("configuration is valid")
		return
	}

	a, err := build(cfg, *cfgPath)
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(3)
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := a.run(ctx); err != nil {
		slog.Error("runtime failure", "error", err)
		os.Exit(4)
	}
	slog.Info("meshd stopped")
}

// setupLogging installs the structured logger.
func setupLogging(c config.LogConfig) {
	var level slog.Level
	switch c.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if c.Format == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}

// build constructs every component. It performs no network access, so a
// construction failure is a configuration failure.
func build(cfg *config.Config, cfgPath string) (*app, error) {
	a := &app{}

	a.metrics = metrics.New(metrics.Config{
		MaxSeries:   cfg.Metrics.MaxSeries,
		RTTBuckets:  cfg.Metrics.RTTBuckets,
		Percentiles: cfg.Metrics.Percentiles,
	})

	if err := a.buildTopology(cfg); err != nil {
		return nil, err
	}

	a.cfgWatcher = config.NewWatcher(cfgPath, cfg)
	a.store = inventory.NewStore()
	a.provs = provider.NewManager(a.store, a.metrics)
	registerProviders(a.provs)
	a.health = health.New(cfg.Health, nil)

	// The state file is loaded before the first reconcile, so existing
	// assignments survive a restart and no time series breaks.
	loaded, ok, err := state.Load(cfg.Persist.Path)
	if err != nil {
		slog.Warn("state discarded, assignment restarts from scratch",
			"path", cfg.Persist.Path, "error", err)
		a.metrics.AddStateReset()
	} else if ok {
		slog.Info("state loaded", "path", cfg.Persist.Path,
			"pairings", len(loaded.Pairings))
	}
	a.state = state.NewStore(cfg.Persist, loaded)

	a.icmp = icmpsrc.New(cfg.MeshPing)
	a.responder = responder.New(cfg.Responder)

	probers := map[probe.Kind]probe.Prober{
		probe.KindTCP: tcpsrc.New(),
		probe.KindUDP: udpsrc.New(),
	}
	if cfg.Probes.ICMP.Enabled {
		probers[probe.KindICMP] = a.icmp
	}

	a.runner = runner.New(runner.Deps{
		NodeID:  cfg.NodeID,
		Config:  a.cfgWatcher.Current,
		Probers: probers,
		Metrics: a.metrics,
		Health:  a.health,
	})

	a.loop = reconcile.NewLoop(reconcile.Deps{
		Store:   a.store,
		Config:  a.cfgWatcher.Current,
		Health:  a.health,
		State:   a.state,
		Metrics: a.metrics,
		Rule:    func() *zone.Rule { return a.rule.Load() },
		Filter:  func() *pairing.Filter { return a.filter.Load() },
		Scanner: func() *slot.Scanner { return a.scanner.Load() },
		OnDelta: a.onDelta,
	})

	a.api = api.New(cfg.API, api.Deps{
		Config:    a.cfgWatcher.Current,
		Store:     a.store,
		Rule:      func() *zone.Rule { return a.rule.Load() },
		State:     a.state,
		Health:    a.health,
		Runner:    a.runner,
		Providers: a.provs,
		Loop:      a.loop,
		Metrics:   a.metrics,
		Version:   Version,
		StartedAt: time.Now(),
	})
	return a, nil
}

// buildTopology compiles the zone rule, the pairing filter, and the
// scanner. A reload calls this again, so the three values always match
// the active configuration.
func (a *app) buildTopology(cfg *config.Config) error {
	rule, err := zone.NewRule(cfg.Zone)
	if err != nil {
		return err
	}
	filter, err := pairing.NewFilter(cfg.Pairings)
	if err != nil {
		return err
	}
	a.rule.Store(rule)
	a.filter.Store(filter)
	a.scanner.Store(slot.NewScanner(cfg.Slots))
	return nil
}

// run starts every goroutine and blocks until the context is cancelled.
// The start order is: state store, providers, meshping, responder,
// reconcile loop, runner, configuration watcher, API.
func (a *app) run(ctx context.Context) error {
	cfg := a.cfgWatcher.Current()
	slog.Info("meshd starting", "version", Version, "node_id", cfg.NodeID,
		"zone_rule", a.rule.Load().String())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	start := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				slog.Error("component failed", "component", name, "error", err)
			}
		}()
	}

	start("state", a.state.Run)

	if err := a.provs.Apply(ctx, cfg); err != nil {
		slog.Error("provider start failed", "error", err)
	}

	if cfg.Probes.ICMP.Enabled {
		start("meshping", a.icmp.Run)
	}
	start("responder", a.responder.Run)
	start("runner", a.runner.Run)
	start("reconcile", a.loop.Run)
	start("config", a.cfgWatcher.Run)
	start("api", a.api.Run)

	// The provider generation is watched here rather than inside the
	// providers, so that one discovery source cannot trigger the
	// reconcile more often than the others.
	start("watch", a.watchInventory)
	start("stats", a.publishStats)

	reloads := a.cfgWatcher.Subscribe()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case newCfg := <-reloads:
				if err := a.onConfig(ctx, newCfg); err != nil {
					slog.Error("configuration apply failed", "error", err)
				}
			}
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown requested")
	a.shutdown(15 * time.Second)
	wg.Wait()
	return nil
}

// watchInventory triggers a reconcile when the inventory generation
// advances, so a provider update is applied without waiting for the
// periodic tick.
func (a *app) watchInventory(ctx context.Context) error {
	var last uint64
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if g := a.store.Generation(); g != last {
				last = g
				a.loop.Trigger(reconcile.TriggerProvider)
			}
		}
	}
}

// publishStats copies the component counters into the metrics registry
// on a fixed interval, so the exporter does not read locks on scrape.
func (a *app) publishStats(ctx context.Context) error {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			a.metrics.SetPersist(a.state.Stats())
			a.metrics.SetResponder(a.responder.Stats())
			a.metrics.SetICMPAvailable(a.icmp.Available())
			a.metrics.SetMeshPingRestarts(a.icmp.Restarts())
			a.metrics.SetTasks(a.runner.Count())

			snap := a.store.Snapshot()
			bySource := make(map[string]int, len(snap.Sources))
			for name, s := range snap.Sources {
				bySource[name] = s.Hosts
			}
			a.metrics.SetHealth(a.health.Counts(), bySource)
		}
	}
}

// onConfig applies a reloaded configuration. It rebuilds the topology
// values, reapplies the provider set, and triggers a reconcile.
func (a *app) onConfig(ctx context.Context, cfg *config.Config) error {
	if err := a.buildTopology(cfg); err != nil {
		return err
	}
	a.health.SetConfig(cfg.Health)
	if err := a.provs.Apply(ctx, cfg); err != nil {
		return err
	}
	a.loop.Trigger(reconcile.TriggerConfig)
	return nil
}

// onDelta forwards a reconcile result to the runner. The runner starts
// and stops only what changed, so an unchanged task keeps running.
func (a *app) onDelta(d reconcile.Delta, st *state.State, snap *inventory.Snapshot) {
	a.runner.Apply(st, snap)
}

// shutdown stops components in reverse order and writes the state.
func (a *app) shutdown(grace time.Duration) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.provs.Stop()
		if written, err := a.state.Flush(); err != nil {
			slog.Error("final state write failed", "error", err)
		} else if written {
			slog.Info("state written")
		}
	}()
	select {
	case <-done:
	case <-time.After(grace):
		slog.Warn("shutdown grace expired")
	}
}
