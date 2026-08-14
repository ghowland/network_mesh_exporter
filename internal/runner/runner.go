// Package runner owns the running measurement tasks. It applies the
// reconcile delta by starting and stopping only what changed, so a task
// whose endpoints did not move is never interrupted.
package runner

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/health"
	"github.com/example/mesh/internal/inventory"
	"github.com/example/mesh/internal/probe"
	"github.com/example/mesh/internal/slot"
	"github.com/example/mesh/internal/state"
)

// TaskKey identifies one directed probe task. It is stable across
// reconciles, so an unchanged task is never restarted.
type TaskKey struct {
	Pairing string
	Slot    int
	Kind    probe.Kind
	Forward bool // true means side A is the source
}

// String returns the canonical text form, used as a map key and in logs.
func (k TaskKey) String() string {
	dir := "ba"
	if k.Forward {
		dir = "ab"
	}
	return fmt.Sprintf("%s#%d/%s/%s", k.Pairing, k.Slot, k.Kind, dir)
}

// Task is one running measurement. Each task owns one goroutine and one
// ticker.
type Task struct {
	Key     TaskKey
	Src     probe.Target
	Dst     probe.Target
	ZoneSrc string
	ZoneDst string
	Class   string
	Rank    int
	Params  probe.Params
	Window  *probe.Window

	startedAt time.Time
	cancel    context.CancelFunc
	done      chan struct{}
	prober    probe.Prober
}

// ident describes the endpoints of a task. A change of any field means
// the task measures a different thing and must be restarted with a fresh
// window, so that samples from two hosts never merge into one series.
func (t *Task) ident() string {
	return t.Src.HostID + ">" + t.Dst.HostID + "@" + t.Dst.Addr()
}

// TaskInfo is the API view of one running task.
type TaskInfo struct {
	Key       string      `json:"key"`
	Src       string      `json:"src"`
	Dst       string      `json:"dst"`
	ZoneSrc   string      `json:"zone_src"`
	ZoneDst   string      `json:"zone_dst"`
	Kind      string      `json:"kind"`
	Class     string      `json:"class"`
	Rank      int         `json:"rank"`
	StartedAt time.Time   `json:"started_at"`
	Stats     probe.Stats `json:"stats"`
}

// Sink receives each completed cycle for export.
type Sink interface {
	Observe(t *Task, c probe.Cycle, st probe.Stats)
	Forget(t *Task)
}

// Deps holds what the Runner needs from the rest of the system.
type Deps struct {
	NodeID  string
	Config  func() *config.Config
	Probers map[probe.Kind]probe.Prober
	Metrics Sink
	Health  *health.Tracker
}

// Runner owns the task set.
type Runner struct {
	deps Deps

	mu    sync.Mutex
	tasks map[TaskKey]*Task
	ctx   context.Context
	wg    sync.WaitGroup
}

// New creates a Runner.
func New(d Deps) *Runner {
	return &Runner{deps: d, tasks: make(map[TaskKey]*Task)}
}

// Run blocks until ctx is cancelled, then stops every task and waits.
func (r *Runner) Run(ctx context.Context) error {
	r.mu.Lock()
	r.ctx = ctx
	r.mu.Unlock()

	<-ctx.Done()

	r.mu.Lock()
	keys := make([]TaskKey, 0, len(r.tasks))
	for k := range r.tasks {
		keys = append(keys, k)
	}
	r.mu.Unlock()
	for _, k := range keys {
		r.stopTask(k)
	}
	r.wg.Wait()
	return nil
}

// Apply installs a new state. It starts tasks that are new, stops tasks
// that are gone, and leaves unchanged tasks running without
// interruption. A task whose endpoints changed is stopped and restarted
// with a fresh window.
func (r *Runner) Apply(st *state.State, snap *inventory.Snapshot) {
	r.mu.Lock()
	ready := r.ctx != nil
	r.mu.Unlock()
	if !ready {
		return
	}

	want := r.desired(st, snap)

	r.mu.Lock()
	var toStop []TaskKey
	var toStart []*Task
	for k, cur := range r.tasks {
		nt, ok := want[k]
		if !ok {
			toStop = append(toStop, k)
			continue
		}
		if nt.ident() != cur.ident() || nt.Params != cur.Params {
			toStop = append(toStop, k)
			toStart = append(toStart, nt)
			continue
		}
		// The task is unchanged, so it keeps running with its window.
		// Only the labels that do not affect measurement are refreshed.
		cur.Class = nt.Class
		cur.Rank = nt.Rank
	}
	for k, nt := range want {
		if _, ok := r.tasks[k]; !ok {
			toStart = append(toStart, nt)
			_ = k
		}
	}
	r.mu.Unlock()

	for _, k := range toStop {
		r.stopTask(k)
	}
	for _, t := range toStart {
		r.startTask(t)
	}

	if len(toStop) > 0 || len(toStart) > 0 {
		slog.Info("task set updated",
			"started", len(toStart), "stopped", len(toStop), "running", r.count())
	}
}

func (r *Runner) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.tasks)
}

// desired computes the task set for this node. It selects only the slots
// where this node is an endpoint, and only the direction where this node
// is the source, so each node probes its own direction independently
// with no coordination.
func (r *Runner) desired(st *state.State, snap *inventory.Snapshot) map[TaskKey]*Task {
	cfg := r.deps.Config()
	out := make(map[TaskKey]*Task)

	kinds := make([]probe.Kind, 0, 3)
	if cfg.Probes.ICMP.Enabled {
		kinds = append(kinds, probe.KindICMP)
	}
	if cfg.Probes.UDP.Enabled {
		kinds = append(kinds, probe.KindUDP)
	}
	if cfg.Probes.TCP.Enabled {
		kinds = append(kinds, probe.KindTCP)
	}

	window := cfg.Probes.Window.D()

	for key, p := range st.Pairings {
		if !p.RemoveAt.IsZero() {
			continue
		}
		for _, s := range p.Slots {
			if !s.Filled() {
				continue
			}
			// Both directions are candidates; this node keeps the one
			// where it is the source.
			pairs := []struct {
				forward  bool
				src, dst string
				zs, zd   string
				rank     int
			}{
				{true, s.HostA, s.HostB, p.ZoneA, p.ZoneB, s.RankA},
				{false, s.HostB, s.HostA, p.ZoneB, p.ZoneA, s.RankB},
			}
			for _, d := range pairs {
				if d.src != r.deps.NodeID {
					continue
				}
				dstRec, ok := snap.Get(d.dst)
				if !ok {
					continue
				}
				for _, kind := range kinds {
					params, port := probeParams(cfg, kind)
					k := TaskKey{Pairing: key, Slot: s.Index, Kind: kind, Forward: d.forward}
					out[k] = &Task{
						Key:     k,
						Src:     probe.Target{HostID: d.src},
						Dst:     probe.Target{HostID: d.dst, Address: dstRec.Address, Port: port},
						ZoneSrc: d.zs,
						ZoneDst: d.zd,
						Class:   s.Class,
						Rank:    d.rank,
						Params:  params,
						Window:  probe.NewWindow(window),
					}
				}
			}
		}
	}
	return out
}

// probeParams resolves the per-kind settings from the configuration.
func probeParams(cfg *config.Config, kind probe.Kind) (probe.Params, int) {
	switch kind {
	case probe.KindICMP:
		c := cfg.Probes.ICMP
		return probe.Params{
			Kind: kind, Count: c.Count, Interval: c.Interval.D(),
			Timeout: c.Timeout.D(), PayloadBytes: c.PayloadBytes,
			TTL: c.TTL, DF: c.DF,
		}, 0
	case probe.KindUDP:
		c := cfg.Probes.UDP
		return probe.Params{
			Kind: kind, Count: c.Count, Interval: c.Interval.D(),
			Timeout: c.Timeout.D(), PayloadBytes: c.PayloadBytes, Port: c.Port,
		}, c.Port
	default:
		c := cfg.Probes.TCP
		return probe.Params{
			Kind: kind, Count: c.Count, Interval: c.Interval.D(),
			Timeout: c.Timeout.D(), PayloadBytes: c.PayloadBytes,
			Port: c.Port, Mode: c.Mode,
		}, c.Port
	}
}

// startTask launches one task goroutine.
func (r *Runner) startTask(t *Task) {
	r.mu.Lock()
	parent := r.ctx
	if parent == nil {
		r.mu.Unlock()
		return
	}
	if _, exists := r.tasks[t.Key]; exists {
		r.mu.Unlock()
		return
	}
	prober := r.deps.Probers[t.Key.Kind]
	if prober == nil {
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	t.cancel = cancel
	t.done = make(chan struct{})
	t.prober = prober
	t.startedAt = time.Now()
	r.tasks[t.Key] = t
	r.mu.Unlock()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(t.done)
		r.loop(ctx, t)
	}()
}

// stopTask cancels one task, waits for the goroutine to exit, and drops
// its metric series so that a replaced host leaves nothing stale behind.
func (r *Runner) stopTask(k TaskKey) {
	r.mu.Lock()
	t := r.tasks[k]
	delete(r.tasks, k)
	r.mu.Unlock()
	if t == nil {
		return
	}
	t.cancel()
	select {
	case <-t.done:
	case <-time.After(30 * time.Second):
		slog.Warn("task did not stop in time", "task", k.String())
	}
	if r.deps.Metrics != nil {
		r.deps.Metrics.Forget(t)
	}
}

// loop runs one task until its context is cancelled. The first tick is
// delayed by a per-task offset, so that all tasks do not fire in the
// same instant and produce a synchronised burst on the network.
func (r *Runner) loop(ctx context.Context, t *Task) {
	cfg := r.deps.Config()
	cycle := cfg.Probes.Cycle.D()
	if cycle <= 0 {
		cycle = 15 * time.Second
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(offset(t.Key, cycle)):
	}

	ticker := time.NewTicker(cycle)
	defer ticker.Stop()

	for {
		r.runCycle(ctx, t)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runCycle performs one measurement and publishes it.
func (r *Runner) runCycle(ctx context.Context, t *Task) {
	budget := time.Duration(t.Params.Count)*(t.Params.Interval+t.Params.Timeout) + 10*time.Second
	cctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	c, err := t.prober.Probe(cctx, t.Dst, t.Params)
	if err != nil {
		c.Err = err
		c.ErrClass = probe.Classify(err)
	}
	if c.StartedAt.IsZero() {
		c.StartedAt = time.Now()
	}
	t.Window.Add(c)

	if r.deps.Health != nil {
		// Only the destination health is inferred here. The source is
		// this node, and a local failure would affect every task alike.
		r.deps.Health.RecordProbe(t.Dst.HostID, c.OK())
		if c.ErrClass == "resolve" {
			r.deps.Health.RecordResolve(t.Dst.HostID, false)
		} else if c.OK() {
			r.deps.Health.RecordResolve(t.Dst.HostID, true)
		}
	}
	if r.deps.Metrics != nil {
		cfg := r.deps.Config()
		r.deps.Metrics.Observe(t, c, t.Window.Stats(cfg.Metrics.Percentiles))
	}
}

// offset returns the start delay for one task inside the cycle. It is a
// pure function of the key, so the spread is stable across restarts.
func offset(k TaskKey, cycle time.Duration) time.Duration {
	h := fnv.New32a()
	_, _ = h.Write([]byte(k.String()))
	return time.Duration(uint64(h.Sum32()) % uint64(cycle))
}

// Tasks returns the running task set for the API.
func (r *Runner) Tasks() []TaskInfo {
	cfg := r.deps.Config()
	r.mu.Lock()
	list := make([]*Task, 0, len(r.tasks))
	for _, t := range r.tasks {
		list = append(list, t)
	}
	r.mu.Unlock()

	out := make([]TaskInfo, 0, len(list))
	for _, t := range list {
		out = append(out, TaskInfo{
			Key:       t.Key.String(),
			Src:       t.Src.HostID,
			Dst:       t.Dst.HostID,
			ZoneSrc:   t.ZoneSrc,
			ZoneDst:   t.ZoneDst,
			Kind:      string(t.Key.Kind),
			Class:     t.Class,
			Rank:      t.Rank,
			StartedAt: t.startedAt,
			Stats:     t.Window.Stats(cfg.Metrics.Percentiles),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Count returns the running task count for the metrics.
func (r *Runner) Count() int { return r.count() }

// slotClass is retained for readability at call sites that compare
// against the class constants.
var _ = slot.ClassAnchor

