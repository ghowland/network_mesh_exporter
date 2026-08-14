// Package metrics holds the Prometheus registry and every metric
// definition. It also enforces the series limit, so that a wrong zone
// rule cannot exhaust memory through label cardinality.
package metrics

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/example/mesh/internal/health"
	"github.com/example/mesh/internal/probe"
	"github.com/example/mesh/internal/reconcile"
	"github.com/example/mesh/internal/responder"
	"github.com/example/mesh/internal/runner"
	"github.com/example/mesh/internal/state"
)

// probeLabels is the label set attached to every probe metric. The slot
// and reuse_rank labels are what let a query separate a path problem
// from a single bad host.
var probeLabels = []string{
	"zone_src", "zone_dst", "host_src", "host_dst",
	"slot", "class", "reuse_rank", "probe",
}

// Registry holds every metric and enforces the series limit.
type Registry struct {
	reg  *prometheus.Registry
	cfg  Config
	mu   sync.Mutex
	seen map[string]bool

	rtt         *prometheus.HistogramVec
	rttMin      *prometheus.GaugeVec
	rttMax      *prometheus.GaugeVec
	rttMean     *prometheus.GaugeVec
	rttQuantile *prometheus.GaugeVec
	jitter      *prometheus.GaugeVec
	loss        *prometheus.GaugeVec
	sent        *prometheus.CounterVec
	received    *prometheus.CounterVec
	lost        *prometheus.CounterVec
	reorder     *prometheus.CounterVec
	tcpConnect  *prometheus.HistogramVec
	probeErrors *prometheus.CounterVec
	lastSuccess *prometheus.GaugeVec

	hosts          *prometheus.GaugeVec
	hostsUnres     prometheus.Gauge
	zones          prometheus.Gauge
	pairings       prometheus.Gauge
	slots          *prometheus.GaugeVec
	slotsUnfilled  prometheus.Gauge
	slotChanges    *prometheus.CounterVec
	reconcileDur   prometheus.Histogram
	reconcileTotal *prometheus.CounterVec
	reconcileErrs  prometheus.Counter

	providerFetch   *prometheus.CounterVec
	providerCache   *prometheus.GaugeVec
	providerSuccess *prometheus.GaugeVec

	persistTotal  prometheus.Gauge
	persistFails  prometheus.Gauge
	persistDirty  prometheus.Gauge
	stateReset    prometheus.Counter
	icmpAvailable prometheus.Gauge
	pingRestarts  prometheus.Gauge
	tasksRunning  prometheus.Gauge
	seriesCount   prometheus.Gauge
	seriesDropped prometheus.Counter
	responderVec  *prometheus.GaugeVec
}

// Config is the metrics configuration the registry needs.
type Config struct {
	MaxSeries   int
	RTTBuckets  []float64
	Percentiles []float64
}

// New builds the registry and registers every collector.
func New(cfg Config) *Registry {
	if len(cfg.RTTBuckets) == 0 {
		cfg.RTTBuckets = prometheus.DefBuckets
	}
	r := &Registry{
		reg:  prometheus.NewRegistry(),
		cfg:  cfg,
		seen: make(map[string]bool),
	}
	f := promauto(r.reg)

	r.rtt = f.histogramVec("mesh_rtt_seconds",
		"Round trip time of one probe sample.", cfg.RTTBuckets, probeLabels)
	r.rttMin = f.gaugeVec("mesh_rtt_min_seconds",
		"Lowest round trip time in the current window.", probeLabels)
	r.rttMax = f.gaugeVec("mesh_rtt_max_seconds",
		"Highest round trip time in the current window.", probeLabels)
	r.rttMean = f.gaugeVec("mesh_rtt_mean_seconds",
		"Mean round trip time in the current window.", probeLabels)
	r.rttQuantile = f.gaugeVec("mesh_rtt_quantile_seconds",
		"Round trip time quantile in the current window.",
		append(append([]string{}, probeLabels...), "quantile"))
	r.jitter = f.gaugeVec("mesh_jitter_seconds",
		"Mean absolute difference between consecutive round trip times.", probeLabels)
	r.loss = f.gaugeVec("mesh_loss_ratio",
		"Lost probes divided by sent probes in the current window.", probeLabels)
	r.sent = f.counterVec("mesh_packets_sent_total",
		"Probes sent.", probeLabels)
	r.received = f.counterVec("mesh_packets_received_total",
		"Probe replies received.", probeLabels)
	r.lost = f.counterVec("mesh_packets_lost_total",
		"Probes with no reply.", probeLabels)
	r.reorder = f.counterVec("mesh_reorder_total",
		"Replies that arrived out of send order.", probeLabels)
	r.tcpConnect = f.histogramVec("mesh_tcp_connect_seconds",
		"TCP handshake time, separate from the payload round trip.",
		cfg.RTTBuckets, probeLabels)
	r.probeErrors = f.counterVec("mesh_probe_errors_total",
		"Probe cycles that ended in an error, by error class.",
		append(append([]string{}, probeLabels...), "err_class"))
	r.lastSuccess = f.gaugeVec("mesh_probe_last_success_timestamp_seconds",
		"Unix time of the most recent successful sample.", probeLabels)

	r.hosts = f.gaugeVec("mesh_hosts_total",
		"Hosts in the inventory, by source and health state.", []string{"source", "state"})
	r.hostsUnres = f.gauge("mesh_hosts_unresolved",
		"Hosts that produced no zone key under the current rule.")
	r.zones = f.gauge("mesh_zones_total", "Zones derived from the inventory.")
	r.pairings = f.gauge("mesh_pairings_total", "Active zone pairings.")
	r.slots = f.gaugeVec("mesh_slots_total", "Slots by class.", []string{"class"})
	r.slotsUnfilled = f.gauge("mesh_slots_unfilled",
		"Slots with at least one empty side.")
	r.slotChanges = f.counterVec("mesh_slot_changes_total",
		"Slot sides that changed, by reason.", []string{"reason"})
	r.reconcileDur = f.histogram("mesh_reconcile_duration_seconds",
		"Time taken by one reconcile.",
		[]float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5})
	r.reconcileTotal = f.counterVec("mesh_reconcile_total",
		"Reconcile runs, by trigger.", []string{"trigger"})
	r.reconcileErrs = f.counter("mesh_reconcile_errors_total",
		"Reconcile runs that failed and kept the previous assignment.")

	r.providerFetch = f.counterVec("mesh_provider_fetch_total",
		"Provider fetches, by source and result.", []string{"source", "result"})
	r.providerCache = f.gaugeVec("mesh_provider_cache_age_seconds",
		"Age of the cached inventory document.", []string{"source"})
	r.providerSuccess = f.gaugeVec("mesh_provider_last_success_timestamp_seconds",
		"Unix time of the most recent successful provider fetch.", []string{"source"})

	r.persistTotal = f.gauge("mesh_state_persist_total", "State file writes.")
	r.persistFails = f.gauge("mesh_state_persist_failures_total", "State file write failures.")
	r.persistDirty = f.gauge("mesh_state_dirty",
		"One when the in-memory state has unwritten changes.")
	r.stateReset = f.counter("mesh_state_reset_total",
		"Times the state was discarded and assignment restarted.")
	r.icmpAvailable = f.gauge("mesh_icmp_available",
		"One when the meshping helper is running with ICMP permission.")
	r.pingRestarts = f.gauge("mesh_meshping_restarts_total",
		"Restarts of the meshping helper.")
	r.tasksRunning = f.gauge("mesh_tasks_running", "Probe tasks running on this node.")
	r.seriesCount = f.gauge("mesh_series_count", "Registered probe label sets.")
	r.seriesDropped = f.counter("mesh_series_dropped_total",
		"Samples dropped because the series limit was reached.")
	r.responderVec = f.gaugeVec("mesh_responder_total",
		"Responder activity counters.", []string{"kind"})

	r.reg.MustRegister(collectors.NewGoCollector())
	r.reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return r
}

// Handler returns the Prometheus exposition handler.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}

// labelsFor builds the probe label values for one task.
func labelsFor(t *runner.Task) prometheus.Labels {
	return prometheus.Labels{
		"zone_src":   t.ZoneSrc,
		"zone_dst":   t.ZoneDst,
		"host_src":   t.Src.HostID,
		"host_dst":   t.Dst.HostID,
		"slot":       strconv.Itoa(t.Key.Slot),
		"class":      t.Class,
		"reuse_rank": strconv.Itoa(t.Rank),
		"probe":      string(t.Key.Kind),
	}
}

func labelKey(l prometheus.Labels) string {
	return l["zone_src"] + "|" + l["zone_dst"] + "|" + l["host_src"] + "|" +
		l["host_dst"] + "|" + l["slot"] + "|" + l["class"] + "|" +
		l["reuse_rank"] + "|" + l["probe"]
}

// admit reserves a series slot and reports whether the sample may be
// recorded.
func (r *Registry) admit(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen[key] {
		return true
	}
	if r.cfg.MaxSeries > 0 && len(r.seen) >= r.cfg.MaxSeries {
		return false
	}
	r.seen[key] = true
	r.seriesCount.Set(float64(len(r.seen)))
	return true
}

// Observe records one completed cycle and its window statistics.
func (r *Registry) Observe(t *runner.Task, c probe.Cycle, st probe.Stats) {
	l := labelsFor(t)
	if !r.admit(labelKey(l)) {
		r.seriesDropped.Inc()
		return
	}

	for _, d := range c.RTT {
		r.rtt.With(l).Observe(d.Seconds())
	}
	for _, d := range c.ConnectRTT {
		r.tcpConnect.With(l).Observe(d.Seconds())
	}
	r.sent.With(l).Add(float64(c.Sent))
	r.received.With(l).Add(float64(c.Received))
	r.lost.With(l).Add(float64(c.Lost))
	r.reorder.With(l).Add(float64(c.Reordered))
	if c.ErrClass != "" {
		el := copyLabels(l)
		el["err_class"] = string(c.ErrClass)
		r.probeErrors.With(el).Inc()
	}

	r.rttMin.With(l).Set(st.Min.Seconds())
	r.rttMax.With(l).Set(st.Max.Seconds())
	r.rttMean.With(l).Set(st.Mean.Seconds())
	r.jitter.With(l).Set(st.Jitter.Seconds())
	r.loss.With(l).Set(st.LossRatio)
	for q, d := range st.Percentiles {
		ql := copyLabels(l)
		ql["quantile"] = strconv.FormatFloat(q, 'g', -1, 64)
		r.rttQuantile.With(ql).Set(d.Seconds())
	}
	if !st.LastSuccess.IsZero() {
		r.lastSuccess.With(l).Set(float64(st.LastSuccess.Unix()))
	}
}

// Forget removes every series for one task, called when the task stops,
// so that a replaced host does not leave a stale time series behind.
func (r *Registry) Forget(t *runner.Task) {
	l := labelsFor(t)
	r.mu.Lock()
	delete(r.seen, labelKey(l))
	r.seriesCount.Set(float64(len(r.seen)))
	r.mu.Unlock()

	r.rtt.Delete(l)
	r.tcpConnect.Delete(l)
	r.rttMin.Delete(l)
	r.rttMax.Delete(l)
	r.rttMean.Delete(l)
	r.jitter.Delete(l)
	r.loss.Delete(l)
	r.sent.Delete(l)
	r.received.Delete(l)
	r.lost.Delete(l)
	r.reorder.Delete(l)
	r.lastSuccess.Delete(l)
	r.rttQuantile.DeletePartialMatch(l)
	r.probeErrors.DeletePartialMatch(l)
}

// ReconcileDone implements reconcile.Observer.
func (r *Registry) ReconcileDone(t reconcile.Trigger, st reconcile.Stats,
	d reconcile.Delta, err error) {

	r.reconcileTotal.WithLabelValues(string(t)).Inc()
	if err != nil {
		r.reconcileErrs.Inc()
		return
	}
	r.reconcileDur.Observe(st.Duration.Seconds())
	r.hostsUnres.Set(float64(st.Unresolved))
	r.zones.Set(float64(st.Zones))
	r.pairings.Set(float64(st.Pairings))
	r.slotsUnfilled.Set(float64(st.SlotsUnfilled))
	for class, n := range st.ByClass {
		r.slots.WithLabelValues(class).Set(float64(n))
	}
	for _, c := range d.SidesChanged {
		r.slotChanges.WithLabelValues(string(c.Reason)).Inc()
	}
}

// Success implements provider.Reporter.
func (r *Registry) Success(source string, hosts int, at time.Time) {
	r.providerFetch.WithLabelValues(source, "success").Inc()
	if !at.IsZero() {
		r.providerSuccess.WithLabelValues(source).Set(float64(at.Unix()))
	}
}

// Failure implements provider.Reporter.
func (r *Registry) Failure(source string, err error, at time.Time) {
	r.providerFetch.WithLabelValues(source, "failure").Inc()
}

// CacheAge implements provider.Reporter.
func (r *Registry) CacheAge(source string, age time.Duration) {
	r.providerCache.WithLabelValues(source).Set(age.Seconds())
}

// SetHealth publishes the host count by state.
func (r *Registry) SetHealth(counts map[health.State]int, bySource map[string]int) {
	r.hosts.Reset()
	for st, n := range counts {
		r.hosts.WithLabelValues("all", st.String()).Set(float64(n))
	}
	for src, n := range bySource {
		r.hosts.WithLabelValues(src, "present").Set(float64(n))
	}
}

// SetICMPAvailable publishes the helper availability.
func (r *Registry) SetICMPAvailable(ok bool) {
	if ok {
		r.icmpAvailable.Set(1)
		return
	}
	r.icmpAvailable.Set(0)
}

// SetMeshPingRestarts publishes the helper restart count.
func (r *Registry) SetMeshPingRestarts(n uint64) { r.pingRestarts.Set(float64(n)) }

// SetPersist publishes the state store activity.
func (r *Registry) SetPersist(s state.Stats) {
	r.persistTotal.Set(float64(s.Writes))
	r.persistFails.Set(float64(s.Failures))
	if s.Dirty {
		r.persistDirty.Set(1)
	} else {
		r.persistDirty.Set(0)
	}
}

// AddStateReset records a discarded state file.
func (r *Registry) AddStateReset() { r.stateReset.Inc() }

// SetResponder publishes the responder counters.
func (r *Registry) SetResponder(s responder.Stats) {
	r.responderVec.WithLabelValues("udp_packets").Set(float64(s.UDPPackets))
	r.responderVec.WithLabelValues("udp_rejected").Set(float64(s.UDPRejected))
	r.responderVec.WithLabelValues("tcp_conns").Set(float64(s.TCPConns))
	r.responderVec.WithLabelValues("tcp_rejected").Set(float64(s.TCPRejected))
}

// SetTasks publishes the running task count.
func (r *Registry) SetTasks(n int) { r.tasksRunning.Set(float64(n)) }

// SeriesCount returns the current registered series count.
func (r *Registry) SeriesCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

func copyLabels(l prometheus.Labels) prometheus.Labels {
	out := make(prometheus.Labels, len(l)+1)
	for k, v := range l {
		out[k] = v
	}
	return out
}

// factory reduces the repetition of the metric declarations above.
type factory struct{ reg *prometheus.Registry }

func promauto(reg *prometheus.Registry) factory { return factory{reg: reg} }

func (f factory) gauge(name, help string) prometheus.Gauge {
	m := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	f.reg.MustRegister(m)
	return m
}

func (f factory) counter(name, help string) prometheus.Counter {
	m := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	f.reg.MustRegister(m)
	return m
}

func (f factory) gaugeVec(name, help string, labels []string) *prometheus.GaugeVec {
	m := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	f.reg.MustRegister(m)
	return m
}

func (f factory) counterVec(name, help string, labels []string) *prometheus.CounterVec {
	m := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	f.reg.MustRegister(m)
	return m
}

func (f factory) histogram(name, help string, buckets []float64) prometheus.Histogram {
	m := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: name, Help: help, Buckets: buckets})
	f.reg.MustRegister(m)
	return m
}

func (f factory) histogramVec(name, help string, buckets []float64,
	labels []string) *prometheus.HistogramVec {
	m := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: name, Help: help, Buckets: buckets}, labels)
	f.reg.MustRegister(m)
	return m
}
