// Package probe defines the measurement interface, the payload format
// shared with the responder, and the rolling window that turns raw
// samples into the statistics the exporter publishes.
package probe

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/example/mesh/pkg/pingproto"
)

// Kind identifies the probe type. It becomes a metric label.
type Kind string

const (
	KindICMP Kind = "icmp"
	KindUDP  Kind = "udp"
	KindTCP  Kind = "tcp"
)

// Target is the destination of one directed task.
type Target struct {
	HostID  string
	Address string
	Port    int
}

// Addr returns the dial string for a port-based probe.
func (t Target) Addr() string {
	return net.JoinHostPort(t.Address, fmt.Sprint(t.Port))
}

// Params are the per-cycle probe settings, resolved from the config.
type Params struct {
	Kind         Kind
	Count        int
	Interval     time.Duration
	Timeout      time.Duration
	PayloadBytes int
	Port         int
	Mode         string // TCP only: connect or echo
	TTL          int    // ICMP only
	DF           bool   // ICMP only
}

// Cycle is the raw outcome of one probe cycle. It carries samples, not
// statistics, so that the window owns all aggregation.
type Cycle struct {
	Kind       Kind
	Target     Target
	StartedAt  time.Time
	Sent       int
	Received   int
	Lost       int
	Reordered  int
	RTT        []time.Duration
	ConnectRTT []time.Duration
	Err        error
	ErrClass   pingproto.ErrClass
}

// OK reports whether the cycle produced at least one sample. The health
// tracker uses this as the per-cycle probe outcome.
func (c Cycle) OK() bool { return c.Received > 0 }

// Prober runs one cycle against one target.
type Prober interface {
	Kind() Kind
	Probe(ctx context.Context, t Target, p Params) (Cycle, error)
}

// Magic is the four-byte value at the start of every UDP and TCP
// payload, so that a responder rejects unrelated traffic.
var Magic = [4]byte{0x6d, 0x65, 0x73, 0x68}

// HeaderSize is the encoded size of Header.
const HeaderSize = 16

// Header is the fixed prefix of a UDP or TCP payload. The responder
// returns it unchanged except for the receive timestamp, which it
// overwrites so that one-way delay information is available later
// without a second protocol.
type Header struct {
	Magic     [4]byte
	Seq       uint32
	SendNanos int64
}

// EncodeHeader writes a header and pads the buffer to n bytes. The
// buffer must be at least n bytes long.
func EncodeHeader(buf []byte, h Header, n int) error {
	if n < HeaderSize {
		return fmt.Errorf("probe: payload %d is below the header size %d", n, HeaderSize)
	}
	if len(buf) < n {
		return fmt.Errorf("probe: buffer %d is shorter than payload %d", len(buf), n)
	}
	copy(buf[0:4], Magic[:])
	binary.BigEndian.PutUint32(buf[4:8], h.Seq)
	binary.BigEndian.PutUint64(buf[8:16], uint64(h.SendNanos))
	for i := HeaderSize; i < n; i++ {
		buf[i] = byte(i)
	}
	return nil
}

// DecodeHeader reads a header and verifies the magic value.
func DecodeHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderSize {
		return Header{}, errors.New("probe: payload shorter than the header")
	}
	var h Header
	copy(h.Magic[:], buf[0:4])
	if h.Magic != Magic {
		return Header{}, errors.New("probe: payload magic does not match")
	}
	h.Seq = binary.BigEndian.Uint32(buf[4:8])
	h.SendNanos = int64(binary.BigEndian.Uint64(buf[8:16]))
	return h, nil
}

// Classify maps a network error to a stable ErrClass label value.
func Classify(err error) pingproto.ErrClass {
	if err == nil {
		return pingproto.ErrNone
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return pingproto.ErrResolve
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return pingproto.ErrTimeout
	}
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return pingproto.ErrRefused
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return pingproto.ErrUnreachable
	case errors.Is(err, syscall.EPERM), errors.Is(err, syscall.EACCES):
		return pingproto.ErrPermission
	}
	return pingproto.ErrInternal
}

// Stats is the aggregate over one window.
type Stats struct {
	Samples     int                           `json:"samples"`
	Sent        uint64                        `json:"sent"`
	Received    uint64                        `json:"received"`
	Lost        uint64                        `json:"lost"`
	Reordered   uint64                        `json:"reordered"`
	LossRatio   float64                       `json:"loss_ratio"`
	Min         time.Duration                 `json:"min"`
	Max         time.Duration                 `json:"max"`
	Mean        time.Duration                 `json:"mean"`
	Jitter      time.Duration                 `json:"jitter"`
	Percentiles map[float64]time.Duration     `json:"percentiles"`
	ConnectMean time.Duration                 `json:"connect_mean"`
	LastSuccess time.Time                     `json:"last_success"`
	Errors      map[pingproto.ErrClass]uint64 `json:"errors"`
}

type entry struct {
	at      time.Time
	rtt     time.Duration
	connect time.Duration
	hasConn bool
}

type counterEntry struct {
	at        time.Time
	sent      uint64
	received  uint64
	lost      uint64
	reordered uint64
	class     pingproto.ErrClass
}

// Window aggregates cycles over a rolling duration. It is safe for
// concurrent use.
type Window struct {
	mu       sync.Mutex
	dur      time.Duration
	samples  []entry
	counters []counterEntry
	lastOK   time.Time
}

// NewWindow creates a Window of the given duration.
func NewWindow(d time.Duration) *Window {
	if d <= 0 {
		d = 60 * time.Second
	}
	return &Window{dur: d}
}

// Add folds one cycle into the window and drops expired samples. The
// samples stay in send order, which the jitter calculation requires.
func (w *Window) Add(c Cycle) {
	w.mu.Lock()
	defer w.mu.Unlock()

	at := c.StartedAt
	if at.IsZero() {
		at = time.Now()
	}
	for i, rtt := range c.RTT {
		e := entry{at: at, rtt: rtt}
		if i < len(c.ConnectRTT) {
			e.connect, e.hasConn = c.ConnectRTT[i], true
		}
		w.samples = append(w.samples, e)
	}
	w.counters = append(w.counters, counterEntry{
		at:        at,
		sent:      uint64(c.Sent),
		received:  uint64(c.Received),
		lost:      uint64(c.Lost),
		reordered: uint64(c.Reordered),
		class:     c.ErrClass,
	})
	if c.Received > 0 {
		w.lastOK = at
	}
	w.expire(time.Now())
}

// expire drops entries outside the window. The caller holds the lock.
func (w *Window) expire(now time.Time) {
	cut := now.Add(-w.dur)
	i := 0
	for i < len(w.samples) && w.samples[i].at.Before(cut) {
		i++
	}
	if i > 0 {
		w.samples = append(w.samples[:0], w.samples[i:]...)
	}
	j := 0
	for j < len(w.counters) && w.counters[j].at.Before(cut) {
		j++
	}
	if j > 0 {
		w.counters = append(w.counters[:0], w.counters[j:]...)
	}
}

// Reset clears the window. It is called when a slot side changes host,
// so that samples from the old host do not mix with the new one.
func (w *Window) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.samples = w.samples[:0]
	w.counters = w.counters[:0]
	w.lastOK = time.Time{}
}

// Stats computes the current aggregate. Jitter is the mean absolute
// difference between consecutive samples in send order.
func (w *Window) Stats(percentiles []float64) Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.expire(time.Now())

	out := Stats{
		Samples:     len(w.samples),
		LastSuccess: w.lastOK,
		Percentiles: make(map[float64]time.Duration, len(percentiles)),
		Errors:      make(map[pingproto.ErrClass]uint64),
	}
	for _, c := range w.counters {
		out.Sent += c.sent
		out.Received += c.received
		out.Lost += c.lost
		out.Reordered += c.reordered
		if c.class != pingproto.ErrNone {
			out.Errors[c.class]++
		}
	}
	if out.Sent > 0 {
		out.LossRatio = float64(out.Lost) / float64(out.Sent)
	}
	if len(w.samples) == 0 {
		return out
	}

	var total, jitterSum time.Duration
	var connTotal time.Duration
	connCount := 0
	out.Min = w.samples[0].rtt
	out.Max = w.samples[0].rtt

	for i, s := range w.samples {
		total += s.rtt
		if s.rtt < out.Min {
			out.Min = s.rtt
		}
		if s.rtt > out.Max {
			out.Max = s.rtt
		}
		if i > 0 {
			d := s.rtt - w.samples[i-1].rtt
			if d < 0 {
				d = -d
			}
			jitterSum += d
		}
		if s.hasConn {
			connTotal += s.connect
			connCount++
		}
	}
	out.Mean = total / time.Duration(len(w.samples))
	if len(w.samples) > 1 {
		out.Jitter = jitterSum / time.Duration(len(w.samples)-1)
	}
	if connCount > 0 {
		out.ConnectMean = connTotal / time.Duration(connCount)
		connMean := out.ConnectMean
		_ = connMean
	}

	sorted := make([]time.Duration, len(w.samples))
	for i, s := range w.samples {
		sorted[i] = s.rtt
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, q := range percentiles {
		out.Percentiles[q] = quantile(sorted, q)
	}
	return out
}

// quantile returns the nearest-rank value for q over a sorted slice.
func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q*float64(len(sorted)-1) + 0.5)
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
