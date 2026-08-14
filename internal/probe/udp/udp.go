// Package udp measures UDP echo round trip time. It requires a responder
// on the target port, which meshd runs by default.
package udp

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/example/mesh/internal/probe"
)

// Prober sends a burst of packets on one socket and matches replies by
// sequence number, so that loss and reorder are distinguishable.
type Prober struct{}

// New creates a UDP Prober.
func New() *Prober { return &Prober{} }

func (p *Prober) Kind() probe.Kind { return probe.KindUDP }

// Probe runs one cycle: send Count packets at Interval, then read until
// the last packet's timeout expires.
func (p *Prober) Probe(ctx context.Context, t probe.Target, pr probe.Params) (probe.Cycle, error) {
	c := probe.Cycle{Kind: probe.KindUDP, Target: t, StartedAt: time.Now()}

	conn, err := net.DialTimeout("udp", t.Addr(), pr.Timeout)
	if err != nil {
		c.Err = err
		c.ErrClass = probe.Classify(err)
		return c, nil
	}
	defer func() { _ = conn.Close() }()

	n := pr.PayloadBytes
	if n < probe.HeaderSize {
		n = probe.HeaderSize
	}
	sent := make(map[uint32]time.Time, pr.Count)
	order := make([]uint32, 0, pr.Count)

	for i := 0; i < pr.Count; i++ {
		if ctx.Err() != nil {
			break
		}
		seq := uint32(i + 1)
		at, err := p.send(conn, seq, n)
		if err != nil {
			if c.Err == nil {
				c.Err = err
				c.ErrClass = probe.Classify(err)
			}
			continue
		}
		c.Sent++
		sent[seq] = at
		order = append(order, seq)

		if i < pr.Count-1 && pr.Interval > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(pr.Interval):
			}
		}
	}

	if c.Sent == 0 {
		return c, nil
	}

	deadline := time.Now().Add(pr.Timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	rtts, arrived, err := p.receive(conn, sent, deadline)
	if err != nil && c.Err == nil {
		c.Err = err
		c.ErrClass = probe.Classify(err)
	}

	// Order the samples by send sequence, because the jitter
	// calculation reads them in send order.
	byPos := make(map[uint32]time.Duration, len(rtts))
	for seq, d := range rtts {
		byPos[seq] = d
	}
	for _, seq := range order {
		if d, ok := byPos[seq]; ok {
			c.RTT = append(c.RTT, d)
			c.Received++
		} else {
			c.Lost++
		}
	}
	c.Reordered = arrived
	return c, nil
}

// send writes one packet with the sequence number embedded.
func (p *Prober) send(conn net.Conn, seq uint32, n int) (time.Time, error) {
	buf := make([]byte, n)
	h := probe.Header{Seq: seq, SendNanos: time.Now().UnixNano()}
	if err := probe.EncodeHeader(buf, h, n); err != nil {
		return time.Time{}, err
	}
	at := time.Now()
	if _, err := conn.Write(buf); err != nil {
		return time.Time{}, err
	}
	return at, nil
}

// receive reads replies until the deadline and matches them by sequence.
// A reply whose sequence is lower than a reply already seen counts as a
// reorder rather than as a loss.
func (p *Prober) receive(conn net.Conn, sent map[uint32]time.Time,
	deadline time.Time) (map[uint32]time.Duration, int, error) {

	out := make(map[uint32]time.Duration, len(sent))
	buf := make([]byte, 65535)
	reordered := 0
	var highest uint32

	for len(out) < len(sent) {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return out, reordered, err
		}
		n, err := conn.Read(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return out, reordered, nil
			}
			return out, reordered, err
		}
		now := time.Now()
		h, err := probe.DecodeHeader(buf[:n])
		if err != nil {
			continue
		}
		at, ok := sent[h.Seq]
		if !ok {
			continue
		}
		if _, dup := out[h.Seq]; dup {
			continue
		}
		out[h.Seq] = now.Sub(at)
		if h.Seq < highest {
			reordered++
		} else {
			highest = h.Seq
		}
	}
	return out, reordered, nil
}

