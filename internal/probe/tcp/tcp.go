// Package tcp measures TCP handshake time, and optionally an echo round
// trip when a responder is present on the target port.
package tcp

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/example/mesh/internal/probe"
)

// Prober measures TCP connect time and, in echo mode, the payload round
// trip separately from the handshake.
type Prober struct {
	dialer *net.Dialer
}

// New creates a TCP Prober.
func New() *Prober {
	return &Prober{dialer: &net.Dialer{}}
}

func (p *Prober) Kind() probe.Kind { return probe.KindTCP }

// Probe runs one cycle. Each iteration opens its own connection, because
// a reused connection would measure only the payload path and would hide
// a handshake failure.
func (p *Prober) Probe(ctx context.Context, t probe.Target, pr probe.Params) (probe.Cycle, error) {
	c := probe.Cycle{Kind: probe.KindTCP, Target: t, StartedAt: time.Now()}
	addr := t.Addr()

	for i := 0; i < pr.Count; i++ {
		if ctx.Err() != nil {
			break
		}
		c.Sent++

		var (
			connect, rtt time.Duration
			err          error
		)
		if pr.Mode == "echo" {
			connect, rtt, err = p.echoOnce(ctx, addr, pr)
		} else {
			connect, err = p.connectOnce(ctx, addr, pr.Timeout)
			rtt = connect
		}

		if err != nil {
			c.Lost++
			if c.Err == nil {
				c.Err = err
				c.ErrClass = probe.Classify(err)
			}
		} else {
			c.Received++
			c.RTT = append(c.RTT, rtt)
			c.ConnectRTT = append(c.ConnectRTT, connect)
		}

		if i < pr.Count-1 && pr.Interval > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(pr.Interval):
			}
		}
	}
	return c, nil
}

// connectOnce dials and measures the handshake, then closes
// immediately. No payload is sent, so PayloadBytes is ignored in
// connect mode.
func (p *Prober) connectOnce(ctx context.Context, addr string, timeout time.Duration) (time.Duration, error) {
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	conn, err := p.dialer.DialContext(dctx, "tcp", addr)
	if err != nil {
		return 0, err
	}
	elapsed := time.Since(start)
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = conn.Close()
	return elapsed, nil
}

// echoOnce dials, sends PayloadBytes bytes, reads the same count back,
// and returns the handshake time and the payload round trip separately.
func (p *Prober) echoOnce(ctx context.Context, addr string, pr probe.Params) (time.Duration, time.Duration, error) {
	dctx, cancel := context.WithTimeout(ctx, pr.Timeout)
	defer cancel()

	start := time.Now()
	conn, err := p.dialer.DialContext(dctx, "tcp", addr)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = conn.Close() }()
	connect := time.Since(start)

	deadline, ok := dctx.Deadline()
	if !ok {
		deadline = time.Now().Add(pr.Timeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return connect, 0, err
	}

	n := pr.PayloadBytes
	if n < probe.HeaderSize {
		n = probe.HeaderSize
	}
	buf := make([]byte, n)
	h := probe.Header{Seq: 1, SendNanos: time.Now().UnixNano()}
	if err := probe.EncodeHeader(buf, h, n); err != nil {
		return connect, 0, err
	}

	sendAt := time.Now()
	if _, err := conn.Write(buf); err != nil {
		return connect, 0, err
	}
	reply := make([]byte, n)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return connect, 0, err
	}
	rtt := time.Since(sendAt)

	if _, err := probe.DecodeHeader(reply); err != nil {
		return connect, 0, err
	}
	return connect, rtt, nil
}

