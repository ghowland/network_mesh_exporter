// Package icmp is the client side of the meshping protocol. meshd holds
// no raw socket of its own; all ICMP work is multiplexed through one
// long-lived child process.
package icmp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/probe"
	"github.com/example/mesh/pkg/pingproto"
)

// ErrUnavailable reports that the helper is not running or has no ICMP
// permission.
var ErrUnavailable = errors.New("icmp: meshping is not available")

type waiter struct {
	ch   chan pingproto.Result
	once sync.Once
}

func (w *waiter) deliver(r pingproto.Result) {
	w.once.Do(func() { w.ch <- r })
}

// Client owns the meshping child process.
type Client struct {
	cfg config.MeshPingConfig

	mu      sync.Mutex
	enc     *pingproto.Encoder
	waiting map[string]*waiter
	hello   pingproto.Hello

	avail    atomic.Bool
	restarts atomic.Uint64
	seq      atomic.Uint64
	fatal    atomic.Bool
}

// New creates a Client. It does not start the child process.
func New(cfg config.MeshPingConfig) *Client {
	return &Client{cfg: cfg, waiting: make(map[string]*waiter)}
}

func (c *Client) Kind() probe.Kind { return probe.KindICMP }

// Available reports whether ICMP probing is possible now.
func (c *Client) Available() bool { return c.avail.Load() }

// Priv returns the permission mode reported by the child.
func (c *Client) Priv() pingproto.PrivMode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hello.Priv
}

// Restarts returns the child restart count for the metrics.
func (c *Client) Restarts() uint64 { return c.restarts.Load() }

// Run supervises the child process until ctx is cancelled. When the
// child reports that it could not obtain ICMP permission, Run stops
// restarting it, logs the reason once, and returns nil, so that the TCP
// and UDP probes continue without ICMP.
func (c *Client) Run(ctx context.Context) error {
	attempt := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := c.session(ctx)
		c.avail.Store(false)
		c.failAllWaiters()

		if ctx.Err() != nil {
			return nil
		}
		if c.fatal.Load() {
			slog.Error("icmp probing disabled; meshping has no permission",
				"path", c.cfg.Path)
			return nil
		}
		attempt++
		c.restarts.Add(1)
		delay := backoff(attempt, c.cfg.RestartBackoffMin.D(), c.cfg.RestartBackoffMax.D())
		slog.Warn("meshping exited, restarting", "error", err, "in", delay)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

// session runs one child process from start to exit.
func (c *Client) session(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, c.cfg.Path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", c.cfg.Path, err)
	}
	go c.stderrLoop(stderr)

	c.mu.Lock()
	c.enc = pingproto.NewEncoder(stdin)
	c.mu.Unlock()

	dec := pingproto.NewDecoder(stdout, pingproto.DefaultMaxLine)

	helloTimeout := c.cfg.HelloTimeout.D()
	if helloTimeout <= 0 {
		helloTimeout = 5 * time.Second
	}
	helloCh := make(chan error, 1)
	go func() { helloCh <- c.readLoop(dec) }()

	select {
	case <-time.After(helloTimeout):
		if !c.avail.Load() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return errors.New("meshping did not send hello in time")
		}
	case err := <-helloCh:
		_ = cmd.Wait()
		return err
	}

	err = <-helloCh
	_ = cmd.Wait()
	return err
}

// readLoop consumes the child stdout and delivers results by ID.
func (c *Client) readLoop(dec *pingproto.Decoder) error {
	for {
		raw, env, err := dec.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		switch env.Type {
		case pingproto.MsgHello:
			var h pingproto.Hello
			if err := json.Unmarshal(raw, &h); err != nil {
				return err
			}
			c.mu.Lock()
			c.hello = h
			c.mu.Unlock()
			if h.Priv == pingproto.PrivNone {
				c.fatal.Store(true)
				return fmt.Errorf("meshping has no icmp permission: %s", h.Reason)
			}
			if h.Version != pingproto.ProtocolVersion {
				c.fatal.Store(true)
				return fmt.Errorf("meshping protocol version %s is not %s",
					h.Version, pingproto.ProtocolVersion)
			}
			c.avail.Store(true)
			slog.Info("meshping ready", "priv", h.Priv, "pid", h.PID,
				"ipv4", h.IPv4, "ipv6", h.IPv6)

		case pingproto.MsgResult:
			var r pingproto.Result
			if err := json.Unmarshal(raw, &r); err != nil {
				continue
			}
			c.mu.Lock()
			w := c.waiting[r.ID]
			delete(c.waiting, r.ID)
			c.mu.Unlock()
			if w != nil {
				w.deliver(r)
			}

		case pingproto.MsgError:
			var e pingproto.Error
			if err := json.Unmarshal(raw, &e); err != nil {
				continue
			}
			c.mu.Lock()
			w := c.waiting[e.ID]
			delete(c.waiting, e.ID)
			c.mu.Unlock()
			if w != nil {
				w.deliver(pingproto.Result{
					Type: pingproto.MsgResult, ID: e.ID,
					Error: e.Message, ErrorClass: e.ErrorClass,
				})
			}
		}
	}
}

// stderrLoop copies child diagnostics into the log.
func (c *Client) stderrLoop(r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		slog.Warn("meshping", "message", sc.Text())
	}
}

// failAllWaiters releases every pending request when the child exits, so
// that no probe goroutine blocks on a process that is gone.
func (c *Client) failAllWaiters() {
	c.mu.Lock()
	waiting := c.waiting
	c.waiting = make(map[string]*waiter)
	c.mu.Unlock()
	for id, w := range waiting {
		w.deliver(pingproto.Result{
			Type: pingproto.MsgResult, ID: id,
			Error: "meshping exited", ErrorClass: "internal",
		})
	}
}

// Probe sends one request and waits for the matching result. It returns
// ErrUnavailable immediately when the child is not running, so that a
// missing helper does not stall the runner.
func (c *Client) Probe(ctx context.Context, t probe.Target, pr probe.Params) (probe.Cycle, error) {
	cyc := probe.Cycle{Kind: probe.KindICMP, Target: t, StartedAt: time.Now()}
	if !c.avail.Load() {
		cyc.Err = ErrUnavailable
		cyc.ErrClass = "permission"
		return cyc, nil
	}

	id := fmt.Sprintf("%d", c.seq.Add(1))
	req := pingproto.Request{
		Type:         pingproto.MsgPing,
		ID:           id,
		Target:       t.Address,
		Count:        pr.Count,
		IntervalMS:   int(pr.Interval / time.Millisecond),
		PayloadBytes: pr.PayloadBytes,
		TimeoutMS:    int(pr.Timeout / time.Millisecond),
		TTL:          pr.TTL,
		DF:           pr.DF,
	}

	w := &waiter{ch: make(chan pingproto.Result, 1)}
	c.mu.Lock()
	enc := c.enc
	if enc == nil {
		c.mu.Unlock()
		cyc.Err = ErrUnavailable
		cyc.ErrClass = "internal"
		return cyc, nil
	}
	c.waiting[id] = w
	c.mu.Unlock()

	if err := enc.Encode(req); err != nil {
		c.mu.Lock()
		delete(c.waiting, id)
		c.mu.Unlock()
		cyc.Err = err
		cyc.ErrClass = probe.Classify(err)
		return cyc, nil
	}

	// The abandon bound is the worst-case work time plus a margin, so a
	// lost result never leaks a goroutine or a map entry.
	limit := time.Duration(pr.Count)*(pr.Interval+pr.Timeout) + 5*time.Second

	select {
	case <-ctx.Done():
		c.abandon(id)
		cyc.Err = ctx.Err()
		cyc.ErrClass = "timeout"
		return cyc, nil
	case <-time.After(limit):
		c.abandon(id)
		cyc.Err = errors.New("meshping result did not arrive")
		cyc.ErrClass = "timeout"
		return cyc, nil
	case res := <-w.ch:
		cyc.Sent = res.Sent
		cyc.Received = res.Received
		cyc.Lost = res.Lost
		cyc.Reordered = res.Reordered
		for _, us := range res.RTTMicros {
			cyc.RTT = append(cyc.RTT, time.Duration(us)*time.Microsecond)
		}
		if res.Error != "" {
			cyc.Err = errors.New(res.Error)
			cyc.ErrClass = res.ErrorClass
		}
		return cyc, nil
	}
}

// abandon drops a request whose result never arrived and asks the child
// to stop the work.
func (c *Client) abandon(id string) {
	c.mu.Lock()
	delete(c.waiting, id)
	enc := c.enc
	c.mu.Unlock()
	if enc != nil {
		_ = enc.Encode(pingproto.Cancel{Type: pingproto.MsgCancel, ID: id})
	}
}

func backoff(attempt int, min, max time.Duration) time.Duration {
	if min <= 0 {
		min = time.Second
	}
	if max <= 0 {
		max = 30 * time.Second
	}
	d := min
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

