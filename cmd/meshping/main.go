// Command meshping is the privileged ICMP helper. It holds no
// configuration, performs no discovery, and computes no statistics. It
// reads Requests as line-delimited JSON on stdin and writes Results on
// stdout. Diagnostics go to stderr as plain text.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"

	"github.com/example/mesh/pkg/pingproto"
)

// Version is set at build time.
var Version = "dev"

// magic marks a payload as belonging to this system, so that an
// unrelated echo reply is not counted as a sample.
var magic = [4]byte{0x6d, 0x65, 0x73, 0x68}

// sample is one sent packet awaiting a reply.
type sample struct {
	seq      uint16
	jobID    string
	sentAt   time.Time
	recvAt   time.Time
	received bool
	order    int
}

// job is one in-flight Request.
type job struct {
	req    pingproto.Request
	cancel context.CancelFunc
	dst    net.Addr
	ipv6   bool
	sent   []*sample
}

// pinger owns the ICMP sockets and dispatches requests.
type pinger struct {
	v4    *icmp.PacketConn
	v6    *icmp.PacketConn
	priv  pingproto.PrivMode
	ident int
	seq   atomic.Uint32
	enc   *pingproto.Encoder

	mu       sync.Mutex
	inflight map[string]*job
	pending  map[uint16]*sample

	wg     sync.WaitGroup
	closed atomic.Bool
}

// func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func main() {
	enc := pingproto.NewEncoder(os.Stdout)

	v4, v6, mode, err := openSockets()
	if err != nil || mode == pingproto.PrivNone {
		reason := "no icmp permission"
		if err != nil {
			reason = err.Error()
		}
		_ = enc.Encode(pingproto.Hello{
			Type:    pingproto.MsgHello,
			Version: pingproto.ProtocolVersion,
			Priv:    pingproto.PrivNone,
			PID:     os.Getpid(),
			Reason:  reason,
		})
		fmt.Fprintf(os.Stderr, "meshping: %s\n", reason)
		os.Exit(3)
	}

	if err := dropPrivileges(); err != nil {
		fmt.Fprintf(os.Stderr, "meshping: drop privileges: %v\n", err)
	}

	p := newPinger(v4, v6, mode, enc)

	if err := enc.Encode(pingproto.Hello{
		Type:    pingproto.MsgHello,
		Version: pingproto.ProtocolVersion,
		Priv:    mode,
		IPv4:    v4 != nil,
		IPv6:    v6 != nil,
		PID:     os.Getpid(),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "meshping: hello: %v\n", err)
		os.Exit(4)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if v4 != nil {
		go p.recvLoop(ctx, v4, false)
	}
	if v6 != nil {
		go p.recvLoop(ctx, v6, true)
	}

	dec := pingproto.NewDecoder(os.Stdin, pingproto.DefaultMaxLine)
	if err := p.readLoop(ctx, dec); err != nil {
		fmt.Fprintf(os.Stderr, "meshping: read loop: %v\n", err)
	}
	cancel()
	p.shutdown(5 * time.Second)
}

// openSockets tries an unprivileged ICMP datagram socket first and falls
// back to a raw socket. It reports the mode actually obtained.
func openSockets() (*icmp.PacketConn, *icmp.PacketConn, pingproto.PrivMode, error) {
	if c4, err4 := icmp.ListenPacket("udp4", "0.0.0.0"); err4 == nil {
		c6, _ := icmp.ListenPacket("udp6", "::")
		return c4, c6, pingproto.PrivDatagram, nil
	}
	c4, err4 := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err4 != nil {
		return nil, nil, pingproto.PrivNone, err4
	}
	c6, _ := icmp.ListenPacket("ip6:ipv6-icmp", "::")
	return c4, c6, pingproto.PrivRaw, nil
}

// dropPrivileges lowers the effective user to the real user once the
// sockets are open. It is a no-op when the program is not setuid.
func dropPrivileges() error {
	if os.Geteuid() == os.Getuid() && os.Getegid() == os.Getgid() {
		return nil
	}
	// syscall.Setuid and syscall.Setgid are not available on all
	// platforms, and on Linux the Go runtime requires the process-wide
	// variants in the unix package. The capability path (setcap
	// cap_net_raw+ep) is the supported way to run this program, and it
	// needs no privilege drop because no privilege was ever elevated.
	return errors.New("setuid mode is not supported; use setcap cap_net_raw+ep")
}

// newPinger builds a pinger over already-open sockets.
func newPinger(v4, v6 *icmp.PacketConn, mode pingproto.PrivMode, enc *pingproto.Encoder) *pinger {
	return &pinger{
		v4:       v4,
		v6:       v6,
		priv:     mode,
		ident:    os.Getpid() & 0xffff,
		enc:      enc,
		inflight: make(map[string]*job),
		pending:  make(map[uint16]*sample),
	}
}

// readLoop consumes stdin and dispatches each message until the stream
// ends or a shutdown message arrives.
func (p *pinger) readLoop(ctx context.Context, dec *pingproto.Decoder) error {
	for {
		raw, env, err := dec.Next()
		if err != nil {
			if errors.Is(err, os.ErrClosed) {
				return nil
			}
			return err
		}
		switch env.Type {
		case pingproto.MsgPing:
			var req pingproto.Request
			if err := jsonUnmarshal(raw, &req); err != nil {
				p.emitError(env.ID, err, pingproto.ErrInternal)
				continue
			}
			if err := req.Validate(); err != nil {
				p.emitError(req.ID, err, pingproto.ErrInternal)
				continue
			}
			if err := p.start(ctx, req); err != nil {
				p.emitError(req.ID, err, classify(err))
			}
		case pingproto.MsgCancel:
			p.cancel(env.ID)
		case pingproto.MsgShutdown:
			return nil
		default:
			fmt.Fprintf(os.Stderr, "meshping: unknown message type %q\n", env.Type)
		}
	}
}

// start begins one request in its own goroutine. It returns an error
// only when the request cannot start at all.
func (p *pinger) start(ctx context.Context, req pingproto.Request) error {
	dst, isV6, err := resolve(req.Target, req.IPv6, p.priv)
	if err != nil {
		return err
	}
	if isV6 && p.v6 == nil {
		return errors.New("ipv6 socket unavailable")
	}
	if !isV6 && p.v4 == nil {
		return errors.New("ipv4 socket unavailable")
	}

	jctx, cancel := context.WithCancel(ctx)
	j := &job{req: req, cancel: cancel, dst: dst, ipv6: isV6}

	p.mu.Lock()
	if _, dup := p.inflight[req.ID]; dup {
		p.mu.Unlock()
		cancel()
		return fmt.Errorf("duplicate request id %s", req.ID)
	}
	p.inflight[req.ID] = j
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run(jctx, j)
	}()
	return nil
}

// run sends Count packets at IntervalMS, waits out the final timeout,
// and emits exactly one Result.
func (p *pinger) run(ctx context.Context, j *job) {
	defer j.cancel()

	conn := p.v4
	if j.ipv6 {
		conn = p.v6
	}
	p.applyTTL(conn, j)

	interval := time.Duration(j.req.IntervalMS) * time.Millisecond
	timeout := time.Duration(j.req.TimeoutMS) * time.Millisecond

	var firstErr error
	var firstClass pingproto.ErrClass

	for i := 0; i < j.req.Count; i++ {
		if ctx.Err() != nil {
			break
		}
		seq := uint16(p.seq.Add(1))
		s := &sample{seq: seq, jobID: j.req.ID, order: i}

		p.mu.Lock()
		p.pending[seq] = s
		p.mu.Unlock()
		j.sent = append(j.sent, s)

		msg := p.buildMessage(j, seq)
		wire, err := msg.Marshal(nil)
		if err != nil {
			if firstErr == nil {
				firstErr, firstClass = err, pingproto.ErrInternal
			}
			continue
		}
		s.sentAt = time.Now()
		if _, err := conn.WriteTo(wire, j.dst); err != nil {
			if firstErr == nil {
				firstErr, firstClass = err, classify(err)
			}
		}
		if i < j.req.Count-1 && interval > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(interval):
			}
		}
	}

	// Wait out the reply timeout of the final packet.
	select {
	case <-ctx.Done():
	case <-time.After(timeout):
	}

	p.mu.Lock()
	for _, s := range j.sent {
		delete(p.pending, s.seq)
	}
	delete(p.inflight, j.req.ID)
	p.mu.Unlock()

	res := collect(j)
	if firstErr != nil && res.Received == 0 {
		res.Error = firstErr.Error()
		res.ErrorClass = firstClass
	} else if res.Received == 0 && res.Sent > 0 {
		res.Error = "no replies"
		res.ErrorClass = pingproto.ErrTimeout
	}
	if err := p.enc.Encode(res); err != nil {
		fmt.Fprintf(os.Stderr, "meshping: encode result: %v\n", err)
	}
}

// buildMessage constructs the echo request for one sequence number.
func (p *pinger) buildMessage(j *job, seq uint16) *icmp.Message {
	body := &icmp.Echo{
		ID:   p.ident,
		Seq:  int(seq),
		Data: buildPayload(j.req.PayloadBytes, seq),
	}
	if j.ipv6 {
		return &icmp.Message{Type: ipv6.ICMPTypeEchoRequest, Code: 0, Body: body}
	}
	return &icmp.Message{Type: ipv4.ICMPTypeEcho, Code: 0, Body: body}
}

// applyTTL sets the hop limit when the request asks for one. The
// do-not-fragment bit is not portable through this socket layer and is
// therefore not applied; the field is accepted and ignored.
func (p *pinger) applyTTL(c *icmp.PacketConn, j *job) {
	if j.req.TTL <= 0 {
		return
	}
	if j.ipv6 {
		_ = c.IPv6PacketConn().SetHopLimit(j.req.TTL)
		return
	}
	_ = c.IPv4PacketConn().SetTTL(j.req.TTL)
}

// recvLoop reads replies from one socket and matches them to pending
// samples by sequence number. Matching does not use the ICMP identifier,
// because the kernel rewrites it on an unprivileged datagram socket.
func (p *pinger) recvLoop(ctx context.Context, c *icmp.PacketConn, isV6 bool) {
	buf := make([]byte, 65535)
	proto := 1
	if isV6 {
		proto = 58
	}
	for {
		if ctx.Err() != nil || p.closed.Load() {
			return
		}
		_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, _, err := c.ReadFrom(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return
		}
		now := time.Now()
		msg, err := icmp.ParseMessage(proto, buf[:n])
		if err != nil {
			continue
		}
		echo, ok := msg.Body.(*icmp.Echo)
		if !ok {
			continue
		}
		if msg.Type != ipv4.ICMPTypeEchoReply && msg.Type != ipv6.ICMPTypeEchoReply {
			continue
		}
		if len(echo.Data) >= 8 && !hasMagic(echo.Data) {
			continue
		}
		seq := uint16(echo.Seq)

		p.mu.Lock()
		s := p.pending[seq]
		if s != nil && !s.received {
			s.received = true
			s.recvAt = now
			delete(p.pending, seq)
		}
		p.mu.Unlock()
	}
}

// cancel stops a job by ID. The Result is still emitted by run.
func (p *pinger) cancel(id string) {
	p.mu.Lock()
	j := p.inflight[id]
	p.mu.Unlock()
	if j != nil {
		j.cancel()
	}
}

// emitError reports that a request could not be started.
func (p *pinger) emitError(id string, err error, class pingproto.ErrClass) {
	_ = p.enc.Encode(pingproto.Error{
		Type:       pingproto.MsgError,
		ID:         id,
		Message:    err.Error(),
		ErrorClass: class,
	})
}

// shutdown closes the sockets and waits for in-flight jobs, bounded by
// the grace period.
func (p *pinger) shutdown(grace time.Duration) {
	p.closed.Store(true)
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
	}
	if p.v4 != nil {
		_ = p.v4.Close()
	}
	if p.v6 != nil {
		_ = p.v6.Close()
	}
}

// collect converts finished samples into a Result. A reply that arrives
// after a later reply counts as a reorder rather than as a loss.
func collect(j *job) pingproto.Result {
	res := pingproto.Result{
		Type:     pingproto.MsgResult,
		ID:       j.req.ID,
		Target:   j.req.Target,
		Resolved: addrString(j.dst),
		Sent:     len(j.sent),
	}
	lastOrder := -1
	for _, s := range j.sent {
		if !s.received {
			res.Lost++
			continue
		}
		res.Received++
		res.RTTMicros = append(res.RTTMicros, s.recvAt.Sub(s.sentAt).Microseconds())
		if s.order < lastOrder {
			res.Reordered++
		} else {
			lastOrder = s.order
		}
	}
	return res
}

// resolve converts a target to a destination address and reports the
// address family. The address type depends on the socket mode: a
// datagram socket needs a UDPAddr, a raw socket needs an IPAddr.
func resolve(target string, forceV6 bool, mode pingproto.PrivMode) (net.Addr, bool, error) {
	network := "ip4"
	if forceV6 {
		network = "ip6"
	}
	ipAddr, err := net.ResolveIPAddr(network, target)
	if err != nil {
		if !forceV6 {
			if a6, e6 := net.ResolveIPAddr("ip6", target); e6 == nil {
				ipAddr, forceV6 = a6, true
			} else {
				return nil, false, err
			}
		} else {
			return nil, false, err
		}
	}
	isV6 := ipAddr.IP.To4() == nil
	if mode == pingproto.PrivDatagram {
		return &net.UDPAddr{IP: ipAddr.IP, Zone: ipAddr.Zone}, isV6, nil
	}
	return ipAddr, isV6, nil
}

func addrString(a net.Addr) string {
	switch v := a.(type) {
	case *net.UDPAddr:
		return v.IP.String()
	case *net.IPAddr:
		return v.IP.String()
	default:
		return a.String()
	}
}

// buildPayload returns a payload of n bytes carrying the magic value,
// the sequence number, and the send timestamp.
func buildPayload(n int, seq uint16) []byte {
	if n < 8 {
		n = 8
	}
	b := make([]byte, n)
	copy(b[0:4], magic[:])
	binary.BigEndian.PutUint16(b[4:6], seq)
	if n >= 16 {
		binary.BigEndian.PutUint64(b[8:16], uint64(time.Now().UnixNano()))
	}
	for i := 16; i < n; i++ {
		b[i] = byte(i)
	}
	return b
}

func hasMagic(b []byte) bool {
	return len(b) >= 4 && b[0] == magic[0] && b[1] == magic[1] && b[2] == magic[2] && b[3] == magic[3]
}

// classify maps a network error to a stable ErrClass label value.
func classify(err error) pingproto.ErrClass {
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
	if errors.Is(err, os.ErrPermission) {
		return pingproto.ErrPermission
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return pingproto.ErrRefused
	case strings.Contains(msg, "unreachable"):
		return pingproto.ErrUnreachable
	case strings.Contains(msg, "permission denied"),
		strings.Contains(msg, "operation not permitted"):
		return pingproto.ErrPermission
	}
	return pingproto.ErrInternal

}
