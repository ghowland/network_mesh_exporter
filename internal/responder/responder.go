// Package responder runs the UDP and TCP echo listeners that remote
// probers need. Without it, a node can measure outward but cannot be
// measured, and the reverse direction of every slot would be blank.
package responder

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/probe"
)

// Stats reports responder activity for the metrics.
type Stats struct {
	UDPPackets  uint64 `json:"udp_packets"`
	UDPRejected uint64 `json:"udp_rejected"`
	TCPConns    uint64 `json:"tcp_conns"`
	TCPRejected uint64 `json:"tcp_rejected"`
}

// Server runs both listeners.
type Server struct {
	cfg config.ResponderConfig

	udpPackets  atomic.Uint64
	udpRejected atomic.Uint64
	tcpConns    atomic.Uint64
	tcpRejected atomic.Uint64
}

// New creates a Server from the configuration.
func New(cfg config.ResponderConfig) *Server { return &Server{cfg: cfg} }

// Run starts both listeners and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	if !s.cfg.Enabled {
		return nil
	}

	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	record := func(err error) {
		mu.Lock()
		if firstErr == nil && err != nil {
			firstErr = err
		}
		mu.Unlock()
	}

	if s.cfg.UDPListen != "" {
		pc, err := net.ListenPacket("udp", s.cfg.UDPListen)
		if err != nil {
			return err
		}
		slog.Info("udp responder listening", "addr", s.cfg.UDPListen)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
			_ = pc.Close()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			record(s.serveUDP(ctx, pc))
		}()
	}

	if s.cfg.TCPListen != "" {
		l, err := net.Listen("tcp", s.cfg.TCPListen)
		if err != nil {
			return err
		}
		slog.Info("tcp responder listening", "addr", s.cfg.TCPListen)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
			_ = l.Close()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			record(s.serveTCP(ctx, l))
		}()
	}

	wg.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return firstErr
}

// serveUDP reads packets, verifies the magic value, stamps the receive
// time into the header, and returns the payload otherwise unchanged.
func (s *Server) serveUDP(ctx context.Context, pc net.PacketConn) error {
	buf := make([]byte, 65535)
	for {
		if ctx.Err() != nil {
			return nil
		}
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		h, err := probe.DecodeHeader(buf[:n])
		if err != nil {
			s.udpRejected.Add(1)
			continue
		}
		// The payload is echoed at its original size, so the reply path
		// carries the same packet size as the request path.
		reply := make([]byte, n)
		copy(reply, buf[:n])
		stamp(reply, h)
		if _, err := pc.WriteTo(reply, addr); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		s.udpPackets.Add(1)
	}
}

// serveTCP accepts connections and echoes each payload.
func (s *Server) serveTCP(ctx context.Context, l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		s.tcpConns.Add(1)
		go s.handleConn(ctx, conn)
	}
}

// handleConn echoes one TCP connection until it closes or the read
// deadline expires. The deadline bounds an idle connection so that a
// connect-mode prober, which never sends a payload, does not hold a
// goroutine.
func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()

	buf := make([]byte, 65535)
	for {
		if ctx.Err() != nil {
			return
		}
		if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return
		}
		n, err := conn.Read(buf)
		if n > 0 {
			h, derr := probe.DecodeHeader(buf[:n])
			if derr != nil {
				s.tcpRejected.Add(1)
				return
			}
			reply := make([]byte, n)
			copy(reply, buf[:n])
			stamp(reply, h)
			if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				return
			}
			if _, werr := conn.Write(reply); werr != nil {
				return
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			return
		}
	}
}

// stamp overwrites the send timestamp with the receive timestamp, which
// makes one-way delay recoverable later without a second protocol.
func stamp(buf []byte, h probe.Header) {
	rh := probe.Header{Seq: h.Seq, SendNanos: time.Now().UnixNano()}
	_ = probe.EncodeHeaderPrefix(buf, rh)
}

// Stats reports responder activity.
func (s *Server) Stats() Stats {
	return Stats{
		UDPPackets:  s.udpPackets.Load(),
		UDPRejected: s.udpRejected.Load(),
		TCPConns:    s.tcpConns.Load(),
		TCPRejected: s.tcpRejected.Load(),
	}
}

