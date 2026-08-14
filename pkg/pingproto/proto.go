// Package pingproto defines the line-delimited JSON protocol spoken
// between meshd and the privileged meshping helper. It is the only
// package imported by both binaries and depends on the standard library
// only.
package pingproto

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ProtocolVersion is compared between the two binaries at start.
const ProtocolVersion = "1"

// DefaultMaxLine bounds one protocol line.
const DefaultMaxLine = 1 << 20

// MsgType identifies the kind of message.
type MsgType string

const (
	MsgHello    MsgType = "hello"
	MsgPing     MsgType = "ping"
	MsgResult   MsgType = "result"
	MsgCancel   MsgType = "cancel"
	MsgError    MsgType = "error"
	MsgShutdown MsgType = "shutdown"
)

// PrivMode reports how meshping obtained permission to send ICMP.
type PrivMode string

const (
	PrivRaw      PrivMode = "raw"
	PrivDatagram PrivMode = "datagram"
	PrivNone     PrivMode = "none"
)

// ErrClass is a stable classification of a probe failure. The value
// becomes a Prometheus label, so the set must stay small.
type ErrClass string

const (
	ErrNone        ErrClass = ""
	ErrTimeout     ErrClass = "timeout"
	ErrResolve     ErrClass = "resolve"
	ErrUnreachable ErrClass = "unreachable"
	ErrRefused     ErrClass = "refused"
	ErrPermission  ErrClass = "permission"
	ErrInternal    ErrClass = "internal"
)

// Hello is sent once by meshping at start. meshd waits for this message
// before it sends any request.
type Hello struct {
	Type    MsgType  `json:"type"`
	Version string   `json:"version"`
	Priv    PrivMode `json:"priv"`
	IPv4    bool     `json:"ipv4"`
	IPv6    bool     `json:"ipv6"`
	PID     int      `json:"pid"`
	Reason  string   `json:"reason"`
}

// Request is one unit of ICMP work. It is self-contained: meshping holds
// no configuration and no defaults of its own.
type Request struct {
	Type         MsgType `json:"type"`
	ID           string  `json:"id"`
	Target       string  `json:"target"`
	Count        int     `json:"count"`
	IntervalMS   int     `json:"interval_ms"`
	PayloadBytes int     `json:"payload_bytes"`
	TimeoutMS    int     `json:"timeout_ms"`
	TTL          int     `json:"ttl"`
	DF           bool    `json:"df"`
	IPv6         bool    `json:"ipv6"`
}

// Result carries raw samples only. All aggregation happens in meshd,
// which keeps the privileged program as small as possible.
type Result struct {
	Type       MsgType  `json:"type"`
	ID         string   `json:"id"`
	Target     string   `json:"target"`
	Resolved   string   `json:"resolved"`
	Sent       int      `json:"sent"`
	Received   int      `json:"received"`
	Lost       int      `json:"lost"`
	Reordered  int      `json:"reordered"`
	RTTMicros  []int64  `json:"rtt_us"`
	Error      string   `json:"error"`
	ErrorClass ErrClass `json:"error_class"`
}

// Cancel stops an in-flight request. A Result is still emitted with the
// samples collected so far.
type Cancel struct {
	Type MsgType `json:"type"`
	ID   string  `json:"id"`
}

// Error reports that a request could not be started at all. A request
// that starts and then fails produces a Result instead.
type Error struct {
	Type       MsgType  `json:"type"`
	ID         string   `json:"id"`
	Message    string   `json:"message"`
	ErrorClass ErrClass `json:"error_class"`
}

// Shutdown asks meshping to finish current work and exit.
type Shutdown struct {
	Type MsgType `json:"type"`
}

// Envelope is used to read the type field before the full decode.
type Envelope struct {
	Type MsgType `json:"type"`
	ID   string  `json:"id"`
}

// ErrLineTooLong reports a protocol line above the configured bound.
var ErrLineTooLong = errors.New("pingproto: line exceeds maximum length")

// Encoder writes line-delimited JSON. It is safe for concurrent use.
type Encoder struct {
	mu sync.Mutex
	w  *bufio.Writer
}

// NewEncoder returns an Encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: bufio.NewWriter(w)}
}

// Encode writes one value followed by a newline and flushes.
func (e *Encoder) Encode(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.w.Write(b); err != nil {
		return err
	}
	if err := e.w.WriteByte('\n'); err != nil {
		return err
	}
	return e.w.Flush()
}

// Decoder reads line-delimited JSON with a bounded line length.
type Decoder struct {
	sc *bufio.Scanner
}

// NewDecoder returns a Decoder that reads from r. A line longer than
// maxLine produces ErrLineTooLong.
func NewDecoder(r io.Reader, maxLine int) *Decoder {
	if maxLine <= 0 {
		maxLine = DefaultMaxLine
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	return &Decoder{sc: sc}
}

// Next reads one line and returns the raw bytes and the decoded
// envelope. It returns io.EOF at the end of the stream.
func (d *Decoder) Next() ([]byte, Envelope, error) {
	for {
		if !d.sc.Scan() {
			if err := d.sc.Err(); err != nil {
				if errors.Is(err, bufio.ErrTooLong) {
					return nil, Envelope{}, ErrLineTooLong
				}
				return nil, Envelope{}, err
			}
			return nil, Envelope{}, io.EOF
		}
		line := d.sc.Bytes()
		if len(line) == 0 {
			continue
		}
		raw := make([]byte, len(line))
		copy(raw, line)
		var env Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return raw, Envelope{}, fmt.Errorf("pingproto: decode envelope: %w", err)
		}
		return raw, env, nil
	}
}

// Validate checks a Request for values meshping cannot honour. It
// applies no defaults; meshd is responsible for complete requests.
func (r *Request) Validate() error {
	if r.ID == "" {
		return errors.New("pingproto: request id is empty")
	}
	if r.Target == "" {
		return errors.New("pingproto: request target is empty")
	}
	if r.Count <= 0 || r.Count > 1000 {
		return fmt.Errorf("pingproto: count %d out of range 1..1000", r.Count)
	}
	if r.IntervalMS < 0 || r.IntervalMS > 60000 {
		return fmt.Errorf("pingproto: interval_ms %d out of range", r.IntervalMS)
	}
	if r.TimeoutMS <= 0 || r.TimeoutMS > 60000 {
		return fmt.Errorf("pingproto: timeout_ms %d out of range", r.TimeoutMS)
	}
	if r.PayloadBytes < 8 || r.PayloadBytes > 65000 {
		return fmt.Errorf("pingproto: payload_bytes %d out of range 8..65000", r.PayloadBytes)
	}
	if r.TTL < 0 || r.TTL > 255 {
		return fmt.Errorf("pingproto: ttl %d out of range", r.TTL)
	}
	return nil
}

