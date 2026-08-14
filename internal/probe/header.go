// internal/probe/header.go
package probe

import (
	"encoding/binary"
	"errors"
)

// EncodeHeaderPrefix overwrites only the header of an existing buffer
// and leaves the payload after it untouched. The responder uses this so
// that the echoed payload keeps its original size and content.
func EncodeHeaderPrefix(buf []byte, h Header) error {
	if len(buf) < HeaderSize {
		return errors.New("probe: buffer shorter than the header")
	}
	copy(buf[0:4], Magic[:])
	binary.BigEndian.PutUint32(buf[4:8], h.Seq)
	binary.BigEndian.PutUint64(buf[8:16], uint64(h.SendNanos))
	return nil
}
