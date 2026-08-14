// Package pairing builds the set of zone pairings from a zone index. A
// pairing is defined by the zone rule and the filters, not by the hosts.
// Hosts are slotted into a pairing afterwards, which is what lets the
// pairing set stay stable while hosts come and go.
package pairing

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/zone"
)

// Separator joins the two zone keys of a pairing key.
const Separator = "|"

// Key identifies one zone pairing. It is the two zone keys sorted
// alphabetically and joined with a vertical bar, so that the key is the
// same on every node regardless of which side observed it.
type Key string

// MakeKey builds a Key from two zone keys in any order.
func MakeKey(a, b zone.Key) Key {
	if a <= b {
		return Key(string(a) + Separator + string(b))
	}
	return Key(string(b) + Separator + string(a))
}

// Split returns the two zone keys of a Key.
func (k Key) Split() (zone.Key, zone.Key) {
	s := string(k)
	i := strings.Index(s, Separator)
	if i < 0 {
		return zone.Key(s), zone.Key(s)
	}
	return zone.Key(s[:i]), zone.Key(s[i+len(Separator):])
}

// Intra reports whether both sides of the key are the same zone.
func (k Key) Intra() bool {
	a, b := k.Split()
	return a == b
}

// Pairing is one desired pairing before slots are assigned.
type Pairing struct {
	Key   Key
	ZoneA zone.Key
	ZoneB zone.Key
	Intra bool
}

// ErrTooManyPairings is returned when the desired count exceeds
// MaxPairings. The reconcile keeps the previous state in that case, so
// that a wrong zone rule cannot replace a working topology with a very
// large one.
var ErrTooManyPairings = errors.New("pairing: desired count exceeds max_pairings")

// Filter selects which pairings are wanted. Patterns are matched against
// the pairing key with path.Match semantics.
type Filter struct {
	include []string
	exclude []string
}

// NewFilter compiles the include and exclude patterns. An invalid
// pattern is rejected here rather than silently matching nothing.
func NewFilter(cfg config.PairingsConfig) (*Filter, error) {
	f := &Filter{}
	for i, p := range cfg.Include {
		if _, err := path.Match(p, "x"); err != nil {
			return nil, fmt.Errorf("pairing: include[%d] %q: %w", i, p, err)
		}
		f.include = append(f.include, p)
	}
	for i, p := range cfg.Exclude {
		if _, err := path.Match(p, "x"); err != nil {
			return nil, fmt.Errorf("pairing: exclude[%d] %q: %w", i, p, err)
		}
		f.exclude = append(f.exclude, p)
	}
	return f, nil
}

// Match reports whether a pairing key is wanted. When the include list
// is not empty, a key must match it before the exclude list is applied.
func (f *Filter) Match(k Key) bool {
	if f == nil {
		return true
	}
	s := string(k)
	if len(f.include) > 0 {
		hit := false
		for _, p := range f.include {
			if ok, _ := path.Match(p, s); ok {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	for _, p := range f.exclude {
		if ok, _ := path.Match(p, s); ok {
			return false
		}
	}
	return true
}

// Count returns the pairing count for n zones without building the set.
// The caller uses this to check the limit before allocation.
func Count(zones int, intra bool) int {
	if zones < 0 {
		return 0
	}
	n := zones * (zones - 1) / 2
	if intra {
		n += zones
	}
	return n
}

// Build returns every wanted pairing from the zone index, in sorted
// order. The limit is checked before the set is built, so a wrong zone
// rule cannot cause a very large allocation.
func Build(idx *zone.Index, cfg config.PairingsConfig, f *Filter) ([]Pairing, error) {
	zones := idx.Zones
	if n := Count(len(zones), cfg.IntraZone); cfg.MaxPairings > 0 && n > cfg.MaxPairings {
		return nil, fmt.Errorf("%w: %d desired, %d allowed, %d zones",
			ErrTooManyPairings, n, cfg.MaxPairings, len(zones))
	}

	out := make([]Pairing, 0, Count(len(zones), cfg.IntraZone))
	for i := 0; i < len(zones); i++ {
		if cfg.IntraZone {
			// An intra-zone pairing needs two distinct hosts, so a zone
			// with one member cannot produce one.
			if idx.MemberCount(zones[i]) >= 2 {
				k := MakeKey(zones[i], zones[i])
				if f.Match(k) {
					out = append(out, Pairing{
						Key: k, ZoneA: zones[i], ZoneB: zones[i], Intra: true,
					})
				}
			}
		}
		for j := i + 1; j < len(zones); j++ {
			k := MakeKey(zones[i], zones[j])
			if !f.Match(k) {
				continue
			}
			a, b := k.Split()
			out = append(out, Pairing{Key: k, ZoneA: a, ZoneB: b})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	if cfg.MaxPairings > 0 && len(out) > cfg.MaxPairings {
		return nil, fmt.Errorf("%w: %d after filters, %d allowed",
			ErrTooManyPairings, len(out), cfg.MaxPairings)
	}
	return out, nil
}

