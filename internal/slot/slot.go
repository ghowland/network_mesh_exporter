// Package slot assigns hosts to the numbered containers of a zone
// pairing. The slot index is stable and the hosts inside it are
// replaceable, which is what keeps a time series continuous while the
// underlying host set changes.
package slot

import (
	"hash/fnv"
	"math"
	"sort"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/inventory"
	"github.com/example/mesh/internal/pairing"
	"github.com/example/mesh/internal/zone"
)

// Class is the assignment policy of one slot.
type Class string

const (
	// ClassAnchor slots reuse the same host on each side across all
	// anchor slots in one pairing. The endpoints do not change, so a
	// change in the measurement is a change in the path.
	ClassAnchor Class = "anchor"

	// ClassDiverse slots prefer a host not yet used in this pairing, so
	// the measurement spreads across hosts and one bad host cannot
	// represent the pairing.
	ClassDiverse Class = "diverse"

	// ClassSuper slots pair one designated host on side A with every
	// eligible host on side B, which compares one machine against a
	// whole zone.
	ClassSuper Class = "super"
)

// Side identifies one endpoint of a slot.
type Side int

const (
	SideA Side = 0
	SideB Side = 1
)

func (s Side) String() string {
	if s == SideB {
		return "b"
	}
	return "a"
}

// Rank records how a slot side was filled. The value becomes a metric
// label, so a query can distinguish a slot with independent endpoints
// from a slot that had to reuse a host.
type Rank int

const (
	RankUnfilled Rank = 0
	RankUnique   Rank = 1 // the host is used nowhere else on this side
	RankMinUse   Rank = 2 // reused at the lowest current use count
	RankAny      Rank = 3 // reused, no better candidate existed
)

// Layout is the class composition of one pairing's slot table. Super
// slots are not part of Total; they are added on top of it.
type Layout struct {
	Total   int
	Anchors int
	Diverse int
}

// ComputeLayout derives the class composition from the configuration.
func ComputeLayout(cfg config.SlotsConfig) Layout {
	total := cfg.Count
	if total < 0 {
		total = 0
	}
	raw := float64(total) * cfg.AnchorRatio
	var anchors int
	if cfg.AnchorRounding == "down" {
		anchors = int(math.Floor(raw))
	} else {
		anchors = int(math.Ceil(raw))
	}
	if anchors > total {
		anchors = total
	}
	if anchors < 0 {
		anchors = 0
	}
	// A non-zero ratio must produce at least one anchor slot, otherwise
	// the constant-endpoint measurement disappears at small slot counts.
	if anchors == 0 && cfg.AnchorRatio > 0 && total > 0 {
		anchors = 1
	}
	return Layout{Total: total, Anchors: anchors, Diverse: total - anchors}
}

// ClassAt returns the class of one slot index under a layout. Anchor
// slots occupy the low indices, so raising the slot count adds diverse
// slots at the end and leaves the anchor indices untouched.
func (l Layout) ClassAt(i int) Class {
	if i < l.Anchors {
		return ClassAnchor
	}
	return ClassDiverse
}

// Candidates is the eligible host list of one zone side, in canonical
// order, together with the use counts inside the current pairing.
type Candidates struct {
	Zone     zone.Key
	Hosts    []string
	UseCount map[string]int
}

// NewCandidates builds the candidate list for one side of one pairing.
// The member list is already in canonical order, and that order is
// preserved, because every deterministic scan depends on it.
func NewCandidates(z zone.Key, members []string, eligible func(string) bool) *Candidates {
	c := &Candidates{Zone: z, UseCount: make(map[string]int)}
	c.Hosts = make([]string, 0, len(members))
	for _, id := range members {
		if eligible == nil || eligible(id) {
			c.Hosts = append(c.Hosts, id)
		}
	}
	return c
}

// Len returns the eligible host count.
func (c *Candidates) Len() int { return len(c.Hosts) }

// Use records that a host now occupies one more slot side in this
// pairing.
func (c *Candidates) Use(id string) { c.UseCount[id]++ }

// Release records that a host no longer occupies a slot side.
func (c *Candidates) Release(id string) {
	if c.UseCount[id] > 0 {
		c.UseCount[id]--
	}
}

// Has reports whether a host is eligible on this side.
func (c *Candidates) Has(id string) bool {
	for _, h := range c.Hosts {
		if h == id {
			return true
		}
	}
	return false
}

// minUse returns the lowest use count across eligible hosts.
func (c *Candidates) minUse() int {
	min := math.MaxInt
	for _, h := range c.Hosts {
		if n := c.UseCount[h]; n < min {
			min = n
		}
	}
	if min == math.MaxInt {
		return 0
	}
	return min
}

// Scanner selects hosts for empty slot sides.
type Scanner struct {
	cfg config.SlotsConfig
}

// NewScanner creates a Scanner bound to the slot configuration.
func NewScanner(cfg config.SlotsConfig) *Scanner { return &Scanner{cfg: cfg} }

// StartOffset returns the scan start position for one pairing. It is a
// hash of the pairing key modulo the candidate count. The offset spreads
// slot load across the candidate set instead of loading the
// alphabetically first hosts with every pairing, and it is a pure
// function of the key, so every node computes the same value.
func StartOffset(pk pairing.Key, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(pk))
	return int(h.Sum32() % uint32(n))
}

// Pick selects one host for an empty slot side. exclude holds hosts that
// must not be chosen, which is how the intra-zone case avoids pairing a
// host with itself. It returns RankUnfilled and an empty string when no
// host qualifies, which happens when AllowReuse is false and every
// candidate is already used.
func (s *Scanner) Pick(pk pairing.Key, c *Candidates, exclude map[string]bool) (string, Rank) {
	if c.Len() == 0 {
		return "", RankUnfilled
	}
	start := StartOffset(pk, c.Len())

	// Rank 1: a host not yet used on this side of this pairing.
	if id, ok := s.scan(c, start, exclude, func(h string) bool {
		return c.UseCount[h] == 0
	}); ok {
		return id, RankUnique
	}
	if !s.cfg.AllowReuse {
		return "", RankUnfilled
	}

	// Rank 2: a host at the lowest current use count.
	min := c.minUse()
	if id, ok := s.scan(c, start, exclude, func(h string) bool {
		return c.UseCount[h] == min
	}); ok {
		return id, RankMinUse
	}

	// Rank 3: any eligible host.
	if id, ok := s.scan(c, start, exclude, func(string) bool { return true }); ok {
		return id, RankAny
	}
	return "", RankUnfilled
}

// scan walks the candidate list from the start offset, wrapping once,
// and returns the first host that satisfies the predicate and is not
// excluded.
func (s *Scanner) scan(c *Candidates, start int, exclude map[string]bool,
	pred func(string) bool) (string, bool) {

	n := len(c.Hosts)
	for i := 0; i < n; i++ {
		h := c.Hosts[(start+i)%n]
		if exclude != nil && exclude[h] {
			continue
		}
		if pred(h) {
			return h, true
		}
	}
	return "", false
}

// PickAnchor selects the anchor host for one side of one pairing. All
// anchor slots then copy this host, so the anchor set changes together
// or not at all. The anchor is chosen by scan position only, ignoring
// use counts, so that the same host is chosen on every node.
func (s *Scanner) PickAnchor(pk pairing.Key, c *Candidates, exclude map[string]bool) (string, Rank) {
	if c.Len() == 0 {
		return "", RankUnfilled
	}
	start := StartOffset(pk, c.Len())
	if id, ok := s.scan(c, start, exclude, func(string) bool { return true }); ok {
		rank := RankUnique
		if c.UseCount[id] > 0 {
			rank = RankMinUse
		}
		return id, rank
	}
	return "", RankUnfilled
}

// PickSuper selects the super hosts of one zone. Selection uses the
// attribute selector when it is set and matches enough hosts, and the
// canonical order otherwise. A super host is sticky: current holds the
// previous selection and is kept while its members stay eligible, so
// that the wide fan-out measurement does not move between hosts on every
// inventory change.
func (s *Scanner) PickSuper(z zone.Key, members []string, snap *inventory.Snapshot,
	current []string, eligible func(string) bool) []string {

	want := s.cfg.SuperHosts
	if want <= 0 {
		return nil
	}

	elig := func(id string) bool { return eligible == nil || eligible(id) }

	// Keep the previous selection while it is still valid.
	out := make([]string, 0, want)
	inZone := make(map[string]bool, len(members))
	for _, m := range members {
		inZone[m] = true
	}
	for _, id := range current {
		if len(out) >= want {
			break
		}
		if inZone[id] && elig(id) {
			out = append(out, id)
		}
	}
	if len(out) >= want {
		return out
	}

	chosen := make(map[string]bool, len(out))
	for _, id := range out {
		chosen[id] = true
	}

	// Fill the remainder from the selector matches first.
	if len(s.cfg.SuperSelector) > 0 {
		for _, id := range members {
			if len(out) >= want {
				break
			}
			if chosen[id] || !elig(id) {
				continue
			}
			h, ok := snap.Get(id)
			if !ok || !matchSelector(h, s.cfg.SuperSelector) {
				continue
			}
			out = append(out, id)
			chosen[id] = true
		}
	}

	// Then fill from the canonical order.
	for _, id := range members {
		if len(out) >= want {
			break
		}
		if chosen[id] || !elig(id) {
			continue
		}
		out = append(out, id)
		chosen[id] = true
	}

	sort.Strings(out)
	return out
}

// matchSelector reports whether a host satisfies every key and value
// pair of the super selector.
func matchSelector(h inventory.HostRecord, sel map[string]string) bool {
	for k, want := range sel {
		if got, ok := h.Attributes[k]; !ok || got != want {
			return false
		}
	}
	return true
}

