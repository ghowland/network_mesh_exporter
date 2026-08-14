// Package reconcile computes the slot assignment. The Reconcile function
// is pure: it performs no input and no output, holds no locks, and does
// not mutate its inputs. Two nodes with the same inputs therefore
// produce the same output without communicating.
package reconcile

import (
	"fmt"
	"sort"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/inventory"
	"github.com/example/mesh/internal/pairing"
	"github.com/example/mesh/internal/slot"
	"github.com/example/mesh/internal/state"
	"github.com/example/mesh/internal/zone"
)

// Reason explains why one slot side changed. It becomes a metric label,
// so an operator can distinguish a repair from a topology change.
type Reason string

const (
	ReasonNewSlot       Reason = "new_slot"
	ReasonHostGone      Reason = "host_gone"
	ReasonHostUnhealthy Reason = "host_unhealthy"
	ReasonZoneChanged   Reason = "zone_changed"
	ReasonClassChanged  Reason = "class_changed"
	ReasonAnchorReset   Reason = "anchor_reset"
	ReasonRebalance     Reason = "rebalance"
	ReasonSuperChanged  Reason = "super_changed"
	ReasonUnfilled      Reason = "unfilled"
)

// SideChange is one slot side that gained, lost, or replaced a host.
type SideChange struct {
	Pairing string    `json:"pairing"`
	Slot    int       `json:"slot"`
	Side    slot.Side `json:"side"`
	Class   string    `json:"class"`
	Old     string    `json:"old"`
	New     string    `json:"new"`
	Reason  Reason    `json:"reason"`
	Rank    slot.Rank `json:"rank"`
}

// Delta lists every change. The runner applies only this list, so a task
// that is not in the delta is never interrupted.
type Delta struct {
	PairingsAdded   []string     `json:"pairings_added"`
	PairingsRemoved []string     `json:"pairings_removed"`
	SidesChanged    []SideChange `json:"sides_changed"`
	SuperChanged    []string     `json:"super_changed"`
}

// Empty reports whether nothing changed.
func (d Delta) Empty() bool {
	return len(d.PairingsAdded) == 0 && len(d.PairingsRemoved) == 0 &&
		len(d.SidesChanged) == 0 && len(d.SuperChanged) == 0
}

// Stats summarises one reconcile for the metrics and the API.
type Stats struct {
	Hosts         int            `json:"hosts"`
	Unresolved    int            `json:"unresolved"`
	Zones         int            `json:"zones"`
	Pairings      int            `json:"pairings"`
	SlotsTotal    int            `json:"slots_total"`
	SlotsFilled   int            `json:"slots_filled"`
	SlotsUnfilled int            `json:"slots_unfilled"`
	ByClass       map[string]int `json:"by_class"`
	ByRank        map[int]int    `json:"by_rank"`
	Duration      time.Duration  `json:"duration"`
}

// Input is everything the reconcile reads. Collecting it into one struct
// keeps the function pure and makes it reproducible from fixtures alone.
type Input struct {
	Snapshot *inventory.Snapshot
	Config   *config.Config
	Rule     *zone.Rule
	Filter   *pairing.Filter
	Scanner  *slot.Scanner
	Eligible func(string) bool
	Current  *state.State
	Now      time.Time
}

// Output is the new state plus the description of what changed.
type Output struct {
	State *state.State
	Delta Delta
	Stats Stats
	Index *zone.Index
}

// Reconcile computes the new assignment.
func Reconcile(in Input) (Output, error) {
	start := time.Now()
	if in.Now.IsZero() {
		in.Now = start
	}

	idx := resolveZones(in)

	want, err := desiredPairings(in, idx)
	if err != nil {
		return Output{Index: idx}, err
	}

	next := in.Current.Clone()
	if next == nil {
		next = state.New(in.Rule.Fingerprint(), slotsFingerprint(in.Config.Slots))
	}

	// A change of the zone rule or of the slot layout redefines what a
	// slot means. Keeping the old table would produce assignments the
	// new rule would never make, so the table is rebuilt from scratch.
	zoneFP := in.Rule.Fingerprint()
	slotFP := slotsFingerprint(in.Config.Slots)
	if next.ZoneRule != zoneFP || next.SlotsConfig != slotFP {
		next = state.New(zoneFP, slotFP)
	}

	var d Delta
	diffPairings(in, want, next, &d)
	validateSides(in, idx, next, &d)
	updateSuper(in, idx, next, &d)
	fillSides(in, idx, next, &d)

	sort.Strings(d.PairingsAdded)
	sort.Strings(d.PairingsRemoved)
	sort.Strings(d.SuperChanged)
	sort.Slice(d.SidesChanged, func(i, j int) bool {
		a, b := d.SidesChanged[i], d.SidesChanged[j]
		if a.Pairing != b.Pairing {
			return a.Pairing < b.Pairing
		}
		if a.Slot != b.Slot {
			return a.Slot < b.Slot
		}
		return a.Side < b.Side
	})

	st := buildStats(idx, next)
	st.Hosts = in.Snapshot.Len()
	st.Duration = time.Since(start)

	return Output{State: next, Delta: d, Stats: st, Index: idx}, nil
}

// resolveZones applies the rule to the snapshot.
func resolveZones(in Input) *zone.Index {
	return zone.Build(in.Rule, in.Snapshot)
}

// desiredPairings builds the wanted pairing set and enforces the limit.
func desiredPairings(in Input, idx *zone.Index) ([]pairing.Pairing, error) {
	return pairing.Build(idx, in.Config.Pairings, in.Filter)
}

// diffPairings creates new slot tables, keeps existing ones, and marks
// vanished ones for removal after pairing_removal_hold.
func diffPairings(in Input, want []pairing.Pairing, next *state.State, d *Delta) {
	layout := slot.ComputeLayout(in.Config.Slots)

	wanted := make(map[string]pairing.Pairing, len(want))
	for _, p := range want {
		wanted[string(p.Key)] = p
	}

	for _, p := range want {
		key := string(p.Key)
		cur := next.Pairings[key]
		if cur == nil {
			cur = &state.Pairing{
				Key:   key,
				ZoneA: string(p.ZoneA),
				ZoneB: string(p.ZoneB),
				Intra: p.Intra,
			}
			next.Pairings[key] = cur
			d.PairingsAdded = append(d.PairingsAdded, key)
		}
		// A pairing that returned inside the removal hold keeps its
		// slots, so a zone that briefly disappears does not lose its
		// assignment.
		cur.RemoveAt = time.Time{}
		resizeSlots(cur, layout, d)
	}

	hold := in.Config.Health.PairingRemovalHold.D()
	for key, p := range next.Pairings {
		if _, ok := wanted[key]; ok {
			continue
		}
		if p.RemoveAt.IsZero() {
			p.RemoveAt = in.Now.Add(hold)
			continue
		}
		if !in.Now.Before(p.RemoveAt) {
			delete(next.Pairings, key)
			d.PairingsRemoved = append(d.PairingsRemoved, key)
		}
	}
}

// resizeSlots applies the class layout to one pairing. Anchor slots
// occupy the low indices, so raising the slot count appends diverse
// slots and leaves existing indices in place.
func resizeSlots(p *state.Pairing, layout slot.Layout, d *Delta) {
	// Separate the fixed-layout slots from the super slots, which live
	// above the layout total and are managed by updateSuper.
	base := make([]state.Slot, 0, layout.Total)
	var supers []state.Slot
	for _, s := range p.Slots {
		if s.Class == string(slot.ClassSuper) {
			supers = append(supers, s)
			continue
		}
		base = append(base, s)
	}

	for len(base) < layout.Total {
		base = append(base, state.Slot{Index: len(base)})
	}
	if len(base) > layout.Total {
		for _, s := range base[layout.Total:] {
			if s.HostA != "" {
				d.SidesChanged = append(d.SidesChanged, SideChange{
					Pairing: p.Key, Slot: s.Index, Side: slot.SideA,
					Class: s.Class, Old: s.HostA, Reason: ReasonClassChanged,
				})
			}
			if s.HostB != "" {
				d.SidesChanged = append(d.SidesChanged, SideChange{
					Pairing: p.Key, Slot: s.Index, Side: slot.SideB,
					Class: s.Class, Old: s.HostB, Reason: ReasonClassChanged,
				})
			}
		}
		base = base[:layout.Total]
	}

	for i := range base {
		base[i].Index = i
		want := string(layout.ClassAt(i))
		if base[i].Class == want {
			continue
		}
		// The class of an index changed, so the hosts held under the old
		// policy are released and re-picked under the new one.
		if base[i].HostA != "" {
			d.SidesChanged = append(d.SidesChanged, SideChange{
				Pairing: p.Key, Slot: i, Side: slot.SideA,
				Class: want, Old: base[i].HostA, Reason: ReasonClassChanged,
			})
			base[i].HostA, base[i].RankA = "", int(slot.RankUnfilled)
		}
		if base[i].HostB != "" {
			d.SidesChanged = append(d.SidesChanged, SideChange{
				Pairing: p.Key, Slot: i, Side: slot.SideB,
				Class: want, Old: base[i].HostB, Reason: ReasonClassChanged,
			})
			base[i].HostB, base[i].RankB = "", int(slot.RankUnfilled)
		}
		base[i].Class = want
	}

	p.Slots = append(base, supers...)
}

// validateSides clears each slot side whose host is gone, ineligible, or
// in the wrong zone. It never clears the other side of the same slot,
// which is the mechanism that keeps a valid measurement running when one
// host fails.
func validateSides(in Input, idx *zone.Index, next *state.State, d *Delta) {
	for _, p := range next.Pairings {
		if !p.RemoveAt.IsZero() {
			continue
		}
		for i := range p.Slots {
			s := &p.Slots[i]
			checkSide(in, idx, p, s, slot.SideA, zone.Key(p.ZoneA), d)
			checkSide(in, idx, p, s, slot.SideB, zone.Key(p.ZoneB), d)
		}
	}
}

// checkSide validates one endpoint and clears it when it is no longer
// valid.
func checkSide(in Input, idx *zone.Index, p *state.Pairing, s *state.Slot,
	side slot.Side, want zone.Key, d *Delta) {

	id := s.Host(side)
	if id == "" {
		return
	}

	var reason Reason
	switch {
	case !hostPresent(in.Snapshot, id):
		reason = ReasonHostGone
	case !in.Eligible(id):
		reason = ReasonHostUnhealthy
	default:
		if got, ok := idx.ZoneOf(id); !ok || got != want {
			reason = ReasonZoneChanged
		}
	}
	if reason == "" {
		return
	}

	d.SidesChanged = append(d.SidesChanged, SideChange{
		Pairing: p.Key, Slot: s.Index, Side: side,
		Class: s.Class, Old: id, Reason: reason,
	})
	s.SetHost(side, "", slot.RankUnfilled, time.Time{})
}

// updateSuper recomputes the super host set of each zone and rebuilds
// the super slots of each pairing. A super slot exists for one target
// host, so the slot set follows the eligible host set of the far zone.
func updateSuper(in Input, idx *zone.Index, next *state.State, d *Delta) {
	if in.Config.Slots.SuperHosts <= 0 {
		// Super testing is disabled, so any super slots left from a
		// previous configuration are removed.
		for _, p := range next.Pairings {
			p.Slots = dropSuper(p, d)
		}
		if len(next.SuperHosts) > 0 {
			for z := range next.SuperHosts {
				d.SuperChanged = append(d.SuperChanged, z)
			}
			next.SuperHosts = make(map[string][]string)
		}
		return
	}

	// Recompute the super host set per zone.
	for _, z := range idx.Zones {
		key := string(z)
		chosen := in.Scanner.PickSuper(z, idx.Members[z], in.Snapshot,
			next.SuperHosts[key], in.Eligible)
		if !sameStrings(next.SuperHosts[key], chosen) {
			next.SuperHosts[key] = chosen
			d.SuperChanged = append(d.SuperChanged, key)
		}
	}
	for key := range next.SuperHosts {
		if _, ok := idx.Members[zone.Key(key)]; !ok {
			delete(next.SuperHosts, key)
			d.SuperChanged = append(d.SuperChanged, key)
		}
	}

	max := in.Config.Slots.SuperMaxTargets
	layout := slot.ComputeLayout(in.Config.Slots)

	for _, p := range next.Pairings {
		if !p.RemoveAt.IsZero() {
			continue
		}
		base := p.Slots[:min(len(p.Slots), layout.Total)]
		existing := make(map[string]state.Slot)
		for _, s := range p.Slots[len(base):] {
			if s.Class == string(slot.ClassSuper) {
				existing[s.HostA+"|"+s.SuperTarget] = s
			}
		}

		wanted := superPairs(in, idx, next, p, max)
		out := make([]state.Slot, 0, len(wanted))
		used := make(map[string]bool, len(wanted))
		idxNo := layout.Total

		for _, w := range wanted {
			k := w.src + "|" + w.dst
			used[k] = true
			if s, ok := existing[k]; ok {
				s.Index = idxNo
				out = append(out, s)
				idxNo++
				continue
			}
			s := state.Slot{
				Index:       idxNo,
				Class:       string(slot.ClassSuper),
				SuperTarget: w.dst,
			}
			s.SetHost(slot.SideA, w.src, slot.RankUnique, in.Now)
			s.SetHost(slot.SideB, w.dst, slot.RankUnique, in.Now)
			out = append(out, s)
			d.SidesChanged = append(d.SidesChanged, SideChange{
				Pairing: p.Key, Slot: idxNo, Side: slot.SideA,
				Class: string(slot.ClassSuper), New: w.src,
				Reason: ReasonNewSlot, Rank: slot.RankUnique,
			})
			d.SidesChanged = append(d.SidesChanged, SideChange{
				Pairing: p.Key, Slot: idxNo, Side: slot.SideB,
				Class: string(slot.ClassSuper), New: w.dst,
				Reason: ReasonNewSlot, Rank: slot.RankUnique,
			})
			idxNo++
		}

		for k, s := range existing {
			if used[k] {
				continue
			}
			d.SidesChanged = append(d.SidesChanged, SideChange{
				Pairing: p.Key, Slot: s.Index, Side: slot.SideA,
				Class: string(slot.ClassSuper), Old: s.HostA, Reason: ReasonSuperChanged,
			})
			d.SidesChanged = append(d.SidesChanged, SideChange{
				Pairing: p.Key, Slot: s.Index, Side: slot.SideB,
				Class: string(slot.ClassSuper), Old: s.HostB, Reason: ReasonSuperChanged,
			})
		}

		p.Slots = append(append([]state.Slot{}, base...), out...)
	}
}

type superPair struct{ src, dst string }

// superPairs lists the desired super slot endpoints of one pairing. Each
// super host of one zone pairs with every eligible host of the other
// zone, bounded by super_max_targets, and both directions are produced
// so that each zone's super hosts are measured against the far zone.
func superPairs(in Input, idx *zone.Index, next *state.State,
	p *state.Pairing, max int) []superPair {

	var out []superPair
	add := func(srcZone, dstZone string) {
		for _, src := range next.SuperHosts[srcZone] {
			if !in.Eligible(src) {
				continue
			}
			n := 0
			for _, dst := range idx.Members[zone.Key(dstZone)] {
				if n >= max {
					break
				}
				if dst == src || !in.Eligible(dst) {
					continue
				}
				out = append(out, superPair{src: src, dst: dst})
				n++
			}
		}
	}
	add(p.ZoneA, p.ZoneB)
	if p.ZoneA != p.ZoneB {
		add(p.ZoneB, p.ZoneA)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].src != out[j].src {
			return out[i].src < out[j].src
		}
		return out[i].dst < out[j].dst
	})
	return out
}

// dropSuper removes every super slot from a pairing and records the
// removals.
func dropSuper(p *state.Pairing, d *Delta) []state.Slot {
	out := p.Slots[:0]
	for _, s := range p.Slots {
		if s.Class != string(slot.ClassSuper) {
			out = append(out, s)
			continue
		}
		if s.HostA != "" {
			d.SidesChanged = append(d.SidesChanged, SideChange{
				Pairing: p.Key, Slot: s.Index, Side: slot.SideA,
				Class: s.Class, Old: s.HostA, Reason: ReasonSuperChanged,
			})
		}
		if s.HostB != "" {
			d.SidesChanged = append(d.SidesChanged, SideChange{
				Pairing: p.Key, Slot: s.Index, Side: slot.SideB,
				Class: s.Class, Old: s.HostB, Reason: ReasonSuperChanged,
			})
		}
	}
	return out
}

// fillSides fills empty sides in the order anchor, then diverse, so that
// anchor hosts are chosen from the widest candidate set. Super slots are
// filled by updateSuper and are not touched here.
func fillSides(in Input, idx *zone.Index, next *state.State, d *Delta) {
	keys := make([]string, 0, len(next.Pairings))
	for k, p := range next.Pairings {
		if p.RemoveAt.IsZero() {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		p := next.Pairings[key]
		pk := pairing.Key(key)

		candA := slot.NewCandidates(zone.Key(p.ZoneA), idx.Members[zone.Key(p.ZoneA)], in.Eligible)
		candB := slot.NewCandidates(zone.Key(p.ZoneB), idx.Members[zone.Key(p.ZoneB)], in.Eligible)

		// Count the hosts already held, so that the scan sees the true
		// use counts of this pairing.
		for _, s := range p.Slots {
			if s.HostA != "" {
				candA.Use(s.HostA)
			}
			if s.HostB != "" {
				candB.Use(s.HostB)
			}
		}

		fillAnchors(in, p, pk, candA, candB, d)
		fillDiverse(in, p, pk, candA, candB, d)
	}
}

// fillAnchors gives every anchor slot in the pairing the same host on
// each side. When the anchor host is gone, every anchor side is
// re-picked together, so the anchor set stays internally consistent.
func fillAnchors(in Input, p *state.Pairing, pk pairing.Key,
	candA, candB *slot.Candidates, d *Delta) {

	var anchors []int
	for i, s := range p.Slots {
		if s.Class == string(slot.ClassAnchor) {
			anchors = append(anchors, i)
		}
	}
	if len(anchors) == 0 {
		return
	}

	fillSide := func(side slot.Side, cand *slot.Candidates) {
		// An existing anchor host is kept, and every empty anchor slot
		// copies it.
		host := ""
		for _, i := range anchors {
			if h := p.Slots[i].Host(side); h != "" {
				host = h
				break
			}
		}
		if host == "" {
			exclude := map[string]bool{}
			if p.Intra {
				// In an intra-zone pairing the two sides must differ, so
				// the far side's anchor is excluded here.
				other := slot.SideA
				if side == slot.SideA {
					other = slot.SideB
				}
				for _, i := range anchors {
					if h := p.Slots[i].Host(other); h != "" {
						exclude[h] = true
					}
				}
			}
			id, rank := in.Scanner.PickAnchor(pk, cand, exclude)
			if id == "" {
				return
			}
			host = id
			for _, i := range anchors {
				if p.Slots[i].Host(side) != "" {
					continue
				}
				p.Slots[i].SetHost(side, host, rank, in.Now)
				cand.Use(host)
				d.SidesChanged = append(d.SidesChanged, SideChange{
					Pairing: p.Key, Slot: p.Slots[i].Index, Side: side,
					Class: string(slot.ClassAnchor), New: host,
					Reason: ReasonNewSlot, Rank: rank,
				})
			}
			return
		}
		for _, i := range anchors {
			if p.Slots[i].Host(side) != "" {
				continue
			}
			p.Slots[i].SetHost(side, host, slot.RankMinUse, in.Now)
			cand.Use(host)
			d.SidesChanged = append(d.SidesChanged, SideChange{
				Pairing: p.Key, Slot: p.Slots[i].Index, Side: side,
				Class: string(slot.ClassAnchor), New: host,
				Reason: ReasonAnchorReset, Rank: slot.RankMinUse,
			})
		}
	}

	fillSide(slot.SideA, candA)
	fillSide(slot.SideB, candB)
}

// fillDiverse fills each diverse slot side by the ranked scan.
func fillDiverse(in Input, p *state.Pairing, pk pairing.Key,
	candA, candB *slot.Candidates, d *Delta) {

	for i := range p.Slots {
		s := &p.Slots[i]
		if s.Class != string(slot.ClassDiverse) {
			continue
		}
		if s.HostA == "" {
			exclude := map[string]bool{}
			if p.Intra && s.HostB != "" {
				exclude[s.HostB] = true
			}
			if id, rank := in.Scanner.Pick(pk, candA, exclude); id != "" {
				s.SetHost(slot.SideA, id, rank, in.Now)
				candA.Use(id)
				d.SidesChanged = append(d.SidesChanged, SideChange{
					Pairing: p.Key, Slot: s.Index, Side: slot.SideA,
					Class: s.Class, New: id, Reason: ReasonNewSlot, Rank: rank,
				})
			}
		}
		if s.HostB == "" {
			exclude := map[string]bool{}
			if p.Intra && s.HostA != "" {
				exclude[s.HostA] = true
			}
			if id, rank := in.Scanner.Pick(pk, candB, exclude); id != "" {
				s.SetHost(slot.SideB, id, rank, in.Now)
				candB.Use(id)
				d.SidesChanged = append(d.SidesChanged, SideChange{
					Pairing: p.Key, Slot: s.Index, Side: slot.SideB,
					Class: s.Class, New: id, Reason: ReasonNewSlot, Rank: rank,
				})
			}
		}
	}
}

// buildStats summarises the resulting state.
func buildStats(idx *zone.Index, st *state.State) Stats {
	out := Stats{
		Unresolved: len(idx.Unresolved),
		Zones:      len(idx.Zones),
		ByClass:    make(map[string]int),
		ByRank:     make(map[int]int),
	}
	for _, p := range st.Pairings {
		if !p.RemoveAt.IsZero() {
			continue
		}
		out.Pairings++
		for _, s := range p.Slots {
			out.SlotsTotal++
			out.ByClass[s.Class]++
			if s.Filled() {
				out.SlotsFilled++
				out.ByRank[s.RankA]++
				out.ByRank[s.RankB]++
			} else {
				out.SlotsUnfilled++
			}
		}
	}
	return out
}

// slotsFingerprint describes the slot layout configuration. A change of
// this value rebuilds the slot table, because the meaning of a slot
// index changed.
func slotsFingerprint(c config.SlotsConfig) string {
	return fmt.Sprintf("n=%d ratio=%.4f round=%s super=%d max=%d reuse=%t",
		c.Count, c.AnchorRatio, c.AnchorRounding, c.SuperHosts,
		c.SuperMaxTargets, c.AllowReuse)
}

func hostPresent(snap *inventory.Snapshot, id string) bool {
	_, ok := snap.Get(id)
	return ok
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

