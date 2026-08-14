// Package zone derives zone keys from host attributes by rule. The rule
// is the only place that decides what a zone is, so a change of mesh
// level is a change of configuration and not a change of code.
package zone

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/inventory"
)

// Key is a derived zone identifier.
type Key string

// MissingPolicy controls what happens when a required attribute is
// absent.
type MissingPolicy int

const (
	MissingExclude MissingPolicy = iota
	MissingEmpty
	MissingLiteral
)

// Transform is a value rewrite applied before the join.
type Transform func(string) string

type keySpec struct {
	key       string
	transform Transform
	raw       string
}

// Rule converts a host record into a zone key. It is built once from the
// configuration and is then read-only and safe for concurrent use.
type Rule struct {
	keys    []keySpec
	sep     string
	missing MissingPolicy
	literal string
	desc    string
	fp      string
}

// NewRule compiles a ZoneConfig into a Rule. Invalid transforms and an
// empty key list are rejected here rather than at apply time.
func NewRule(cfg config.ZoneConfig) (*Rule, error) {
	if len(cfg.Keys) == 0 {
		return nil, fmt.Errorf("zone: keys is empty")
	}
	r := &Rule{sep: cfg.Separator}
	if r.sep == "" {
		r.sep = "/"
	}
	switch {
	case cfg.Missing == "" || cfg.Missing == "exclude":
		r.missing = MissingExclude
	case cfg.Missing == "empty":
		r.missing = MissingEmpty
	case strings.HasPrefix(cfg.Missing, "literal:"):
		r.missing = MissingLiteral
		r.literal = strings.TrimPrefix(cfg.Missing, "literal:")
	default:
		return nil, fmt.Errorf("zone: unknown missing policy %q", cfg.Missing)
	}

	parts := make([]string, 0, len(cfg.Keys))
	for i, k := range cfg.Keys {
		if k.Key == "" {
			return nil, fmt.Errorf("zone: keys[%d] has an empty key", i)
		}
		ks := keySpec{key: k.Key, raw: k.Transform}
		if k.Transform != "" {
			t, err := compileTransform(k.Transform)
			if err != nil {
				return nil, fmt.Errorf("zone: keys[%d]: %w", i, err)
			}
			ks.transform = t
			parts = append(parts, k.Key+"|"+k.Transform)
		} else {
			parts = append(parts, k.Key)
		}
		r.keys = append(r.keys, ks)
	}
	r.desc = strings.Join(parts, ",") + " sep=" + r.sep + " missing=" + cfg.Missing
	sum := sha256.Sum256([]byte(r.desc))
	r.fp = hex.EncodeToString(sum[:8])
	return r, nil
}

// Apply returns the zone key for one host. ok is false when the host has
// no zone under this rule.
func (r *Rule) Apply(h inventory.HostRecord) (Key, bool) {
	parts := make([]string, 0, len(r.keys))
	for _, ks := range r.keys {
		v, present := h.Attributes[ks.key]
		if !present || v == "" {
			switch r.missing {
			case MissingExclude:
				return "", false
			case MissingEmpty:
				v = ""
			case MissingLiteral:
				v = r.literal
			}
		}
		if ks.transform != nil {
			v = ks.transform(v)
		}
		parts = append(parts, v)
	}
	return Key(strings.Join(parts, r.sep)), true
}

// MissingKey reports the first rule key that the host does not satisfy.
// The API uses this to explain why a host is unresolved.
func (r *Rule) MissingKey(h inventory.HostRecord) string {
	for _, ks := range r.keys {
		if v, ok := h.Attributes[ks.key]; !ok || v == "" {
			return ks.key
		}
	}
	return ""
}

// Keys returns the attribute keys the rule reads, in order.
func (r *Rule) Keys() []string {
	out := make([]string, len(r.keys))
	for i, k := range r.keys {
		out[i] = k.key
	}
	return out
}

// String returns a stable description of the rule.
func (r *Rule) String() string { return r.desc }

// Fingerprint returns a hash of the rule. A changed fingerprint in the
// loaded state means the topology definition changed and the slot table
// is rebuilt from scratch.
func (r *Rule) Fingerprint() string { return r.fp }

// compileTransform builds a Transform from a specification string.
// Supported: lower, upper, trim, prefix:<n>, regex:<pattern>:<replacement>.
func compileTransform(spec string) (Transform, error) {
	switch {
	case spec == "lower":
		return strings.ToLower, nil
	case spec == "upper":
		return strings.ToUpper, nil
	case spec == "trim":
		return strings.TrimSpace, nil
	case strings.HasPrefix(spec, "prefix:"):
		n, err := strconv.Atoi(strings.TrimPrefix(spec, "prefix:"))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid prefix length in %q", spec)
		}
		return func(s string) string {
			if len(s) <= n {
				return s
			}
			return s[:n]
		}, nil
	case strings.HasPrefix(spec, "regex:"):
		body := strings.TrimPrefix(spec, "regex:")
		i := strings.LastIndex(body, ":")
		if i < 0 {
			return nil, fmt.Errorf("regex transform needs pattern and replacement: %q", spec)
		}
		re, err := regexp.Compile(body[:i])
		if err != nil {
			return nil, fmt.Errorf("invalid regex in %q: %w", spec, err)
		}
		repl := body[i+1:]
		return func(s string) string { return re.ReplaceAllString(s, repl) }, nil
	default:
		return nil, fmt.Errorf("unknown transform %q", spec)
	}
}

// Unresolved records one host that produced no zone key.
type Unresolved struct {
	HostID     string `json:"host_id"`
	MissingKey string `json:"missing_key"`
}

// Index is the result of applying a rule to a whole snapshot.
type Index struct {
	Rule       *Rule
	Zones      []Key
	Members    map[Key][]string
	HostZone   map[string]Key
	Unresolved []Unresolved
}

// Build applies the rule to every host in the snapshot. Member lists
// keep the canonical order of the snapshot, which every deterministic
// scan depends on.
func Build(r *Rule, snap *inventory.Snapshot) *Index {
	idx := &Index{
		Rule:     r,
		Members:  make(map[Key][]string),
		HostZone: make(map[string]Key, snap.Len()),
	}
	for _, h := range snap.Hosts {
		k, ok := r.Apply(h)
		if !ok {
			idx.Unresolved = append(idx.Unresolved, Unresolved{
				HostID:     h.ID,
				MissingKey: r.MissingKey(h),
			})
			continue
		}
		idx.Members[k] = append(idx.Members[k], h.ID)
		idx.HostZone[h.ID] = k
	}
	idx.Zones = make([]Key, 0, len(idx.Members))
	for k := range idx.Members {
		idx.Zones = append(idx.Zones, k)
	}
	sort.Slice(idx.Zones, func(i, j int) bool { return idx.Zones[i] < idx.Zones[j] })
	return idx
}

// MemberCount returns the host count of one zone.
func (i *Index) MemberCount(k Key) int { return len(i.Members[k]) }

// ZoneOf returns the zone key of one host.
func (i *Index) ZoneOf(hostID string) (Key, bool) {
	k, ok := i.HostZone[hostID]
	return k, ok
}

