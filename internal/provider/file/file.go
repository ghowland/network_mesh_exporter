// Package file reads the inventory document from a local file. The
// document schema is shared with the HTTP provider, so a node can move
// between the two sources without a topology change.
package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/inventory"
	"github.com/example/mesh/internal/provider"
)

// Name is the source identifier.
const Name = "file"

// DefaultNameFormat is the field order assumed when the document does
// not set one. It describes web-001.product.prod.sjc01.domain.com.
var DefaultNameFormat = []string{"role_ordinal", "service", "environment", "site"}

// SiteEntry is the internal record for one site token. It carries the
// country and the data centre label, which the DNS name does not
// contain.
type SiteEntry struct {
	Country    string            `json:"country"`
	Metro      string            `json:"metro"`
	DCLabel    string            `json:"dc_label"`
	DCInstance string            `json:"dc_instance"`
	Extra      map[string]string `json:"extra"`
}

// HostEntry is one host in the document.
type HostEntry struct {
	Name       string            `json:"name"`
	Address    string            `json:"address"`
	Enabled    *bool             `json:"enabled"`
	Attributes map[string]string `json:"attributes"`
}

// Document is the inventory JSON accepted by both the file provider and
// the HTTP provider.
type Document struct {
	Version    int                  `json:"version"`
	SiteTable  map[string]SiteEntry `json:"site_table"`
	NameFormat []string             `json:"name_format"`
	Hosts      []HostEntry          `json:"hosts"`
}

// Validate checks a document for structural problems. A document that
// fails is never applied, so a bad publish cannot empty the inventory.
func (d *Document) Validate() error {
	if d.Version != 0 && d.Version != 1 {
		return fmt.Errorf("inventory: unsupported document version %d", d.Version)
	}
	if len(d.Hosts) == 0 {
		return errors.New("inventory: document contains no hosts")
	}
	seen := make(map[string]bool, len(d.Hosts))
	for i, h := range d.Hosts {
		if h.Name == "" {
			return fmt.Errorf("inventory: hosts[%d] has an empty name", i)
		}
		if seen[h.Name] {
			return fmt.Errorf("inventory: duplicate host name %q", h.Name)
		}
		seen[h.Name] = true
	}
	return nil
}

// Provider reads the document from a local file.
type Provider struct {
	cfg     config.FileConfig
	refresh chan struct{}
	mtime   time.Time
	size    int64
}

// New creates a file Provider.
func New(cfg config.FileConfig) *Provider {
	return &Provider{cfg: cfg, refresh: make(chan struct{}, 1)}
}

func (p *Provider) Name() string { return Name }

// Refresh requests an immediate re-read.
func (p *Provider) Refresh() {
	select {
	case p.refresh <- struct{}{}:
	default:
	}
}

// Run re-reads the file on modification and on the configured interval.
func (p *Provider) Run(ctx context.Context, sink provider.Sink) error {
	interval := p.cfg.Interval.D()
	if interval <= 0 {
		interval = 60 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()

	p.poll(sink, true)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			p.poll(sink, false)
		case <-p.refresh:
			p.poll(sink, true)
		}
	}
}

// poll re-reads the file when it changed, or unconditionally when force
// is set. A parse failure keeps the current set in place.
func (p *Provider) poll(sink provider.Sink, force bool) {
	fi, err := os.Stat(p.cfg.Path)
	if err != nil {
		slog.Error("inventory file unavailable", "path", p.cfg.Path, "error", err)
		return
	}
	if !force && fi.ModTime().Equal(p.mtime) && fi.Size() == p.size {
		return
	}
	set, err := p.load()
	if err != nil {
		slog.Error("inventory file rejected, keeping previous set",
			"path", p.cfg.Path, "error", err)
		return
	}
	p.mtime, p.size = fi.ModTime(), fi.Size()
	sink.Replace(set)
	slog.Info("inventory file loaded", "path", p.cfg.Path, "hosts", len(set.Hosts))
}

// load reads and parses the file and converts it to a host set.
func (p *Provider) load() (inventory.Set, error) {
	data, err := os.ReadFile(p.cfg.Path)
	if err != nil {
		return inventory.Set{}, err
	}
	return Parse(data, Name, p.cfg.Priority)
}

// Parse validates a document and converts it into host records. It is
// exported so that the HTTP provider uses the same conversion.
func Parse(data []byte, source string, priority int) (inventory.Set, error) {
	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return inventory.Set{}, fmt.Errorf("inventory: parse: %w", err)
	}
	if err := doc.Validate(); err != nil {
		return inventory.Set{}, err
	}
	format := doc.NameFormat
	if len(format) == 0 {
		format = DefaultNameFormat
	}

	now := time.Now()
	set := inventory.Set{Source: source, FetchAt: now}
	set.Hosts = make([]inventory.HostRecord, 0, len(doc.Hosts))

	for _, h := range doc.Hosts {
		attrs, err := ParseName(h.Name, format)
		if err != nil {
			return inventory.Set{}, fmt.Errorf("inventory: host %q: %w", h.Name, err)
		}
		if site, ok := attrs["site"]; ok {
			if e, found := doc.SiteTable[site]; found {
				applySite(attrs, e)
			} else {
				metro, inst := splitSite(site)
				setIfAbsent(attrs, "metro", metro)
				setIfAbsent(attrs, "dc_instance", inst)
			}
		}
		// Explicit per-host attributes override the derived values, so
		// that one host can be corrected without a schema change.
		for k, v := range h.Attributes {
			attrs[k] = v
		}

		addr := h.Address
		if addr == "" {
			addr = h.Name
		}
		enabled := true
		reason := ""
		if h.Enabled != nil && !*h.Enabled {
			enabled = false
			reason = "disabled in inventory document"
		}
		set.Hosts = append(set.Hosts, inventory.HostRecord{
			ID:         h.Name,
			Address:    addr,
			Attributes: attrs,
			Source:     source,
			Healthy:    enabled,
			Reason:     reason,
			SeenAt:     now,
			Priority:   priority,
		})
	}
	return set, nil
}

// ParseName splits a DNS name into named fields using the name format.
// For web-001.product.prod.sjc01.domain.com with the default format the
// result contains role, ordinal, service, environment, site, domain, and
// fqdn.
func ParseName(name string, format []string) (map[string]string, error) {
	labels := strings.Split(name, ".")
	if len(labels) <= len(format) {
		return nil, fmt.Errorf("name has %d labels, format needs more than %d",
			len(labels), len(format))
	}
	attrs := make(map[string]string, len(format)+4)
	attrs["fqdn"] = name

	for i, field := range format {
		v := labels[i]
		switch field {
		case "role_ordinal":
			role, ordinal := splitOrdinal(v)
			attrs["role"] = role
			attrs["ordinal"] = ordinal
		case "site":
			attrs["site"] = v
		default:
			attrs[field] = v
		}
	}
	attrs["domain"] = strings.Join(labels[len(format):], ".")
	attrs["hostname"] = labels[0]
	return attrs, nil
}

// splitOrdinal separates a leading role from a trailing ordinal, so that
// web-001 becomes web and 001.
func splitOrdinal(label string) (string, string) {
	i := strings.LastIndex(label, "-")
	if i > 0 && i < len(label)-1 && isDigits(label[i+1:]) {
		return label[:i], label[i+1:]
	}
	j := len(label)
	for j > 0 && label[j-1] >= '0' && label[j-1] <= '9' {
		j--
	}
	if j == len(label) || j == 0 {
		return label, ""
	}
	return label[:j], label[j:]
}

// splitSite separates a site token into a metro and a DC instance, so
// that sjc01 becomes sjc and 01. This is the fallback used when the site
// is absent from the site table.
func splitSite(site string) (string, string) {
	j := len(site)
	for j > 0 && site[j-1] >= '0' && site[j-1] <= '9' {
		j--
	}
	if j == len(site) {
		return site, ""
	}
	return site[:j], site[j:]
}

// applySite merges a site table entry into the attribute map. Values
// already present are not overwritten.
func applySite(attrs map[string]string, e SiteEntry) {
	setIfAbsent(attrs, "country", e.Country)
	setIfAbsent(attrs, "metro", e.Metro)
	setIfAbsent(attrs, "dc_label", e.DCLabel)
	setIfAbsent(attrs, "dc_instance", e.DCInstance)
	for k, v := range e.Extra {
		setIfAbsent(attrs, k, v)
	}
}

func setIfAbsent(m map[string]string, k, v string) {
	if v == "" {
		return
	}
	if _, ok := m[k]; !ok {
		m[k] = v
	}
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

