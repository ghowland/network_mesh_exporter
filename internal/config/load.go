package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Load reads a YAML or JSON file, applies defaults for absent fields,
// and validates the result. A file that fails validation is not returned.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	format := "yaml"
	if strings.EqualFold(filepath.Ext(path), ".json") {
		format = "json"
	}
	return Parse(data, format)
}

// Parse is Load without file access.
func Parse(data []byte, format string) (*Config, error) {
	cfg := Default()
	var err error
	switch format {
	case "json":
		err = json.Unmarshal(data, &cfg)
	default:
		err = yaml.Unmarshal(data, &cfg)
	}
	if err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	cfg.normalise()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// normalise fills values that depend on other values.
func (c *Config) normalise() {
	if c.NodeID == "" {
		if h, err := os.Hostname(); err == nil {
			c.NodeID = h
		}
	}
	if c.Zone.Separator == "" {
		c.Zone.Separator = "/"
	}
	if c.Zone.Missing == "" {
		c.Zone.Missing = "exclude"
	}
	if c.Slots.AnchorRounding == "" {
		c.Slots.AnchorRounding = "up"
	}
	if c.Probes.TCP.Mode == "" {
		c.Probes.TCP.Mode = "connect"
	}
	if c.Persist.MaxDelay.D() < c.Persist.Debounce.D() {
		c.Persist.MaxDelay = Duration(5 * c.Persist.Debounce.D())
	}
}

// Validate reports every problem found, joined into one error. It never
// mutates the Config.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if c.NodeID == "" {
		add("node_id is empty and the hostname could not be read")
	}

	p := c.Providers
	if !p.File.Enabled && !p.HTTP.Enabled && !p.K8s.Enabled {
		add("no provider is enabled")
	}
	if p.File.Enabled && p.File.Path == "" {
		add("providers.file.path is empty")
	}
	if p.HTTP.Enabled {
		if p.HTTP.URL == "" {
			add("providers.http.url is empty")
		} else if !strings.HasPrefix(p.HTTP.URL, "http://") && !strings.HasPrefix(p.HTTP.URL, "https://") {
			add("providers.http.url must start with http:// or https://")
		}
		if p.HTTP.CachePath == "" {
			add("providers.http.cache_path is empty")
		}
		if p.HTTP.Interval.D() < time.Second {
			add("providers.http.interval must be at least 1s")
		}
	}
	if p.K8s.Enabled && p.K8s.ClusterName == "" {
		add("providers.k8s.cluster_name is empty; it is part of the host identifier")
	}

	if len(c.Zone.Keys) == 0 {
		add("zone.keys is empty")
	}
	for i, k := range c.Zone.Keys {
		if k.Key == "" {
			add("zone.keys[%d].key is empty", i)
		}
	}
	switch {
	case c.Zone.Missing == "exclude", c.Zone.Missing == "empty":
	case strings.HasPrefix(c.Zone.Missing, "literal:"):
	default:
		add("zone.missing must be exclude, empty, or literal:<value>")
	}

	if c.Pairings.MaxPairings <= 0 {
		add("pairings.max_pairings must be positive")
	}

	if c.Slots.Count <= 0 {
		add("slots.count must be positive")
	}
	if c.Slots.AnchorRatio < 0 || c.Slots.AnchorRatio > 1 {
		add("slots.anchor_ratio must be between 0 and 1")
	}
	if c.Slots.AnchorRounding != "up" && c.Slots.AnchorRounding != "down" {
		add("slots.anchor_rounding must be up or down")
	}
	if c.Slots.SuperHosts < 0 {
		add("slots.super_hosts must not be negative")
	}
	if c.Slots.SuperMaxTargets <= 0 {
		add("slots.super_max_targets must be positive")
	}

	if c.Health.UnhealthyAfter <= 0 {
		add("health.unhealthy_after must be positive")
	}
	if c.Health.HealthyAfter <= 0 {
		add("health.healthy_after must be positive")
	}

	if c.Probes.Cycle.D() <= 0 {
		add("probes.cycle must be positive")
	}
	if c.Probes.Window.D() < c.Probes.Cycle.D() {
		add("probes.window must be at least probes.cycle")
	}
	if !c.Probes.ICMP.Enabled && !c.Probes.UDP.Enabled && !c.Probes.TCP.Enabled {
		add("no probe type is enabled")
	}
	validateProbe := func(name string, enabled bool, count int, payload int, iv, to Duration) {
		if !enabled {
			return
		}
		if count <= 0 {
			add("probes.%s.count must be positive", name)
		}
		if payload < 8 {
			add("probes.%s.payload_bytes must be at least 8", name)
		}
		if to.D() <= 0 {
			add("probes.%s.timeout must be positive", name)
		}
		if iv.D() < 0 {
			add("probes.%s.interval must not be negative", name)
		}
	}
	validateProbe("icmp", c.Probes.ICMP.Enabled, c.Probes.ICMP.Count,
		c.Probes.ICMP.PayloadBytes, c.Probes.ICMP.Interval, c.Probes.ICMP.Timeout)
	validateProbe("udp", c.Probes.UDP.Enabled, c.Probes.UDP.Count,
		c.Probes.UDP.PayloadBytes, c.Probes.UDP.Interval, c.Probes.UDP.Timeout)
	validateProbe("tcp", c.Probes.TCP.Enabled, c.Probes.TCP.Count,
		c.Probes.TCP.PayloadBytes, c.Probes.TCP.Interval, c.Probes.TCP.Timeout)

	if c.Probes.UDP.Enabled && (c.Probes.UDP.Port <= 0 || c.Probes.UDP.Port > 65535) {
		add("probes.udp.port is out of range")
	}
	if c.Probes.TCP.Enabled {
		if c.Probes.TCP.Port <= 0 || c.Probes.TCP.Port > 65535 {
			add("probes.tcp.port is out of range")
		}
		if c.Probes.TCP.Mode != "connect" && c.Probes.TCP.Mode != "echo" {
			add("probes.tcp.mode must be connect or echo")
		}
	}

	if c.Probes.ICMP.Enabled && c.MeshPing.Path == "" {
		add("meshping.path is empty but icmp probes are enabled")
	}
	if c.Persist.Path == "" {
		add("persist.path is empty")
	}
	if c.Persist.Debounce.D() <= 0 {
		add("persist.debounce must be positive")
	}
	if c.API.Listen == "" {
		add("api.listen is empty")
	}
	if c.Metrics.MaxSeries <= 0 {
		add("metrics.max_series must be positive")
	}
	for _, q := range c.Metrics.Percentiles {
		if q <= 0 || q >= 1 {
			add("metrics.percentiles values must be between 0 and 1 exclusive")
		}
	}
	return errors.Join(errs...)
}

// Equal reports whether two configurations are identical. It is used to
// suppress a reconcile after a reload that changed nothing.
func (c *Config) Equal(o *Config) bool {
	if c == nil || o == nil {
		return c == o
	}
	return reflect.DeepEqual(*c, *o)
}

// TopologyChanged reports whether a reload changes zone keys, pairing
// filters, or slot layout. These changes rewrite the whole slot table,
// so the caller logs them at a higher level than an ordinary reload.
func (c *Config) TopologyChanged(o *Config) bool {
	if c == nil || o == nil {
		return true
	}
	return !reflect.DeepEqual(c.Zone, o.Zone) ||
		!reflect.DeepEqual(c.Pairings, o.Pairings) ||
		!reflect.DeepEqual(c.Slots, o.Slots)
}

// Redacted returns a copy with secret values removed, for the /config
// endpoint and for logging.
func (c *Config) Redacted() *Config {
	cp := *c
	if cp.Providers.HTTP.AuthHeader != "" {
		cp.Providers.HTTP.AuthHeader = "[redacted]"
	}
	return &cp
}

