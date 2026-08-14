// Package config holds the complete effective configuration, its
// defaults, its validation, and the reload watcher.
package config

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration so that a value such as 60s is accepted
// in both YAML and JSON. The standard library does not parse a duration
// string into time.Duration directly.
type Duration time.Duration

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		var n int64
		if err2 := value.Decode(&n); err2 != nil {
			return err
		}
		*d = Duration(time.Duration(n) * time.Second)
		return nil
	}
	p, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(p)
	return nil
}

func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		p, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		*d = Duration(p)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*d = Duration(time.Duration(n) * time.Second)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// Config is the complete effective configuration after defaults.
type Config struct {
	NodeID    string          `yaml:"node_id"   json:"node_id"`
	Providers ProvidersConfig `yaml:"providers" json:"providers"`
	Zone      ZoneConfig      `yaml:"zone"      json:"zone"`
	Pairings  PairingsConfig  `yaml:"pairings"  json:"pairings"`
	Slots     SlotsConfig     `yaml:"slots"     json:"slots"`
	Health    HealthConfig    `yaml:"health"    json:"health"`
	Probes    ProbesConfig    `yaml:"probes"    json:"probes"`
	Responder ResponderConfig `yaml:"responder" json:"responder"`
	MeshPing  MeshPingConfig  `yaml:"meshping"  json:"meshping"`
	Reconcile ReconcileConfig `yaml:"reconcile" json:"reconcile"`
	Persist   PersistConfig   `yaml:"persist"   json:"persist"`
	API       APIConfig       `yaml:"api"       json:"api"`
	Metrics   MetricsConfig   `yaml:"metrics"   json:"metrics"`
	Log       LogConfig       `yaml:"log"       json:"log"`
}

type ProvidersConfig struct {
	File FileConfig `yaml:"file" json:"file"`
	HTTP HTTPConfig `yaml:"http" json:"http"`
	K8s  K8sConfig  `yaml:"k8s"  json:"k8s"`
}

type FileConfig struct {
	Enabled  bool     `yaml:"enabled"  json:"enabled"`
	Path     string   `yaml:"path"     json:"path"`
	Interval Duration `yaml:"interval" json:"interval"`
	Priority int      `yaml:"priority" json:"priority"`
}

type HTTPConfig struct {
	Enabled        bool     `yaml:"enabled"          json:"enabled"`
	URL            string   `yaml:"url"              json:"url"`
	Interval       Duration `yaml:"interval"         json:"interval"`
	Timeout        Duration `yaml:"timeout"          json:"timeout"`
	CachePath      string   `yaml:"cache_path"       json:"cache_path"`
	CacheMaxAge    Duration `yaml:"cache_max_age"    json:"cache_max_age"`
	AuthHeader     string   `yaml:"auth_header"      json:"-"`
	AuthHeaderFile string   `yaml:"auth_header_file" json:"auth_header_file"`
	CAFile         string   `yaml:"ca_file"          json:"ca_file"`
	InsecureTLS    bool     `yaml:"insecure_tls"     json:"insecure_tls"`
	BackoffMin     Duration `yaml:"backoff_min"      json:"backoff_min"`
	BackoffMax     Duration `yaml:"backoff_max"      json:"backoff_max"`
	Priority       int      `yaml:"priority"         json:"priority"`
}

type K8sConfig struct {
	Enabled         bool     `yaml:"enabled"          json:"enabled"`
	ClusterName     string   `yaml:"cluster_name"     json:"cluster_name"`
	Kubeconfig      string   `yaml:"kubeconfig"       json:"kubeconfig"`
	Resync          Duration `yaml:"resync"           json:"resync"`
	Interval        Duration `yaml:"interval"         json:"interval"`
	Debounce        Duration `yaml:"debounce"         json:"debounce"`
	LabelSelector   string   `yaml:"label_selector"   json:"label_selector"`
	AddressOrder    []string `yaml:"address_order"    json:"address_order"`
	TaintDeny       []string `yaml:"taint_deny"       json:"taint_deny"`
	AnnotationAllow []string `yaml:"annotation_allow" json:"annotation_allow"`
	Priority        int      `yaml:"priority"         json:"priority"`
}

// ZoneKeySpec is one element of the zone rule. It accepts either a bare
// string or an object carrying a transform.
type ZoneKeySpec struct {
	Key       string `yaml:"key"       json:"key"`
	Transform string `yaml:"transform" json:"transform"`
}

// UnmarshalYAML accepts both the scalar form and the mapping form.
func (z *ZoneKeySpec) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		return value.Decode(&z.Key)
	}
	type alias ZoneKeySpec
	var a alias
	if err := value.Decode(&a); err != nil {
		return err
	}
	*z = ZoneKeySpec(a)
	return nil
}

func (z *ZoneKeySpec) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		z.Key = s
		return nil
	}
	type alias ZoneKeySpec
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*z = ZoneKeySpec(a)
	return nil
}

type ZoneConfig struct {
	Keys      []ZoneKeySpec `yaml:"keys"      json:"keys"`
	Separator string        `yaml:"separator" json:"separator"`
	Missing   string        `yaml:"missing"   json:"missing"`
}

type PairingsConfig struct {
	IntraZone   bool     `yaml:"intra_zone"   json:"intra_zone"`
	Include     []string `yaml:"include"      json:"include"`
	Exclude     []string `yaml:"exclude"      json:"exclude"`
	MaxPairings int      `yaml:"max_pairings" json:"max_pairings"`
}

type SlotsConfig struct {
	Count           int               `yaml:"count"             json:"count"`
	AnchorRatio     float64           `yaml:"anchor_ratio"      json:"anchor_ratio"`
	AnchorRounding  string            `yaml:"anchor_rounding"   json:"anchor_rounding"`
	SuperHosts      int               `yaml:"super_hosts"       json:"super_hosts"`
	SuperSelector   map[string]string `yaml:"super_selector"    json:"super_selector"`
	SuperMaxTargets int               `yaml:"super_max_targets" json:"super_max_targets"`
	AllowReuse      bool              `yaml:"allow_reuse"       json:"allow_reuse"`
	RebalanceOnAdd  bool              `yaml:"rebalance_on_add"  json:"rebalance_on_add"`
}

type HealthConfig struct {
	UnhealthyAfter     int      `yaml:"unhealthy_after"      json:"unhealthy_after"`
	ReleaseHold        Duration `yaml:"release_hold"         json:"release_hold"`
	HealthyAfter       int      `yaml:"healthy_after"        json:"healthy_after"`
	InitialGrace       Duration `yaml:"initial_grace"        json:"initial_grace"`
	MissingGrace       Duration `yaml:"missing_grace"        json:"missing_grace"`
	DNSGrace           Duration `yaml:"dns_grace"            json:"dns_grace"`
	FlapThreshold      int      `yaml:"flap_threshold"       json:"flap_threshold"`
	FlapWindow         Duration `yaml:"flap_window"          json:"flap_window"`
	FlapCooldown       Duration `yaml:"flap_cooldown"        json:"flap_cooldown"`
	PairingRemovalHold Duration `yaml:"pairing_removal_hold" json:"pairing_removal_hold"`
	Reclaim            bool     `yaml:"reclaim"              json:"reclaim"`
}

type ProbesConfig struct {
	Cycle  Duration   `yaml:"cycle"  json:"cycle"`
	Window Duration   `yaml:"window" json:"window"`
	ICMP   ICMPConfig `yaml:"icmp"   json:"icmp"`
	UDP    UDPConfig  `yaml:"udp"    json:"udp"`
	TCP    TCPConfig  `yaml:"tcp"    json:"tcp"`
}

type ICMPConfig struct {
	Enabled      bool     `yaml:"enabled"       json:"enabled"`
	Interval     Duration `yaml:"interval"      json:"interval"`
	Count        int      `yaml:"count"         json:"count"`
	PayloadBytes int      `yaml:"payload_bytes" json:"payload_bytes"`
	Timeout      Duration `yaml:"timeout"       json:"timeout"`
	TTL          int      `yaml:"ttl"           json:"ttl"`
	DF           bool     `yaml:"df"            json:"df"`
}

type UDPConfig struct {
	Enabled      bool     `yaml:"enabled"       json:"enabled"`
	Interval     Duration `yaml:"interval"      json:"interval"`
	Count        int      `yaml:"count"         json:"count"`
	PayloadBytes int      `yaml:"payload_bytes" json:"payload_bytes"`
	Port         int      `yaml:"port"          json:"port"`
	Timeout      Duration `yaml:"timeout"       json:"timeout"`
}

type TCPConfig struct {
	Enabled      bool     `yaml:"enabled"       json:"enabled"`
	Interval     Duration `yaml:"interval"      json:"interval"`
	Count        int      `yaml:"count"         json:"count"`
	PayloadBytes int      `yaml:"payload_bytes" json:"payload_bytes"`
	Port         int      `yaml:"port"          json:"port"`
	Timeout      Duration `yaml:"timeout"       json:"timeout"`
	Mode         string   `yaml:"mode"          json:"mode"`
}

type ResponderConfig struct {
	Enabled   bool   `yaml:"enabled"    json:"enabled"`
	UDPListen string `yaml:"udp_listen" json:"udp_listen"`
	TCPListen string `yaml:"tcp_listen" json:"tcp_listen"`
}

type MeshPingConfig struct {
	Path              string   `yaml:"path"                json:"path"`
	RestartBackoffMin Duration `yaml:"restart_backoff_min" json:"restart_backoff_min"`
	RestartBackoffMax Duration `yaml:"restart_backoff_max" json:"restart_backoff_max"`
	HelloTimeout      Duration `yaml:"hello_timeout"       json:"hello_timeout"`
}

type ReconcileConfig struct {
	Interval Duration `yaml:"interval" json:"interval"`
}

type PersistConfig struct {
	Path     string   `yaml:"path"      json:"path"`
	Debounce Duration `yaml:"debounce"  json:"debounce"`
	MaxDelay Duration `yaml:"max_delay" json:"max_delay"`
}

type APIConfig struct {
	Listen string `yaml:"listen" json:"listen"`
}

type MetricsConfig struct {
	MaxSeries   int       `yaml:"max_series"  json:"max_series"`
	RTTBuckets  []float64 `yaml:"rtt_buckets" json:"rtt_buckets"`
	Percentiles []float64 `yaml:"percentiles" json:"percentiles"`
}

type LogConfig struct {
	Level  string `yaml:"level"  json:"level"`
	Format string `yaml:"format" json:"format"`
}

