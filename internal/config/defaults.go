package config

import "time"

// Default returns a Config with every default value applied.
func Default() Config {
	return Config{
		Providers: ProvidersConfig{
			File: FileConfig{
				Path:     "/etc/meshd/inventory.json",
				Interval: Duration(60 * time.Second),
				Priority: 10,
			},
			HTTP: HTTPConfig{
				Interval:    Duration(60 * time.Second),
				Timeout:     Duration(10 * time.Second),
				CachePath:   "/var/lib/meshd/inventory.json",
				CacheMaxAge: Duration(24 * time.Hour),
				BackoffMin:  Duration(5 * time.Second),
				BackoffMax:  Duration(300 * time.Second),
				Priority:    20,
			},
			K8s: K8sConfig{
				Resync:       Duration(300 * time.Second),
				Interval:     Duration(60 * time.Second),
				Debounce:     Duration(2 * time.Second),
				AddressOrder: []string{"InternalIP", "Hostname"},
				TaintDeny: []string{
					"node.kubernetes.io/unreachable",
					"node.kubernetes.io/not-ready",
					"node.kubernetes.io/unschedulable",
				},
				AnnotationAllow: []string{},
				Priority:        30,
			},
		},
		Zone: ZoneConfig{
			Keys:      []ZoneKeySpec{{Key: "metro"}, {Key: "dc_instance"}},
			Separator: "/",
			Missing:   "exclude",
		},
		Pairings: PairingsConfig{
			IntraZone:   false,
			MaxPairings: 5000,
		},
		Slots: SlotsConfig{
			Count:           4,
			AnchorRatio:     0.5,
			AnchorRounding:  "up",
			SuperHosts:      0,
			SuperMaxTargets: 50,
			AllowReuse:      true,
			RebalanceOnAdd:  false,
		},
		Health: HealthConfig{
			UnhealthyAfter:     3,
			ReleaseHold:        Duration(60 * time.Second),
			HealthyAfter:       2,
			InitialGrace:       Duration(90 * time.Second),
			MissingGrace:       Duration(60 * time.Second),
			DNSGrace:           Duration(120 * time.Second),
			FlapThreshold:      3,
			FlapWindow:         Duration(10 * time.Minute),
			FlapCooldown:       Duration(15 * time.Minute),
			PairingRemovalHold: Duration(300 * time.Second),
			Reclaim:            false,
		},
		Probes: ProbesConfig{
			Cycle:  Duration(15 * time.Second),
			Window: Duration(60 * time.Second),
			ICMP: ICMPConfig{
				Enabled: true, Interval: Duration(time.Second), Count: 10,
				PayloadBytes: 56, Timeout: Duration(time.Second), TTL: 64,
			},
			UDP: UDPConfig{
				Enabled: true, Interval: Duration(time.Second), Count: 10,
				PayloadBytes: 64, Port: 8472, Timeout: Duration(time.Second),
			},
			TCP: TCPConfig{
				Enabled: true, Interval: Duration(5 * time.Second), Count: 5,
				PayloadBytes: 64, Port: 9100, Timeout: Duration(2 * time.Second),
				Mode: "connect",
			},
		},
		Responder: ResponderConfig{
			Enabled:   true,
			UDPListen: "0.0.0.0:8472",
			TCPListen: "0.0.0.0:9100",
		},
		MeshPing: MeshPingConfig{
			Path:              "/usr/local/bin/meshping",
			RestartBackoffMin: Duration(time.Second),
			RestartBackoffMax: Duration(30 * time.Second),
			HelloTimeout:      Duration(5 * time.Second),
		},
		Reconcile: ReconcileConfig{Interval: Duration(30 * time.Second)},
		Persist: PersistConfig{
			Path:     "/var/lib/meshd/state.json",
			Debounce: Duration(60 * time.Second),
			MaxDelay: Duration(300 * time.Second),
		},
		API: APIConfig{Listen: "0.0.0.0:9101"},
		Metrics: MetricsConfig{
			MaxSeries:   200000,
			RTTBuckets:  []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
			Percentiles: []float64{0.5, 0.9, 0.99},
		},
		Log: LogConfig{Level: "info", Format: "json"},
	}
}

