# Technical Specification — Mesh Network Test System

Version 0.1 — design for approval. No code is written until this document is approved.

---

## 1. Scope

This document specifies a distributed network measurement system. The system reads a host inventory from one or more providers, derives zones from host attributes by rule, forms zone pairings, assigns hosts to stable measurement slots, runs ICMP, UDP, and TCP probes across those slots, and exports the results in Prometheus exposition format.

The system contains two programs:

| Program | Privilege | Function |
|---|---|---|
| `meshd` | Unprivileged | Discovery, zone derivation, slot assignment, TCP and UDP probes, state persistence, HTTP API, Prometheus endpoint |
| `meshping` | setuid root, or `CAP_NET_RAW` | ICMP echo probes only. Reads requests as JSON on stdin. Writes results as JSON on stdout |

One instance of `meshd` runs on each node. `meshd` starts `meshping` as a child process and communicates with it over pipes.

---

## 2. Terms

| Term | Definition |
|---|---|
| Host record | One inventory entry. Contains an identifier, an address, and an attribute map |
| Provider | A component that produces a set of host records |
| Inventory | The merged set of host records from all providers |
| Zone rule | An ordered list of attribute keys that produces a zone key from a host record |
| Zone key | A string that identifies one zone |
| Zone pairing | An unordered pair of zone keys |
| Slot | A numbered container in a zone pairing. Holds one host on side A and one host on side B |
| Slot side | One endpoint of one slot |
| Slot class | `anchor`, `diverse`, or `super` |
| Delta | The list of changes produced by one reconcile |
| Task | One directed probe of one type on one slot |

---

## 3. Architecture

```
                 +---------------------+
  file  ------>  |                     |
  http  ------>  |     Providers       |
  k8s   ------>  |                     |
                 +----------+----------+
                            | host records
                            v
                 +---------------------+
                 |     Inventory       |  merged, snapshot on read
                 +----------+----------+
                            |
                            v
                 +---------------------+
                 |   Zone Resolver     |  applies zone rule
                 +----------+----------+
                            |
                            v
                 +---------------------+      +---------------+
                 |     Reconciler      |<---->|  State (RAM)  |
                 +----------+----------+      +-------+-------+
                            | delta                   |
                            v                         v
                 +---------------------+      +---------------+
                 |      Runner         |      | JSON persist  |
                 +----+-----------+----+      +---------------+
                      |           |
             tcp/udp  |           | icmp over pipe
                      v           v
                 +--------+  +----------+
                 | net    |  | meshping |
                 +--------+  +----------+
                      |           |
                      v           v
                 +---------------------+
                 |   Metric Registry   |
                 +----------+----------+
                            |
                            v
                 +---------------------+
                 |    HTTP Server      |  /metrics /state /inventory ...
                 +---------------------+
```

---

## 4. Data model

### 4.1 Host record

```go
type HostRecord struct {
    ID         string            // unique across the inventory
    Address    string            // IP or DNS name used by probes
    Attributes map[string]string // topology data, untyped
    Source     string            // "file", "http", or "k8s"
    Healthy    bool              // as reported by the provider
    Reason     string            // why not healthy, empty when healthy
    SeenAt     time.Time         // last time the provider reported this host
}
```

The record has no country field, no metro field, and no data center field. All topology data is in `Attributes`. Zones are derived from `Attributes` by rule. This lets the Kubernetes provider work with any label set.

### 4.2 State

```go
type State struct {
    Version  int
    Pairings map[string]*Pairing  // key is the zone pairing key
}

type Pairing struct {
    ZoneA     string
    ZoneB     string
    Slots     []Slot
    RemoveAt  time.Time  // zero when the pairing is active
}

type Slot struct {
    Index     int
    Class     string     // "anchor", "diverse", "super"
    HostA     string     // host ID, empty when unfilled
    HostB     string     // host ID, empty when unfilled
    AssignedA time.Time
    AssignedB time.Time
    ReuseRank int        // 1, 2, or 3. See section 8.3
}
```

`State` is the only value that persists across restarts. It contains no measurements.

---

## 5. Providers

Each provider produces a complete host set. A provider update replaces the previous set from that provider atomically. The inventory is the union of all provider sets. If two providers produce the same host ID, the provider with the higher configured priority wins, and a counter increments.

### 5.1 File provider

Reads a local JSON file. Re-reads on file modification and on a fixed interval.

```json
{
  "site_table": {
    "sjc01": { "country": "us", "metro": "sjc", "dc_label": "sjc-equinix-sv5", "dc_instance": "01" },
    "sjc02": { "country": "us", "metro": "sjc", "dc_label": "sjc-digital-sjc2", "dc_instance": "02" }
  },
  "name_format": ["role_ordinal", "service", "environment", "site"],
  "hosts": [
    { "name": "web-001.product.prod.sjc01.domain.com", "enabled": true },
    { "name": "web-002.product.prod.sjc01.domain.com", "enabled": true },
    { "name": "web-001.product.prod.sjc02.domain.com", "enabled": true }
  ]
}
```

Parsing of `web-001.product.prod.sjc01.domain.com` produces these attributes:

| Key | Value |
|---|---|
| `role` | `web` |
| `ordinal` | `001` |
| `service` | `product` |
| `environment` | `prod` |
| `site` | `sjc01` |
| `domain` | `domain.com` |
| `fqdn` | full name |

The `site` value is then looked up in `site_table`. The table entry adds `country`, `metro`, `dc_label`, and `dc_instance`. If the site is not in the table, the provider splits the site token into a leading alphabetic part and a trailing numeric part, and sets `metro` and `dc_instance` from the split. `country` and `dc_label` stay unset.

The host ID is the FQDN. The address is the FQDN.

### 5.2 HTTP provider

Fetches the same JSON document from a URL. This is the source of truth when the file is not present on the node.

Fetch behaviour:

| Item | Behaviour |
|---|---|
| Interval | `http.interval`, default 60 s |
| Method | GET |
| Conditional request | Sends `If-None-Match` and `If-Modified-Since` from the previous response |
| 200 response | Parse, validate, replace the provider set, write the body to the cache file |
| 304 response | Keep the current set. No cache write |
| Non-2xx, or network failure | Keep the current set. Increment an error counter. Retry with exponential backoff, base 5 s, cap 300 s, with jitter |
| Parse failure or validation failure | Keep the current set. Increment a counter. The bad document is never applied |
| Timeout | `http.timeout`, default 10 s |

Cache behaviour:

- The cache file path is `http.cache_path`, default `/var/lib/meshd/inventory.json`.
- The write is atomic: write to a temporary file in the same directory, `fsync`, then `rename`.
- A sidecar file `inventory.meta.json` holds the ETag, the `Last-Modified` value, and the fetch timestamp.
- On start, the cache is read first and applied immediately. The first HTTP fetch then runs. This makes the node operational before the network is available.
- `http.cache_max_age`, default 24 h. If the cache is older than this value and no fetch has succeeded, the provider reports its hosts as stale. Stale hosts stay in the inventory and a gauge reports the cache age. Existing slots are not cleared because of staleness.

Authentication: optional `Authorization` header from a config value or from a file path. TLS uses the system trust store, with an optional `ca_file`.

### 5.3 Kubernetes provider

Lists and watches `Node` objects. Uses in-cluster credentials when available, otherwise a kubeconfig path.

| Item | Behaviour |
|---|---|
| Mode | Watch with a resync interval, `k8s.resync`, default 300 s |
| Fallback | If watch is not available, list on `k8s.interval`, default 60 s |
| Selector | `k8s.label_selector`, default empty, which selects all nodes |
| Host ID | `k8s://<cluster_name>/<node_name>` |
| Address | First match from `k8s.address_order`, default `["InternalIP", "Hostname"]` |

Attribute mapping:

| Attribute key | Source |
|---|---|
| `k8s.cluster` | `k8s.cluster_name` from config |
| `k8s.node` | node name |
| `k8s.region` | label `topology.kubernetes.io/region` |
| `k8s.zone` | label `topology.kubernetes.io/zone` |
| `k8s.instance-type` | label `node.kubernetes.io/instance-type` |
| `k8s.arch` | label `kubernetes.io/arch` |
| `k8s.os` | label `kubernetes.io/os` |
| `k8s.hostname` | label `kubernetes.io/hostname` |
| `k8s.label.<name>` | every node label, with `/` replaced by `_` |
| `k8s.annotation.<name>` | node annotations that match `k8s.annotation_allow` |

The default `k8s.annotation_allow` is empty. Annotations are large and change often. They are opt-in.

---

## 6. Zone derivation

### 6.1 Rule

```json
{
  "zone": {
    "keys": ["metro", "dc_instance"],
    "separator": "/",
    "missing": "exclude"
  }
}
```

The resolver reads each key from the host attributes in order and joins the values with the separator.

| `missing` value | Behaviour when a key has no value |
|---|---|
| `exclude` | The host is not placed in a zone. Default |
| `empty` | The value is treated as an empty string |
| `literal:<v>` | The value is replaced by `<v>` |

### 6.2 Mesh modes as rules

| Mode | `keys` value |
|---|---|
| Full mesh | `["fqdn"]` or `["k8s.node"]` |
| DC to DC | `["metro", "dc_instance"]` |
| Metro to metro | `["metro"]` |
| Country to country | `["country"]` |
| Kubernetes zone to zone | `["k8s.region", "k8s.zone"]` |
| Kubernetes region to region | `["k8s.region"]` |

Full mesh is not a separate code path. It is the rule that puts one host in each zone.

### 6.3 Optional transforms

Each key can carry a transform, applied before the join.

```json
{ "keys": [{ "key": "k8s.zone", "transform": "lower" }] }
```

Supported transforms: `lower`, `upper`, `trim`, `prefix:<n>` which keeps the first n characters, `regex:<pattern>:<replacement>`.

### 6.4 Pairing set

```json
{
  "pairings": {
    "intra_zone": false,
    "include": [],
    "exclude": [],
    "max_pairings": 5000
  }
}
```

- The base set is all unordered pairs of distinct zone keys.
- `intra_zone: true` adds one pairing per zone where both sides are the same zone. Slot fill then requires two distinct hosts.
- `include` and `exclude` hold glob patterns matched against the pairing key. `include` runs first when non-empty.
- The pairing key is the two zone keys sorted alphabetically and joined with `|`.
- If the pairing count exceeds `max_pairings`, the reconcile aborts, keeps the previous state, and sets an alert gauge. This prevents a wrong zone rule from creating a very large pairing set.

---

## 7. Slots

### 7.1 Configuration

```json
{
  "slots": {
    "count": 4,
    "anchor_ratio": 0.5,
    "anchor_rounding": "up",
    "super_hosts": 0,
    "allow_reuse": true,
    "rebalance_on_add": false
  }
}
```

`count` is the value of N per zone pairing. It is a minimum and also the target.

### 7.2 Classes

| Class | Fill rule | Purpose |
|---|---|---|
| `anchor` | All anchor slots in one pairing use the same host on side A and the same host on side B | The endpoints do not change. A change in the result is a change in the path |
| `diverse` | Prefers a host not yet used in this pairing on that side | Spreads across hosts. One bad host cannot represent the pairing |
| `super` | One designated host on side A pairs with each eligible host on side B, one slot per target host | Compares one machine against every machine in the other zone |

Anchor slot count is `round(count * anchor_ratio)` using `anchor_rounding`. The remainder are diverse. With `count: 4` and `anchor_ratio: 0.5` the result is 2 anchor slots and 2 diverse slots.

Super slots are additional. They are not taken from `count`. A pairing with `super_hosts: 1` and a side B of 20 hosts adds 20 super slots. Super slot count is capped by `super_max_targets`, default 50.

### 7.3 Super host selection

`super_hosts` is a count per zone. Selection is by rule, in this order:

1. Hosts matching `slots.super_selector`, a set of attribute key and value pairs. If the selector is set and matches enough hosts, use those hosts in canonical order.
2. Otherwise, the first `super_hosts` healthy hosts in the zone, in canonical order.

A super host is sticky. It changes only when it becomes ineligible.

---

## 8. Reconcile

### 8.1 Signature

```go
func Reconcile(snap Inventory, cfg Config, cur *State, now time.Time) (*State, Delta)
```

The function is pure. It performs no input and no output. It is fully testable from fixtures.

### 8.2 Steps

1. **Snapshot.** Copy the inventory under a read lock.
2. **Resolve zones.** Apply the zone rule to every host. Build the zone key set and the sorted host list per zone. Count unresolved hosts.
3. **Desired pairings.** Build all pairings, apply filters, check `max_pairings`.
4. **Diff pairings.**
   - Present in both: keep the slot table.
   - New: create an empty slot table with the correct class layout.
   - Absent: set `RemoveAt = now + pairing_removal_hold`. Delete when `now >= RemoveAt`. Clear `RemoveAt` if the pairing returns.
5. **Validate slot sides.** A side is cleared when any of these is true:
   - The host is not in the snapshot.
   - The host is unhealthy past the hysteresis threshold.
   - The host no longer resolves to the required zone.
   - The host no longer satisfies its slot class predicate.
   - The slot class layout changed because `count` or `anchor_ratio` changed.
   
   **Clearing one side never clears the other side.** This is the mechanism that keeps valid measurements running when one host fails.
6. **Fill.** For each empty side, in order anchor, then super, then diverse, run the scan in section 8.3.
7. **Emit delta.** List the slot sides that changed, the pairings created, and the pairings deleted.

### 8.3 Scan

Candidates are the healthy hosts of the required zone, sorted by host ID. The scan starts at `fnv32(pairing_key) % len(candidates)` and wraps. The start offset spreads slot load across the candidate set instead of loading the alphabetically first hosts with every pairing. The offset is a pure function of the pairing key and is therefore identical on every node.

Rank order for a diverse slot side:

| Rank | Condition |
|---|---|
| 1 | The host is not used on this side in this pairing |
| 2 | The host is used on this side at the minimum current use count |
| 3 | Any eligible host |

The scan takes the first candidate at the best available rank. When the zone has enough hosts, rank 1 always wins and no host is reused. When hosts are scarce, the slot still fills at a lower rank. The chosen rank is stored in `Slot.ReuseRank` and is exported as a metric label, so a query can distinguish a slot with independent endpoints from a slot that reuses a host.

If `allow_reuse` is false, only rank 1 is accepted. An unfillable slot stays empty and increments `mesh_slots_unfilled`.

For an anchor slot, the first anchor slot in the pairing selects the host by scan. All other anchor slots copy that host. If the anchor host becomes ineligible, all anchor slot sides in that pairing are cleared together and re-selected together.

### 8.4 Determinism

Two nodes with the same inventory, the same config, and the same prior state produce the same new state without communicating. The prior state is the only shared value. Nodes do not exchange state; each node loads its own state file. Divergence between nodes is possible after a restart with a lost state file, and is visible in the metrics because the slot host labels differ.

### 8.5 Triggers

Reconcile runs on: a provider update, a config reload, a health state transition, a pairing removal hold expiry, a periodic tick (`reconcile.interval`, default 30 s), and a POST to `/reconcile`.

Triggers are coalesced. One reconcile runs at a time. Triggers that arrive during a run cause exactly one further run afterwards, not one per trigger.

---

## 9. Health and hysteresis

### 9.1 Health inputs

Provider health is authoritative and immediate. Probe health is subject to hysteresis.

**File and HTTP providers:**

| Signal | Default |
|---|---|
| `enabled: false` in the document | Never eligible |
| Address does not resolve | Ineligible after `dns_grace`, default 120 s |
| Consecutive failed probe cycles | 3 marks unhealthy |
| Loss ratio over the window | 100 percent over 60 s marks unhealthy |
| Never probed successfully after assignment | Unhealthy after `initial_grace`, default 90 s |

**Kubernetes provider:**

| Signal | Default |
|---|---|
| `Ready` condition | Must be `True` |
| `spec.unschedulable` | `true` marks ineligible |
| Taints | Ineligible if any taint key is in `k8s.taint_deny`. Default: `node.kubernetes.io/unreachable`, `node.kubernetes.io/not-ready`, `node.kubernetes.io/unschedulable` |
| `metadata.deletionTimestamp` set | Ineligible immediately |
| Node absent from last resync | Ineligible after `missing_grace`, default 60 s |
| Label selector | Must match to be eligible |

### 9.2 Hysteresis

| Option | Default | Effect |
|---|---|---|
| `unhealthy_after` | 3 failed cycles | Cycles of failure before the unhealthy mark |
| `release_hold` | 60 s | Delay between the unhealthy mark and the slot side clear |
| `healthy_after` | 2 successful cycles | Cycles of success before the host is eligible again |
| `initial_grace` | 90 s | New hosts are not marked unhealthy in this window |
| `missing_grace` | 60 s | Grace for a host absent from a provider update |
| `dns_grace` | 120 s | Grace for a name that does not resolve |
| `flap_threshold` | 3 transitions in 10 min | Above this, the host enters cooldown |
| `flap_cooldown` | 15 min | Duration of the ineligible period after flapping |
| `pairing_removal_hold` | 300 s | Delay before a vanished pairing is deleted |
| `reclaim` | false | If true, a recovered host returns to a slot side it previously held when that side is currently filled at rank 2 or rank 3 |

`release_hold` is the important value. Without it, a short network event rewrites slot assignments and breaks the time series in the exact window where the measurement matters.

---

## 10. Probes

### 10.1 Common parameters

```json
{
  "probes": {
    "icmp": { "enabled": true,  "interval": "1s", "count": 10, "payload_bytes": 56,  "timeout": "1s" },
    "udp":  { "enabled": true,  "interval": "1s", "count": 10, "payload_bytes": 64,  "port": 8472, "timeout": "1s" },
    "tcp":  { "enabled": true,  "interval": "5s", "count": 5,  "payload_bytes": 64,  "port": 9100, "timeout": "2s", "mode": "connect" }
  },
  "cycle": "15s",
  "window": "60s"
}
```

- `interval` is the delay between packets inside one cycle.
- `count` is the number of packets per cycle.
- `cycle` is the delay between cycles for one task.
- `window` is the aggregation window for jitter, loss, and percentiles.
- `payload_bytes` is the payload size, not the total frame size. The ICMP total IP packet size is `payload_bytes + 8 + 20` for IPv4.

`tcp.mode` values:

| Value | Behaviour |
|---|---|
| `connect` | Measure the time to complete the three-way handshake. Close immediately. No payload is sent. `payload_bytes` is ignored |
| `echo` | Connect, send `payload_bytes` bytes, read the same number of bytes back, measure the round trip. Requires a responder on the target port |

`udp.mode` is always echo. UDP requires a responder. `meshd` runs a UDP responder on `udp.port` and a TCP responder on `tcp.port` when `responder.enabled` is true, default true. The responder returns the received payload unchanged, with the first 16 bytes replaced by a sequence number and a receive timestamp.

Probe packets carry a magic value in the first 4 bytes so that a responder can reject unrelated traffic.

### 10.2 Measured values

Per directed task, per window:

| Value | Definition |
|---|---|
| RTT min, max, mean | Over successful samples in the window |
| RTT percentiles | p50, p90, p99, configurable |
| Jitter | Mean of the absolute difference between consecutive RTT samples |
| Loss ratio | Lost samples divided by sent samples |
| Sent, received, lost | Counters |
| Reorder count | Samples that arrive out of sequence |
| TCP connect time | Separate from TCP round trip time when `mode: echo` |
| Error count by class | timeout, refused, unreachable, resolve, permission |

### 10.3 Direction

Each slot produces two directed tasks: A to B and B to A. Each node runs only the tasks where it is the source. The forward path and the reverse path are not always the same, so both are measured independently. No coordination between the two nodes is required.

---

## 11. `meshping` helper

### 11.1 Reason for a separate program

ICMP requires a raw socket or `CAP_NET_RAW`. Isolating this in a small program with a narrow input and output surface keeps `meshd` unprivileged. `meshping` contains no configuration parsing, no discovery, no HTTP, and no state.

### 11.2 Privilege

Preferred: `setcap cap_net_raw+ep /usr/local/bin/meshping`. Alternative: setuid root. `meshping` drops all other privileges at start and never executes another program. On Linux, `meshping` may instead use a non-privileged ICMP datagram socket when `net.ipv4.ping_group_range` permits it; it tries this first and falls back to a raw socket.

### 11.3 Protocol

Line-delimited JSON over stdin and stdout. One JSON object per line. `meshd` starts one long-lived `meshping` process and multiplexes all ICMP work through it.

**Request:**

```json
{"type":"ping","id":"a1b2c3","target":"10.0.4.12","count":10,"interval_ms":1000,"payload_bytes":56,"timeout_ms":1000,"ttl":64,"df":false}
```

**Result:**

```json
{"type":"result","id":"a1b2c3","target":"10.0.4.12","resolved":"10.0.4.12",
 "sent":10,"received":9,"lost":1,"reordered":0,
 "rtt_us":[1204,1190,1250,1198,1310,1201,1188,1240,1199],
 "error":"","error_class":""}
```

`meshd` computes all statistics from `rtt_us`. `meshping` performs no aggregation. This keeps the privileged program as small as possible.

**Other messages:**

| Type | Direction | Purpose |
|---|---|---|
| `hello` | out | Sent once at start. Contains the version and the privilege mode obtained |
| `cancel` | in | Cancel a request by ID |
| `error` | out | A request could not be started. Contains the ID and the reason |
| `shutdown` | in | Finish current work and exit |

Errors and diagnostics go to stderr as plain text. Stdout carries only protocol JSON.

### 11.4 Supervision

- `meshd` starts `meshping` at start and restarts it on exit, with backoff of 1 s, doubling to 30 s.
- If `meshping` does not produce `hello` within 5 s, `meshd` terminates it and retries.
- If `meshping` cannot obtain ICMP privilege, it reports the failure in `hello`. `meshd` then disables ICMP probes, logs the reason once, and sets `mesh_icmp_available` to 0. TCP and UDP probes continue.
- A request with no result within `timeout_ms * count + 5s` is marked failed and the ID is abandoned.

---

## 12. Persistence

The authoritative state is in RAM. The JSON file is a durable copy.

| Item | Behaviour |
|---|---|
| Load | On start, before the first reconcile |
| Dirty flag | Set by any reconcile that produces a non-empty delta |
| Debounce | `persist.debounce`, default 60 s. Resets on each further change |
| Maximum delay | `persist.max_delay`, default 300 s. Caps the debounce so continuous churn cannot postpone a write forever |
| Write | Temporary file in the same directory, `fsync`, `rename` |
| Path | `persist.path`, default `/var/lib/meshd/state.json` |
| Shutdown | Write immediately if dirty |
| Missing or corrupt file | Start with empty state, assign from scratch, log a topology reset, increment `mesh_state_reset_total` |
| Version mismatch | Same as corrupt |

A debounce of 60 s caps writes at one per minute. A debounce of 3 s caps writes at 20 per minute. Each write requires a full quiet window, so the write rate is at most `60 / debounce_seconds` per minute.

---

## 13. Metrics

Namespace `mesh`. Common labels on all probe metrics:

| Label | Value |
|---|---|
| `zone_src` | Source zone key |
| `zone_dst` | Destination zone key |
| `host_src` | Source host ID |
| `host_dst` | Destination host ID |
| `slot` | Slot index |
| `class` | `anchor`, `diverse`, `super` |
| `reuse_rank` | 1, 2, or 3 |
| `probe` | `icmp`, `udp`, `tcp` |

Probe metrics:

| Metric | Type |
|---|---|
| `mesh_rtt_seconds` | Histogram |
| `mesh_rtt_min_seconds`, `mesh_rtt_max_seconds`, `mesh_rtt_mean_seconds` | Gauge |
| `mesh_jitter_seconds` | Gauge |
| `mesh_loss_ratio` | Gauge |
| `mesh_packets_sent_total`, `mesh_packets_received_total`, `mesh_packets_lost_total` | Counter |
| `mesh_reorder_total` | Counter |
| `mesh_tcp_connect_seconds` | Histogram |
| `mesh_probe_errors_total` | Counter, extra label `class` |
| `mesh_probe_last_success_timestamp_seconds` | Gauge |

System metrics:

| Metric | Type |
|---|---|
| `mesh_hosts_total` | Gauge, label `source`, `state` |
| `mesh_hosts_unresolved` | Gauge |
| `mesh_zones_total` | Gauge |
| `mesh_pairings_total` | Gauge |
| `mesh_slots_total` | Gauge, label `class` |
| `mesh_slots_unfilled` | Gauge |
| `mesh_slot_changes_total` | Counter, label `reason` |
| `mesh_reconcile_duration_seconds` | Histogram |
| `mesh_reconcile_total` | Counter, label `trigger` |
| `mesh_provider_fetch_total` | Counter, label `source`, `result` |
| `mesh_provider_cache_age_seconds` | Gauge, label `source` |
| `mesh_provider_last_success_timestamp_seconds` | Gauge, label `source` |
| `mesh_state_persist_total` | Counter |
| `mesh_state_reset_total` | Counter |
| `mesh_icmp_available` | Gauge |
| `mesh_meshping_restarts_total` | Counter |

Series count is `pairings * (slots + super_slots) * probe_types * 2`. The exporter refuses to register beyond `metrics.max_series`, default 200000, and increments a counter instead.

---

## 14. HTTP API

| Path | Method | Content |
|---|---|---|
| `/metrics` | GET | Prometheus exposition |
| `/state` | GET | Current pairings and slots, read from RAM under a read lock |
| `/inventory` | GET | Host records with resolved zone keys |
| `/zones` | GET | Zone keys with member counts |
| `/pairings` | GET | Pairing keys with slot fill status |
| `/health` | GET | Per-host health state and hysteresis timers |
| `/config` | GET | Effective config after defaults are applied |
| `/reconcile` | POST | Force an immediate reconcile. Returns the delta |
| `/refresh` | POST | Force a provider fetch. Optional `source` parameter |
| `/livez` | GET | Process is running |
| `/readyz` | GET | At least one provider has succeeded and one reconcile has completed |

`/state` reads the RAM structs, not the JSON file, so it is current inside the debounce window.

---

## 15. Package layout

```
cmd/meshd/main.go
cmd/meshping/main.go

internal/config      config structs, defaults, validation, reload
internal/inventory   HostRecord, merged inventory, snapshot
internal/provider    Provider interface
internal/provider/file
internal/provider/http
internal/provider/k8s
internal/zone        zone rule, transforms, resolver
internal/pairing     pairing set construction and filters
internal/slot        slot classes, scan, rank
internal/reconcile   pure Reconcile function, Delta
internal/state       State structs, JSON load and save, debounce
internal/health      health tracking, hysteresis, flap detection
internal/probe       Prober interface, statistics, windows
internal/probe/tcp
internal/probe/udp
internal/probe/icmp  client side of the meshping protocol
internal/responder   UDP and TCP echo responders
internal/runner      task lifecycle, delta application, goroutine control
internal/metrics     registry and metric definitions
internal/api         HTTP handlers
pkg/pingproto        shared request and result types for meshping
```

`pkg/pingproto` is the only package that both binaries import.

---

## 16. Concurrency

| Component | Model |
|---|---|
| Providers | One goroutine each. Write to the inventory under a write lock |
| Reconciler | One goroutine. Serialised by a trigger channel of capacity 1 |
| Runner | One goroutine per directed task, each with its own ticker |
| ICMP client | One writer goroutine and one reader goroutine on the `meshping` pipes. Requests are correlated by ID through a map under a mutex |
| Responders | One goroutine per listener, plus one per accepted TCP connection |
| Metrics | The Prometheus client library handles its own locking |

Task start is spread over the cycle duration by a per-task offset derived from the task key, so all tasks do not fire in the same instant.

Every long-lived goroutine takes a `context.Context` and exits on cancel. Shutdown order is: HTTP server, runner, `meshping`, reconciler, providers, final state write.

---

## 17. Configuration file

One YAML or JSON file. Reload on `SIGHUP` and on file change. A reload that fails validation is rejected and the previous config stays active.

```yaml
node_id: web-001.product.prod.sjc01.domain.com

providers:
  file:
    enabled: true
    path: /etc/meshd/inventory.json
    interval: 60s
    priority: 10
  http:
    enabled: true
    url: https://config.domain.com/mesh/inventory.json
    interval: 60s
    timeout: 10s
    cache_path: /var/lib/meshd/inventory.json
    cache_max_age: 24h
    auth_header_file: /etc/meshd/token
    priority: 20
  k8s:
    enabled: false
    cluster_name: prod-usw2
    kubeconfig: ""
    resync: 300s
    label_selector: ""
    address_order: [InternalIP, Hostname]
    taint_deny:
      - node.kubernetes.io/unreachable
      - node.kubernetes.io/not-ready
      - node.kubernetes.io/unschedulable
    annotation_allow: []

zone:
  keys: [metro, dc_instance]
  separator: "/"
  missing: exclude

pairings:
  intra_zone: false
  include: []
  exclude: []
  max_pairings: 5000

slots:
  count: 4
  anchor_ratio: 0.5
  anchor_rounding: up
  super_hosts: 0
  super_selector: {}
  super_max_targets: 50
  allow_reuse: true
  rebalance_on_add: false

health:
  unhealthy_after: 3
  release_hold: 60s
  healthy_after: 2
  initial_grace: 90s
  missing_grace: 60s
  dns_grace: 120s
  flap_threshold: 3
  flap_window: 10m
  flap_cooldown: 15m
  pairing_removal_hold: 300s
  reclaim: false

probes:
  cycle: 15s
  window: 60s
  icmp: { enabled: true, interval: 1s, count: 10, payload_bytes: 56, timeout: 1s, ttl: 64, df: false }
  udp:  { enabled: true, interval: 1s, count: 10, payload_bytes: 64, port: 8472, timeout: 1s }
  tcp:  { enabled: true, interval: 5s, count: 5,  payload_bytes: 64, port: 9100, timeout: 2s, mode: connect }

responder:
  enabled: true
  udp_listen: "0.0.0.0:8472"
  tcp_listen: "0.0.0.0:9100"

meshping:
  path: /usr/local/bin/meshping
  restart_backoff_min: 1s
  restart_backoff_max: 30s

reconcile:
  interval: 30s

persist:
  path: /var/lib/meshd/state.json
  debounce: 60s
  max_delay: 300s

api:
  listen: "0.0.0.0:9101"

metrics:
  max_series: 200000
  rtt_buckets: [0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
  percentiles: [0.5, 0.9, 0.99]

log:
  level: info
  format: json
```

---

## 18. Change cost table

| Event | Slot sides changed | Time series affected |
|---|---|---|
| One host becomes unhealthy | One side per slot that held it | Those slots only |
| One host added to a zone | Zero, when `rebalance_on_add` is false | None, unless a slot was unfilled |
| Node pool scales up | Zero | None |
| Node pool scales down | One side per affected slot | Those slots only |
| `slots.count` raised | New slots only | New series added, existing untouched |
| Zone rule changed | All | All. This is a topology change, not a repair |
| New zone appears | New pairings only | New series added |
| HTTP fetch fails | Zero | None. The cached set stays active |
| `meshd` restart with state file present | Zero | None |
| `meshd` restart with state file lost | All | All |

---

## 19. Assumptions taken

These items were open in the previous discussion. The values below are the assumptions in this specification. Correct any that are wrong.

1. **Provider merging.** All providers merge into one inventory with one zone rule. Priority resolves ID collisions. A static host and a Kubernetes node can therefore appear in the same zone pairing.
2. **Super host selection.** By selector rule first, then by canonical order. Sticky until ineligible.
3. **Anchor rounding.** Rounds up by default. At `count: 3` and `anchor_ratio: 0.5` the result is 2 anchor slots and 1 diverse slot.
4. **Direction.** Both nodes probe the same slot from their own side, independently. No coordination.
5. **State sharing.** Nodes do not share state files. Each node holds its own copy.

---

## 20. Approval

On approval of this specification, the implementation order is:

1. `pkg/pingproto` and `cmd/meshping`.
2. `internal/config`, `internal/inventory`, `internal/zone`.
3. `internal/provider/file`, then `http`, then `k8s`.
4. `internal/pairing`, `internal/slot`, `internal/reconcile`, with table-driven tests against fixtures.
5. `internal/state`, `internal/health`.
6. `internal/probe`, `internal/responder`, `internal/runner`.
7. `internal/metrics`, `internal/api`, `cmd/meshd`.

