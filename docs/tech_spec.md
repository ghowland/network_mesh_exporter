# Mesh Network Test System — Technical Specification

**Version:** 1.0 (as implemented)
**Binaries:** `meshd`, `meshping`
**Language:** Go 1.22
**Module:** `github.com/example/mesh`

---

# Part 1 — What this system is

## 1.1 Purpose

The system measures network quality between groups of machines and publishes the results as Prometheus metrics. It answers questions of this form:

- What is the round trip time between data centre `sjc01` and data centre `sjc02` right now?
- Is packet loss between metro `sjc` and metro `iad` a property of the path, or of one bad machine at one end?
- Did the jitter between two regions change after the network change last Tuesday?

It measures RTT, jitter, loss, reorder, and TCP handshake time, over ICMP, UDP, and TCP.

## 1.2 The scaling problem it solves

The naive approach is a full mesh: every machine probes every other machine. For `n` machines this produces `n(n-1)/2` pairs. At 50 machines that is 1,225 pairs. At 500 machines it is 124,750 pairs, times three probe types, times two directions. The measurement traffic and the metric cardinality both become unmanageable.

The observation behind this system is that you rarely need machine-to-machine data. You need **location-to-location** data. Whether `sjc01` can reach `sjc02` is the operational question. Which specific machine in `sjc01` you asked is an implementation detail.

So the system groups machines into **zones**, forms pairs of **zones**, and then selects a small, fixed number of machine pairs to represent each zone pair. Adding 200 machines to `sjc01` does not add a single measurement, because the zone count did not change.

## 1.3 The single-bad-host problem it solves

If one zone pair is measured by exactly one machine pair, and that measurement degrades, you cannot tell whether the network path degraded or whether one of the two machines has a bad network card.

So each zone pair is measured by **N independent machine pairs**, called slots. If one slot degrades, the fault is at an endpoint. If all N degrade together, the fault is in the path.

## 1.4 The metric continuity problem it solves

A Prometheus time series is identified by its labels. If the machine pair behind a measurement changes, the labels change, the old series ends, and a new one begins. Graphs break. Alerts reset. Historical comparison is lost.

So the assignment of machines to slots is **sticky**. It changes only when it must, it changes as little as possible, and it survives process restarts. A machine going away clears exactly the slot sides that held it, and nothing else.

---

# Part 2 — Core concepts

Read this section before any other. Every part of the system is built from these five ideas.

## 2.1 Host record

A machine, as the system knows it. It has three important fields:

| Field | Example | Purpose |
|---|---|---|
| `ID` | `web-001.product.prod.sjc01.domain.com` | Unique name. Never changes for the life of the machine |
| `Address` | `10.4.2.17` | What the probe actually dials |
| `Attributes` | `{metro: sjc, dc_instance: 01, country: us, role: web, ...}` | An untyped bag of key-value pairs |

The critical design decision is that **`Attributes` is untyped**. There is no `Country` field, no `Metro` field, no `DataCenter` field anywhere in the code. There is only a map of strings to strings.

This is what allows the same code path to serve two very different discovery sources. The static source parses DNS names and fills in `metro`, `dc_instance`, `country`. The Kubernetes source copies every node label and fills in `k8s.region`, `k8s.zone`, `k8s.label.nodepool`. Neither knows what a zone is. The zone rule decides that later, from whatever attributes happen to exist.

## 2.2 Zone rule and zone key

A **zone rule** is an ordered list of attribute names. Applying it to a host produces a **zone key**.

```
rule:  [metro, dc_instance]
host:  {metro: sjc, dc_instance: 01, country: us, role: web}
key:   "sjc/01"
```

```
rule:  [k8s.region, k8s.zone]
host:  {k8s.region: us-west-2, k8s.zone: us-west-2a}
key:   "us-west-2/us-west-2a"
```

The mesh level is not a code path. It is a configuration value:

| Intent | Rule |
|---|---|
| Data centre to data centre | `[metro, dc_instance]` |
| Metro to metro | `[metro]` |
| Country to country | `[country]` |
| Cloud zone to cloud zone | `[k8s.region, k8s.zone]` |
| Full mesh, every machine separately | `[fqdn]` |

Full mesh is not a special case. It is the rule that puts exactly one machine in each zone, so a zone pair is a machine pair. All the same machinery applies.

If a host lacks an attribute the rule needs, it is **unresolved**: it gets no zone and participates in nothing. This is deliberate and visible. A typo in a label shows up as an unresolved host count rather than as a silently wrong topology.

## 2.3 Zone pairing

An unordered pair of two distinct zone keys. Its key is the two zone keys sorted alphabetically and joined with a vertical bar:

```
sjc/01|sjc/02
```

Sorting matters: it means the pairing key is identical on every node, regardless of which side computed it. That is a precondition for the determinism described in §2.6.

The set of all zone pairings is derived from the set of zone keys. Nothing about hosts enters this calculation. **Pairings are defined by rules, not by machines.**

## 2.4 Slot

A numbered container inside a zone pairing. It has two sides. Side A holds a host from zone A, side B holds a host from zone B.

```
Pairing "sjc/01|sjc/02"
  Slot 0 [anchor]   A: web-001...sjc01   B: web-001...sjc02
  Slot 1 [diverse]  A: web-002...sjc01   B: web-002...sjc02
```

The slot index is stable. The hosts inside it are replaceable. **Each side is cleared independently.** If `web-002...sjc02` disappears, slot 1 side B is cleared and refilled; slot 1 side A keeps `web-002...sjc01`, and slot 0 is untouched entirely.

This independent-side rule is the single most important behaviour in the system. It is what keeps a working measurement working when something unrelated breaks.

### Slot classes

| Class | Rule | Question it answers |
|---|---|---|
| **anchor** | All anchor slots in one pairing use the **same** host on each side | The endpoints never move, so a change in the measurement is a change in the path |
| **diverse** | Prefers a host not used elsewhere in this pairing | Spreads across machines, so one bad machine cannot represent the pairing |
| **super** | One designated host pairs with **every** host in the far zone | Compares one machine against a whole zone at once |

Anchor and diverse slots split the configured slot count by `anchor_ratio`. With `count: 4` and `anchor_ratio: 0.5`, you get 2 anchor and 2 diverse. Super slots are additional and are driven by the far zone's host count.

### Fill rank

When the scanner picks a host for an empty side, it records how good the choice was:

| Rank | Meaning |
|---|---|
| 1 | The host is used nowhere else on this side of this pairing |
| 2 | Reused, but at the lowest current use count |
| 3 | Reused, no better candidate existed |

The rank becomes a metric label. A query can then distinguish "these two measurements are truly independent" from "these two measurements share a machine, so a fault in it will appear in both."

## 2.5 The reconcile

One pure function that computes the entire assignment:

```
Reconcile(inventory, config, current_state, now) -> (new_state, delta)
```

It performs no I/O, holds no locks, and does not mutate its inputs. Every input arrives in one struct. The consequence is that it is fully reproducible: same inputs, same output, always.

It runs in seven steps:

1. **Resolve zones.** Apply the rule to every host. Build zone key set and per-zone member lists.
2. **Build desired pairings.** All pairs of distinct zones, filtered, limit-checked.
3. **Diff pairings.** New ones get an empty slot table. Vanished ones get a removal deadline. Existing ones keep their tables.
4. **Validate slot sides.** Clear each side whose host is gone, ineligible, or in the wrong zone. Never touch the other side.
5. **Update super slots.** Recompute super hosts and their target lists.
6. **Fill empty sides.** Anchors first (widest candidate set), then diverse.
7. **Emit delta.** The list of what changed.

The **delta** is the only thing anyone downstream consumes. The runner applies the delta. A task not mentioned in the delta is never restarted.

## 2.6 Determinism

Every node runs its own `meshd`. They do not talk to each other. They do not elect a leader. There is no consensus protocol.

They agree because the computation is deterministic:

- The inventory is sorted by host ID. This is the **canonical order**.
- The scan start offset is `fnv32(pairing_key) mod candidate_count`. Same key, same count, same offset, on every machine.
- The scan walks the canonical order from that offset and takes the first host at the best available rank.
- The pairing key is alphabetically sorted, so both sides compute the same key.

Given the same inventory, the same config, and the same prior state, two nodes produce the same assignment independently.

The start offset deserves a note. Without it, every pairing would scan from index 0, and the alphabetically first host in each zone would end up holding a slot side in every pairing that zone participates in. That host would carry all the probe load. The offset spreads the load across the candidate set while staying a pure function of the key.

---

# Part 3 — Architecture

## 3.1 Two binaries

| Binary | Privilege | Job |
|---|---|---|
| `meshd` | Unprivileged | Everything: discovery, topology, TCP and UDP probes, state, HTTP, metrics |
| `meshping` | `CAP_NET_RAW` | ICMP echo only. Reads JSON requests on stdin, writes JSON results on stdout |

ICMP needs a raw socket. Rather than run the whole system with that capability, the ICMP work is isolated in a small program with a narrow interface. `meshping` has no configuration parsing, no network client, no state, and no aggregation. It sends packets and returns raw microsecond samples. Every statistic is computed in `meshd`.

`meshd` starts one long-lived `meshping` process and multiplexes all ICMP work through it over pipes. If `meshping` cannot obtain permission, it says so in its hello message. `meshd` then disables ICMP, logs the reason once, sets `mesh_icmp_available` to 0, and continues with TCP and UDP. This is a normal, supported operating mode, not a failure.

## 3.2 Data flow

```
  file ──┐
  http ──┼──> Providers ──> Inventory Store ──> Snapshot
  k8s  ──┘                                          │
                                                    v
                                              Zone Resolver
                                                    │
                                                    v
   State (RAM) <──────────────────────────>  Reconciler
        │                                           │
        v                                           v delta
  JSON file (debounced)                          Runner
                                                    │
                                        ┌───────────┴──────────┐
                                        v                      v
                                   TCP / UDP              meshping (ICMP)
                                        │                      │
                                        └───────────┬──────────┘
                                                    v
                                             Metric Registry
                                                    │
                                                    v
                                              HTTP Server
                                          /metrics /state /tasks ...
```

## 3.3 Package layout

```
build.sh
go.mod
go.sum

cmd/meshd/main.go              wiring, lifecycle, shutdown order
cmd/meshd/providers.go         provider registration (lives here, see §3.4)
cmd/meshping/main.go           the entire ICMP helper

pkg/pingproto/proto.go         wire types, encoder, decoder — the only
                               package both binaries import

internal/config/config.go      every configuration struct
internal/config/defaults.go    every default value
internal/config/load.go        parse, normalise, validate
internal/config/watcher.go     SIGHUP and file-change reload

internal/inventory/inventory.go  HostRecord, Store, Snapshot, merge

internal/zone/zone.go          Rule, transforms, Index

internal/provider/provider.go  Provider interface, Manager
internal/provider/file/file.go     local JSON document
internal/provider/http/http.go     remote JSON document with cache
internal/provider/k8s/k8s.go       Kubernetes node watcher
internal/provider/k8s/types.go     list options alias

internal/pairing/pairing.go    pairing key, filter, Build
internal/slot/slot.go          classes, ranks, scanner, layout
internal/health/health.go      state machine, hysteresis, flap detection
internal/state/state.go        persisted structs, atomic load and save
internal/state/store.go        in-memory authority, debounced writer
internal/reconcile/reconcile.go  the pure function
internal/reconcile/loop.go     trigger coalescing, scheduling

internal/probe/probe.go        Kind, Target, Params, Cycle, Window, Stats
internal/probe/header.go       in-place header write for the responder
internal/probe/tcp/tcp.go      TCP prober
internal/probe/udp/udp.go      UDP prober
internal/probe/icmp/client.go  meshping supervisor and client

internal/responder/responder.go  UDP and TCP echo listeners
internal/runner/runner.go        task lifecycle
internal/metrics/metrics.go      registry and all metric definitions
internal/api/api.go              HTTP handlers
```

## 3.4 Why provider registration lives in `cmd/meshd`

`internal/provider` defines the `Provider` interface. The three sub-packages implement it, and therefore import the parent. If the parent also imported the children to construct them, that would be an import cycle, which Go forbids.

The rule: **a package that defines an interface must not import the packages that implement it.** Construction belongs in `main`, which is the only package that legitimately imports everything.

This was discovered during the build. It is documented here because it constrains any future provider.

---

# Part 4 — Discovery

Three sources, one output type. Everything downstream is identical regardless of source.

Each provider produces a complete host set. A provider update **replaces** that provider's entire set atomically. There is no incremental update, so a slow or failing source can never produce a half-applied topology.

The inventory is the union across providers. If two providers claim the same host ID, the higher `priority` wins and a collision counter increments.

## 4.1 File provider

Reads a JSON document from local disk. Re-reads on modification and on interval. A parse failure keeps the current set in place.

```json
{
  "version": 1,
  "site_table": {
    "sjc01": {
      "country": "us",
      "metro": "sjc",
      "dc_label": "sjc-equinix-sv5",
      "dc_instance": "01"
    }
  },
  "hosts": [
    { "name": "web-001.product.prod.sjc01.domain.com", "enabled": true }
  ]
}
```

### DNS name parsing

The name format defaults to `[role_ordinal, service, environment, site]`. Applied to `web-001.product.prod.sjc01.domain.com`:

| Label | Field | Produces |
|---|---|---|
| `web-001` | `role_ordinal` | `role=web`, `ordinal=001` |
| `product` | `service` | `service=product` |
| `prod` | `environment` | `environment=prod` |
| `sjc01` | `site` | `site=sjc01` |
| `domain.com` | remainder | `domain=domain.com` |

Plus `fqdn` and `hostname`.

### Site table enrichment

The DNS name cannot carry the country or the data centre label — those are internal facts. The `site_table` supplies them. The `site` value is looked up, and the entry's fields are merged into the attributes.

If the site is absent from the table, a fallback splits the token on the alphabetic-to-numeric boundary: `sjc01` becomes `metro=sjc`, `dc_instance=01`. Country and DC label stay unset. A zone rule that needs them will mark the host unresolved, which is the correct and visible outcome.

Per-host `attributes` in the document override everything derived, so one machine can be corrected without a schema change.

## 4.2 HTTP provider

Fetches the same document schema from a URL. This is the source of truth when nodes should not carry a local copy.

### Fetch behaviour

| Response | Action |
|---|---|
| 200 | Parse, validate, replace the set, rewrite the cache |
| 304 | Keep the current set, do not touch the cache |
| Non-2xx or network error | Keep the current set, back off exponentially |
| Parse or validation failure | Keep the current set, never apply the bad document |

Conditional requests use `If-None-Match` and `If-Modified-Since` from the previous response.

### The cache

The cache is what makes this source safe.

- On start, the cache is read and applied **before** the first fetch. The node is operational before the network is.
- Writes are atomic: temp file in the same directory, `fsync`, `rename`. A crash mid-write cannot corrupt it.
- A sidecar `.meta.json` holds the ETag, `Last-Modified`, and fetch time.
- `cache_max_age` (default 24h) marks the set stale when exceeded. **Stale hosts stay in the inventory.** Staleness is reported through a gauge; it does not clear slots. Losing contact with the config server must not tear down the measurement mesh.

The auth header file is re-read on every fetch, so a rotated token is picked up without a restart.

## 4.3 Kubernetes provider

Lists and watches `Node` objects. In-cluster credentials, or a kubeconfig path.

Events are debounced (default 2s), so a rolling node update does not emit one host set per node.

### Attribute mapping

| Attribute key | Source |
|---|---|
| `k8s.cluster` | config value |
| `k8s.node` | node name |
| `k8s.region` | `topology.kubernetes.io/region` |
| `k8s.zone` | `topology.kubernetes.io/zone` |
| `k8s.instance-type` | `node.kubernetes.io/instance-type` |
| `k8s.arch`, `k8s.os`, `k8s.hostname` | corresponding standard labels |
| `k8s.label.<name>` | **every** node label |
| `k8s.annotation.<name>` | annotations on the allow list (default: none) |
| `k8s.provider-id`, `k8s.kubelet` | node spec and status |

Every label is copied with a prefix, and well-known ones are additionally copied to short keys. The zone rule can then use either. The provider imposes no schema, which is exactly the point: a cluster with a custom `datacenter` label works with `zone.keys: [k8s.label.datacenter]` and no code change.

Host ID is `k8s://<cluster>/<node>`, which keeps it distinct from a DNS-named host in the same inventory.

Address comes from `status.addresses` by preference order, default `[InternalIP, Hostname]`.

### Health signals

| Signal | Effect |
|---|---|
| `Ready` condition not `True` | Ineligible |
| `spec.unschedulable` | Ineligible |
| Taint on the deny list | Ineligible |
| `deletionTimestamp` set | Ineligible immediately |
| Absent from resync | Ineligible after `missing_grace` |

These are **provider-authoritative**: they bypass hysteresis and take effect at once. The cluster knows more about its own nodes than a probe does.

---

# Part 5 — Health and hysteresis

This subsystem exists to stop a brief failure from rewriting the assignment.

## 5.1 States

| State | Eligible for a slot? | Meaning |
|---|---|---|
| `unknown` | **Yes** | Seen but not yet probed |
| `healthy` | **Yes** | Probes succeeding |
| `suspect` | **Yes** | Failing, below `unhealthy_after` |
| `pending` | **Yes** | Marked unhealthy, inside `release_hold` |
| `unhealthy` | No | Hold expired, slot sides may be cleared |
| `cooldown` | No | Flapped too often, held out |
| `ineligible` | No | Provider said so |

Note that four states are eligible. **A host is not released the moment it fails.** It passes through `suspect` (probe failures accumulating), then `pending` (marked, but held), and only reaches `unhealthy` after `release_hold` expires. Only then does the reconcile clear its slot sides.

`unknown` is eligible because a freshly discovered host has never been probed. If it were ineligible, no slot could ever be filled on a fresh start.

## 5.2 Timers

| Setting | Default | Effect |
|---|---|---|
| `unhealthy_after` | 3 cycles | Failures before the mark |
| `release_hold` | 60s | Delay between mark and slot clear |
| `healthy_after` | 2 cycles | Successes before eligible again |
| `initial_grace` | 90s | New host cannot be marked unhealthy |
| `missing_grace` | 60s | Grace for absence from a provider |
| `dns_grace` | 120s | Grace for unresolvable address |
| `flap_threshold` | 3 in 10min | Eligibility transitions before cooldown |
| `flap_cooldown` | 15min | Held ineligible after flapping |
| `pairing_removal_hold` | 300s | Delay before a vanished pairing is deleted |

**`release_hold` is the important one.** Without it, a ten-second network event rewrites slot assignments and breaks the time series in the exact window where the measurement matters most.

**Flap detection** counts eligibility transitions in a rolling window. A host that crosses the threshold is forced into cooldown, so an unstable machine cannot cause repeated reassignment churn.

The reconcile loop asks the tracker for its earliest pending deadline and sleeps until exactly then. It does not poll for expiry.

---

# Part 6 — Persistence

The authoritative state lives in RAM as Go structs. The JSON file is a durable copy.

| Aspect | Behaviour |
|---|---|
| Load | On start, before the first reconcile |
| Dirty | Set by any reconcile with a non-empty delta |
| Debounce | Timer resets on each change; default 60s |
| Cap | `max_delay` (default 300s) bounds total postponement so continuous churn cannot prevent a write |
| Write | Temp file in same directory, `fsync`, `rename` |
| Missing file | Not an error. Empty state, assign from scratch |
| Corrupt or version mismatch | Empty state, log a topology reset, increment `mesh_state_reset_total` |
| Shutdown | Immediate write if dirty |

Write rate is bounded by `60 / debounce_seconds` per minute, because each write requires a full quiet window. A 60s debounce caps at one write per minute; 3s caps at 20.

The file holds pairing keys, slot indices, classes, assigned host IDs, ranks, timestamps, and super host lists. **It holds no measurements.**

## 6.1 Fingerprints

The state carries two fingerprints:

- **Zone rule fingerprint** — a hash of the rule specification.
- **Slot config fingerprint** — a string encoding count, ratio, rounding, super settings, reuse.

If either differs from the current configuration at load time, the entire slot table is discarded and rebuilt. The reason: a changed rule redefines what a slot *means*. Keeping the old table would produce assignments the new rule would never make.

## 6.2 Why `/state` reads RAM

The API reads the in-memory structs under a read lock, not the file. A request during the debounce window therefore returns current data, not data up to 60 seconds old.

---

# Part 7 — Measurement

## 7.1 Task model

A **task** is one directed probe of one type on one slot:

```
TaskKey{Pairing, Slot, Kind, Forward}
```

Each slot produces two directed tasks per enabled probe type: A→B and B→A. **Each node runs only the tasks where it is the source.** The forward and reverse paths are not always the same, and one direction can be lossy while the other is clean, so both are measured independently with no coordination between the two nodes.

Each task owns one goroutine and one ticker. Its first tick is offset by `fnv32(taskkey) mod cycle`, so all tasks do not fire simultaneously and produce a synchronised burst.

## 7.2 Delta application

When the reconcile produces a delta, the runner:

- **Starts** tasks that are new
- **Stops** tasks that are gone
- **Leaves alone** tasks whose endpoints and parameters are unchanged
- **Restarts with a fresh window** tasks whose endpoints changed

The identity check is `src_host > dst_host @ dst_addr` plus the parameter struct. If any of that changed, the task measures a different thing, so mixing old and new samples in one window would be wrong.

When a task stops, its metric series are deleted from the registry. A replaced host leaves nothing stale behind.

## 7.3 Payload format

UDP and TCP echo probes share a 16-byte header:

| Bytes | Content |
|---|---|
| 0–3 | Magic `6d 65 73 68` ("mesh") |
| 4–7 | Sequence number |
| 8–15 | Send timestamp, nanoseconds |
| 16+ | Padding to `payload_bytes` |

The magic value lets the responder reject unrelated traffic. The responder echoes the packet at its original size, overwriting only the timestamp with its receive time, so the reply path carries the same packet size as the request path.

## 7.4 Probers

### TCP

Two modes.

**`connect`** (default): dial, measure the handshake, close immediately. No payload. Needs only a listener on the far side, not a responder. This is the safest default — it works against any TCP service.

**`echo`**: dial, send the payload, read it back, and report handshake time and payload round trip **separately**. Requires the mesh responder on the far side.

Each iteration opens its own connection. A reused connection would measure only the payload path and would hide a handshake failure.

### UDP

One socket per cycle. Sends `count` packets at `interval`, then reads until the timeout. Replies are matched by sequence number.

- Unmatched sequence → **loss**
- Reply arriving after a higher sequence already seen → **reorder**

Samples are reordered into send order before being handed to the window, because the jitter calculation reads consecutive samples in send order.

### ICMP

`meshd` sends a JSON request to `meshping` and waits for the matching result:

```json
{"type":"ping","id":"42","target":"10.0.4.12","count":10,
 "interval_ms":1000,"payload_bytes":56,"timeout_ms":1000,"ttl":64}
```

```json
{"type":"result","id":"42","target":"10.0.4.12","resolved":"10.0.4.12",
 "sent":10,"received":9,"lost":1,"reordered":0,
 "rtt_us":[1204,1190,1250,1198,1310,1201,1188,1240,1199],
 "error":"","error_class":""}
```

A request with no result within `count × (interval + timeout) + 5s` is abandoned and a cancel is sent, so a lost result never leaks a goroutine or a map entry.

#### meshping internals

Sockets are opened in this order:

1. Unprivileged ICMP datagram socket (`udp4`/`udp6`). Works when `net.ipv4.ping_group_range` includes the running group. **No capability needed.**
2. Raw socket (`ip4:icmp`). Needs `CAP_NET_RAW`.
3. Neither → report `priv: none` in hello and exit.

Replies are matched by **sequence number, not by ICMP identifier**. On an unprivileged datagram socket the kernel rewrites the identifier, so identifier matching would drop every reply in that mode. The payload magic value provides the second check.

The do-not-fragment bit is accepted in the protocol and ignored, because setting it is not portable through `golang.org/x/net/icmp`. TTL is applied.

## 7.5 Rolling window

Each task owns a `Window` of duration `probes.window` (default 60s). It holds:

- Individual RTT samples with timestamps, in send order
- Per-cycle counters: sent, received, lost, reordered, error class

Expired entries are dropped on each add and on each read.

Computed statistics:

| Value | Definition |
|---|---|
| Min, Max, Mean | Over successful samples |
| Percentiles | Nearest-rank over the sorted samples; default p50, p90, p99 |
| **Jitter** | Mean absolute difference between **consecutive** samples in send order |
| Loss ratio | Lost ÷ sent |
| Connect mean | TCP handshake time, separate from round trip |
| Last success | Timestamp of the most recent received sample |

Jitter requires send order, which is why the UDP prober reorders its results before handing them over.

## 7.6 Responder

UDP and TCP echo listeners, enabled by default. Without them, a node can measure outward but cannot be measured, and the reverse direction of every slot it participates in would be blank.

- **UDP:** read, verify magic, stamp receive time, write back at the original size.
- **TCP:** accept, echo each payload, 30-second read deadline. The deadline bounds an idle connection, so a `connect`-mode prober that never sends a payload does not hold a goroutine.

Rejected packets — those failing the magic check — are counted separately, which distinguishes "nothing is arriving" from "the wrong thing is arriving."

---

# Part 8 — Metrics

## 8.1 Probe labels

Every probe metric carries:

| Label | Example |
|---|---|
| `zone_src` | `sjc/01` |
| `zone_dst` | `sjc/02` |
| `host_src` | `web-001.product.prod.sjc01.domain.com` |
| `host_dst` | `web-001.product.prod.sjc02.domain.com` |
| `slot` | `0` |
| `class` | `anchor` |
| `reuse_rank` | `1` |
| `probe` | `icmp` |

The `slot` label is what makes N useful:

```promql
# Is the path bad?
avg by (zone_src, zone_dst) (mesh_rtt_mean_seconds{probe="icmp"})

# Is one host bad?
mesh_rtt_mean_seconds{zone_src="sjc/01", zone_dst="sjc/02", probe="icmp"}
```

The first aggregates across slots and gives the zone pair answer. The second keeps the slot label and shows whether one slot differs from the others.

## 8.2 Probe metrics

| Metric | Type |
|---|---|
| `mesh_rtt_seconds` | Histogram |
| `mesh_rtt_min_seconds`, `_max_`, `_mean_` | Gauge |
| `mesh_rtt_quantile_seconds` | Gauge, extra `quantile` label |
| `mesh_jitter_seconds` | Gauge |
| `mesh_loss_ratio` | Gauge |
| `mesh_packets_sent_total`, `_received_`, `_lost_` | Counter |
| `mesh_reorder_total` | Counter |
| `mesh_tcp_connect_seconds` | Histogram |
| `mesh_probe_errors_total` | Counter, extra `err_class` label |
| `mesh_probe_last_success_timestamp_seconds` | Gauge |

The error-class label is named `err_class`, not `class`, because `class` is already taken by the slot class. Prometheus rejects duplicate label names on one metric.

## 8.3 System metrics

| Metric | Meaning |
|---|---|
| `mesh_hosts_total{source,state}` | Inventory by source and health state |
| `mesh_hosts_unresolved` | Hosts with no zone under the current rule |
| `mesh_zones_total`, `mesh_pairings_total` | Topology size |
| `mesh_slots_total{class}`, `mesh_slots_unfilled` | Slot table state |
| `mesh_slot_changes_total{reason}` | Assignment churn, by cause |
| `mesh_reconcile_duration_seconds`, `_total{trigger}`, `_errors_total` | Reconcile behaviour |
| `mesh_provider_fetch_total{source,result}` | Discovery success and failure |
| `mesh_provider_cache_age_seconds{source}` | HTTP cache freshness |
| `mesh_state_persist_total`, `_failures_total`, `mesh_state_dirty` | Persistence |
| `mesh_state_reset_total` | Times the assignment restarted from scratch |
| `mesh_icmp_available` | 1 when meshping has permission |
| `mesh_meshping_restarts_total` | Helper stability |
| `mesh_tasks_running` | Tasks on this node |
| `mesh_series_count`, `mesh_series_dropped_total` | Cardinality guard |
| `mesh_responder_total{kind}` | Responder activity |

## 8.4 Cardinality

Series count is approximately:

```
pairings × (slots + super_slots) × probe_types × 2 directions
```

`metrics.max_series` (default 200,000) caps registration. Beyond it, samples are dropped and `mesh_series_dropped_total` increments. A wrong zone rule degrades observability rather than exhausting memory.

`pairings.max_pairings` (default 5,000) is the earlier guard: if the desired pairing count exceeds it, the reconcile aborts and **keeps the previous assignment**. A configuration mistake cannot replace a working topology with an unworkable one.

---

# Part 9 — HTTP API

Listens on `api.listen`, default `0.0.0.0:9101`.

| Path | Method | Content |
|---|---|---|
| `/metrics` | GET | Prometheus exposition |
| `/state` | GET | Pairings, slots, persistence stats, last delta |
| `/inventory` | GET | Host records with resolved zone keys, plus source status |
| `/zones` | GET | Zone keys, members, unresolved hosts with the missing key |
| `/pairings` | GET | Pairing keys with fill status, super host lists |
| `/health` | GET | Per-host state and timers |
| `/tasks` | GET | Running tasks with live window statistics |
| `/config` | GET | Effective config after defaults, secrets redacted |
| `/reconcile` | POST | Force a reconcile, returns the delta |
| `/refresh` | POST | Force a provider poll, optional `?source=` |
| `/livez` | GET | Process is running |
| `/readyz` | GET | A provider has produced hosts and a reconcile has completed |

`/tasks` is the most useful debugging endpoint. It shows what this node is actually measuring right now and what the results look like, without going through Prometheus.

`/zones` is second. Its `unresolved` list names the specific attribute each excluded host is missing, which turns a silent topology problem into a named one.

---

# Part 10 — Configuration reference

```yaml
node_id: web-001.product.prod.sjc01.domain.com   # must match a host ID exactly

providers:
  file:
    enabled: true
    path: /etc/meshd/inventory.json
    interval: 60s
    priority: 10
  http:
    enabled: false
    url: https://config.example.com/mesh/inventory.json
    interval: 60s
    timeout: 10s
    cache_path: /var/lib/meshd/inventory.json
    cache_max_age: 24h
    auth_header_file: /etc/meshd/token
    ca_file: ""
    insecure_tls: false
    backoff_min: 5s
    backoff_max: 300s
    priority: 20
  k8s:
    enabled: false
    cluster_name: prod-usw2
    kubeconfig: ""
    resync: 300s
    interval: 60s
    debounce: 2s
    label_selector: ""
    address_order: [InternalIP, Hostname]
    taint_deny:
      - node.kubernetes.io/unreachable
      - node.kubernetes.io/not-ready
      - node.kubernetes.io/unschedulable
    annotation_allow: []
    priority: 30

zone:
  keys: [metro, dc_instance]     # or [{key: k8s.zone, transform: lower}]
  separator: "/"
  missing: exclude               # exclude | empty | literal:<value>

pairings:
  intra_zone: false
  include: []                    # glob patterns against the pairing key
  exclude: []
  max_pairings: 5000

slots:
  count: 4
  anchor_ratio: 0.5
  anchor_rounding: up            # up | down
  super_hosts: 0
  super_selector: {}
  super_max_targets: 50
  allow_reuse: true
  rebalance_on_add: false        # accepted, not yet acted on
  reclaim: false                 # accepted, not yet acted on

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

probes:
  cycle: 15s
  window: 60s
  icmp:
    enabled: true
    interval: 1s
    count: 10
    payload_bytes: 56
    timeout: 1s
    ttl: 64
    df: false                    # accepted, not applied
  udp:
    enabled: true
    interval: 1s
    count: 10
    payload_bytes: 64
    port: 8472
    timeout: 1s
  tcp:
    enabled: true
    interval: 5s
    count: 5
    payload_bytes: 64
    port: 9100
    timeout: 2s
    mode: connect                # connect | echo

responder:
  enabled: true
  udp_listen: "0.0.0.0:8472"
  tcp_listen: "0.0.0.0:9100"

meshping:
  path: /usr/local/bin/meshping
  restart_backoff_min: 1s
  restart_backoff_max: 30s
  hello_timeout: 5s

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
  level: info                    # debug | info | warn | error
  format: json                   # json | text
```

## 10.1 Zone transforms

Each key can carry a transform:

| Transform | Effect |
|---|---|
| `lower`, `upper`, `trim` | Case and whitespace |
| `prefix:<n>` | Keep the first n characters |
| `regex:<pattern>:<replacement>` | Full regex rewrite |

Example: `{key: k8s.zone, transform: "regex:^(.*)-[a-z]$:$1"}` turns `us-west-2a` into `us-west-2`, collapsing availability zones into regions without a second attribute.

## 10.2 Reload

`SIGHUP` or file modification triggers a reload. A configuration that fails validation is **rejected** and the previous one stays active. Validation reports every problem at once, not just the first.

A reload that changes `zone`, `pairings`, or `slots` logs a warning, because it will rebuild the entire slot table and break every time series. Other reloads apply without disruption.

Use `meshd -check -config <path>` to validate without starting.

---

# Part 11 — Build

**File: `build.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

go mod tidy
go vet ./...

go build -o ./meshd ./cmd/meshd
go build -o ./meshping ./cmd/meshping

if command -v setcap >/dev/null 2>&1 && [ "$(id -u)" -eq 0 ]; then
    setcap cap_net_raw+ep ./meshping
fi

ls -l ./meshd ./meshping
```

`set -euo pipefail` means a `go vet` failure aborts the script before `go build` runs. If you see stale behaviour from a binary after editing source, check the script's exit code — a silent vet failure leaves the old binary in place.

## 11.1 ICMP permission

Three options, in order of preference:

**Unprivileged datagram socket.** Check `sysctl net.ipv4.ping_group_range`. If it includes your group ID, nothing further is needed.

**Capability.** `sudo setcap cap_net_raw+ep ./meshping`. Grants exactly one capability to one small program.

**Neither.** ICMP is disabled, `mesh_icmp_available` reads 0, TCP and UDP continue. This is a supported mode.

Setuid is **not** supported. The privilege-drop path returns an error rather than attempting a partial drop.

## 11.2 Dependencies

| Module | Purpose |
|---|---|
| `github.com/prometheus/client_golang` | Metric registry and exposition |
| `golang.org/x/net` | ICMP packet construction and parsing |
| `gopkg.in/yaml.v3` | Configuration parsing |
| `k8s.io/client-go`, `k8s.io/api`, `k8s.io/apimachinery` | Node informer |

---

# Part 12 — Operational behaviour

## 12.1 Startup sequence

1. Parse and validate configuration. Failure exits with code 2.
2. Build the metric registry.
3. Compile the zone rule, pairing filter, and slot scanner. Failure exits with code 3.
4. **Load the state file.** Assignments survive the restart.
5. Start the persistence writer.
6. Start providers.
7. Start `meshping` if ICMP is enabled.
8. Start the responder.
9. Start the runner.
10. Start the reconcile loop, which runs immediately.
11. Start the config watcher.
12. Start the HTTP server.

The state loading in step 4 is why a restart produces no time-series break: the same hosts hold the same slots, the delta is empty, and the runner starts exactly the tasks that were running before.

## 12.2 Shutdown

`SIGINT` or `SIGTERM`. Reverse order: HTTP server, runner (each task's context cancelled), `meshping`, responder, reconcile loop, providers, **final state write**. Bounded by a 15-second grace period.

## 12.3 Reconcile triggers

| Trigger | Cause |
|---|---|
| `start` | Process start |
| `provider` | Inventory generation advanced |
| `config` | Configuration reloaded |
| `health` | Host eligibility changed |
| `timer` | A health hold expired |
| `tick` | Periodic safety net |
| `api` | POST to `/reconcile` |

Triggers are coalesced through a capacity-1 channel. Ten triggers during a run produce exactly one further run, not ten.

## 12.4 Change cost

| Event | Slot sides changed | Series affected |
|---|---|---|
| One host unhealthy | One per slot that held it | Those slots only |
| One host added | Zero (default) | None |
| Node pool scales up | Zero | None |
| Node pool scales down | One per affected slot | Those slots only |
| `slots.count` raised | New slots only | New series added, existing untouched |
| Zone rule changed | All | All — this is a topology change, not a repair |
| New zone appears | New pairings only | New series added |
| HTTP fetch fails | Zero | None; the cache stays active |
| Restart with state file | Zero | None |
| Restart without state file | All | All |

Row 2 is a policy choice. `rebalance_on_add: false` keeps existing valid assignments even when a better-ranked candidate appears. Setting it true would improve coverage at the cost of breaking those series. The default favours continuity.

## 12.5 Failure modes

| Symptom | Cause | Where to look |
|---|---|---|
| `mesh_tasks_running` is 0 | `node_id` does not match a host ID | `/inventory`, `/tasks` |
| Hosts in `unresolved` | Zone rule needs an attribute the host lacks | `/zones`, field `missing_key` |
| UDP loss ratio 1.0 | Responder not listening, or wrong port | `mesh_responder_total`, `/config` |
| `mesh_icmp_available` is 0 | `meshping` has no permission | Startup log, `getcap` |
| `mesh_slots_unfilled` above 0 | Zone has too few eligible hosts for the slot count | `/pairings`, `/health` |
| `mesh_reconcile_errors_total` rising | Pairing count exceeds `max_pairings` | Log message names the counts |
| `mesh_provider_cache_age_seconds` rising | HTTP source unreachable, running on cache | `mesh_provider_fetch_total{result="failure"}` |
| `mesh_series_dropped_total` rising | Cardinality limit reached | `mesh_series_count`, review zone rule |
| High `mesh_slot_changes_total{reason="host_unhealthy"}` | Assignment churn | `/health` for flapping hosts |

---

# Part 13 — Deployment

## 13.1 One process per node

Every node runs its own `meshd` with its own `node_id`. Each computes the full topology, then filters down to the tasks where it is the source.

Nodes do **not** share state files. Each holds its own copy. Divergence after a lost state file is possible and is visible in the metrics, because the slot host labels differ between nodes.

## 13.2 Sizing

Per-node task count:

```
tasks ≈ (slots where this node is an endpoint) × (enabled probe types)
```

For a node in a zone of 20 machines, with 4 slots per pairing and 10 zones, this node holds roughly `4 × 9 / 20 ≈ 2` slot sides, giving about 6 tasks across three probe types. Task count per node stays low regardless of fleet size, because slots per pairing is fixed and hosts share the load.

## 13.3 Firewall

| Direction | Port | Protocol |
|---|---|---|
| Inbound | 8472 | UDP, from mesh nodes |
| Inbound | 9100 | TCP, from mesh nodes |
| Inbound | 9101 | TCP, from Prometheus |
| Inbound | — | ICMP echo request, from mesh nodes |
| Outbound | Same set, to mesh nodes | |

If ICMP is blocked, disable it in configuration rather than leaving it enabled and failing.

## 13.4 Prometheus scrape

```yaml
scrape_configs:
  - job_name: mesh
    scrape_interval: 30s
    static_configs:
      - targets: ['web-001.product.prod.sjc01.domain.com:9101']
```

Scrape interval should be at or below `probes.window`, so no window's worth of data is missed.

---

# Part 14 — Known limitations

| Item | Status |
|---|---|
| `rebalance_on_add` | Accepted, validated, fingerprinted; the reconcile does not act on it. Defaults false |
| `reclaim` | Same. Defaults false |
| `probes.icmp.df` | Accepted; not applied. Setting the DF bit is not portable through `golang.org/x/net/icmp`. Path MTU testing would need platform-specific socket options |
| Setuid mode for `meshping` | Not supported. Use `setcap` or the unprivileged datagram socket |
| One-way delay | The responder stamps its receive time into the reply header, but `meshd` does not yet compute one-way delay from it. The wire format supports it |
| Cross-node state sharing | Not implemented and not required. Determinism substitutes for it, but a node with a lost state file will re-derive an assignment that may differ from its peers' view |
| IPv6 | `meshping` opens an IPv6 socket when available and the protocol carries an `ipv6` flag, but address family selection is driven by resolution rather than by configuration |

---

# Part 15 — Reading the code

Suggested order for a new reader:

1. **`internal/inventory/inventory.go`** — `HostRecord`. Note the untyped `Attributes` map. Everything follows from that decision.
2. **`internal/zone/zone.go`** — how a rule turns attributes into a zone key.
3. **`internal/pairing/pairing.go`** — how zone keys become pairings. Short file.
4. **`internal/slot/slot.go`** — classes, ranks, and `StartOffset`. The scan is the heart of the determinism.
5. **`internal/reconcile/reconcile.go`** — the pure function. Read `validateSides` and `fillSides` closely; the independent-side rule lives there.
6. **`internal/health/health.go`** — the state machine. Read `Status.Eligible` and note which states return true.
7. **`internal/runner/runner.go`** — `desired` and `Apply`. How a delta becomes running goroutines.
8. **`internal/probe/probe.go`** — the `Window` and its statistics.
9. **`cmd/meshd/main.go`** — how it is all wired together.

`cmd/meshping/main.go` is independent of everything else and can be read alone.
