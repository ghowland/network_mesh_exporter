# Mesh Network Test System — User Manual

---

# Table of contents

1. Installation
2. Understanding the mental model
3. Scenario A — Two data centres, static server list
4. Scenario B — Metro-level testing across a large fleet
5. Scenario C — Kubernetes cluster, zone to zone
6. Scenario D — Hybrid: bare metal and Kubernetes in one mesh
7. Scenario E — Centralised inventory over HTTP
8. Scenario F — Full mesh for a small fleet
9. Scenario G — Super hosts: one machine against a whole zone
10. Scenario H — Intra-zone testing
11. Protocol configuration in depth
12. Slot tuning
13. Health and stability tuning
14. Operating the system
15. Prometheus queries
16. Troubleshooting

---

# 1. Installation

## 1.1 Build

```bash
git clone <your-repo> mesh
cd mesh
./build.sh
```

Produces `./meshd` and `./meshping` in the repository root.

## 1.2 Install

```bash
sudo install -m 0755 meshd    /usr/local/bin/meshd
sudo install -m 0755 meshping /usr/local/bin/meshping
sudo mkdir -p /etc/meshd /var/lib/meshd
```

## 1.3 ICMP permission

Check whether you need to do anything:

```bash
sysctl net.ipv4.ping_group_range
```

If the output range includes your service account's group ID, `meshping` opens an unprivileged ICMP datagram socket and no further action is required.

Otherwise grant the capability:

```bash
sudo setcap cap_net_raw+ep /usr/local/bin/meshping
getcap /usr/local/bin/meshping
```

Expected output: `/usr/local/bin/meshping cap_net_raw=ep`

If you cannot or will not grant it, set `probes.icmp.enabled: false` in your configuration. TCP and UDP measurement continues normally. `meshd` will also handle this automatically — it logs the reason once, sets `mesh_icmp_available` to 0, and carries on — but disabling it explicitly avoids a confusing warning at every start.

## 1.4 systemd unit

**File: `/etc/systemd/system/meshd.service`**

```ini
[Unit]
Description=Mesh network test daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=meshd
Group=meshd
ExecStart=/usr/local/bin/meshd -config /etc/meshd/meshd.yaml
Restart=always
RestartSec=5
StateDirectory=meshd
ReadWritePaths=/var/lib/meshd

NoNewPrivileges=false
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

`NoNewPrivileges` must be `false`, otherwise the kernel strips the capability from `meshping` when `meshd` executes it.

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin meshd
sudo chown -R meshd:meshd /var/lib/meshd
sudo systemctl daemon-reload
sudo systemctl enable --now meshd
```

## 1.5 Firewall

| Direction | Port | Protocol | Source or destination |
|---|---|---|---|
| Inbound | 8472 | UDP | Other mesh nodes |
| Inbound | 9100 | TCP | Other mesh nodes |
| Inbound | 9101 | TCP | Prometheus servers |
| Inbound | — | ICMP echo request | Other mesh nodes |
| Outbound | 8472, 9100 | UDP, TCP | Other mesh nodes |
| Outbound | — | ICMP echo request | Other mesh nodes |

---

# 2. Understanding the mental model

Before writing any configuration, understand these four steps. Every scenario in this manual is a variation of the same four.

## Step 1 — Machines become host records

A host record has an ID, an address, and a bag of untyped attributes. Where those attributes come from depends on the provider:

| Provider | Attributes come from |
|---|---|
| `file` / `http` | Parsing the DNS name, plus a site table lookup |
| `k8s` | Every node label, every allowed annotation, and standard topology labels |

Neither provider knows what a zone is. It just fills the bag.

## Step 2 — The zone rule turns attributes into a zone key

```
zone:
  keys: [metro, dc_instance]
```

Applied to a host with `metro=sjc, dc_instance=01`, this produces the zone key `sjc/01`.

**Changing the mesh level means changing this list.** There is no separate "DC mode" or "metro mode" in the code.

## Step 3 — Zone keys become zone pairings

Every unordered pair of distinct zone keys becomes a pairing. Two zones give one pairing. Ten zones give 45.

Note what this means for scale: adding 500 machines to an existing zone adds **zero** pairings.

## Step 4 — Slots fill each pairing

Each pairing gets N slots. The scanner picks hosts to fill each side, preferring hosts not already used in that pairing. Assignments are sticky and survive restarts.

---

# 3. Scenario A — Two data centres, static server list

**Situation:** You run bare metal in two data centres in the same metro. You want to know the network quality between them.

## 3.1 Naming

Your hosts follow this pattern:

```
web-001.product.prod.sjc01.example.com
      │       │       │    │
      │       │       │    └── site: metro sjc, DC instance 01
      │       │       └─────── environment
      │       └─────────────── service
      └─────────────────────── role and ordinal
```

## 3.2 Inventory

**File: `/etc/meshd/inventory.json`**

```json
{
  "version": 1,
  "site_table": {
    "sjc01": {
      "country": "us",
      "metro": "sjc",
      "dc_label": "sjc-equinix-sv5",
      "dc_instance": "01"
    },
    "sjc02": {
      "country": "us",
      "metro": "sjc",
      "dc_label": "sjc-digital-sjc2",
      "dc_instance": "02"
    }
  },
  "hosts": [
    { "name": "web-001.product.prod.sjc01.example.com" },
    { "name": "web-002.product.prod.sjc01.example.com" },
    { "name": "web-003.product.prod.sjc01.example.com" },
    { "name": "db-001.product.prod.sjc01.example.com" },
    { "name": "web-001.product.prod.sjc02.example.com" },
    { "name": "web-002.product.prod.sjc02.example.com" },
    { "name": "web-003.product.prod.sjc02.example.com" },
    { "name": "db-001.product.prod.sjc02.example.com" }
  ]
}
```

### Why the site table exists

The DNS name contains `sjc01`. It does **not** contain the country or the human-readable data centre label — those are internal facts your DNS has no reason to publish. The site table supplies them.

If a site is missing from the table, the system falls back to splitting the token at the alphabetic-to-numeric boundary: `sjc01` becomes `metro=sjc`, `dc_instance=01`. Country and DC label stay unset. That fallback is enough for the zone rule in this scenario, but a rule using `country` would mark those hosts unresolved.

### Disabling a host

```json
{ "name": "web-003.product.prod.sjc01.example.com", "enabled": false }
```

Disabled hosts are never assigned to a slot. Use this for maintenance rather than deleting the entry, so the record stays visible.

### Overriding an attribute

```json
{
  "name": "web-001.product.prod.sjc01.example.com",
  "attributes": { "rack": "a17", "uplink": "10g" }
}
```

Explicit attributes override derived ones. Add anything you might want a zone rule to use later.

## 3.3 Configuration

**File: `/etc/meshd/meshd.yaml`**

```yaml
node_id: web-001.product.prod.sjc01.example.com

providers:
  file:
    enabled: true
    path: /etc/meshd/inventory.json
    interval: 60s
  http:
    enabled: false
  k8s:
    enabled: false

zone:
  keys: [metro, dc_instance]
  separator: "/"
  missing: exclude

pairings:
  intra_zone: false
  max_pairings: 100

slots:
  count: 4
  anchor_ratio: 0.5

health:
  release_hold: 60s

probes:
  cycle: 15s
  window: 60s
  icmp:
    enabled: true
    interval: 1s
    count: 10
    payload_bytes: 56
    timeout: 1s
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
    port: 9100
    timeout: 2s
    mode: connect

responder:
  enabled: true
  udp_listen: "0.0.0.0:8472"
  tcp_listen: "0.0.0.0:9100"

meshping:
  path: /usr/local/bin/meshping

persist:
  path: /var/lib/meshd/state.json
  debounce: 60s

api:
  listen: "0.0.0.0:9101"
```

**`node_id` must exactly match one of the host names in the inventory.** This is the single most common misconfiguration. If it does not match, the node computes the full topology correctly but finds no tasks where it is the source, and `/metrics` stays empty of probe data.

Deploy the same `meshd.yaml` to every node with only `node_id` changed. Everything else is identical, which is what allows configuration management to template a single file.

## 3.4 What you get

Two zones: `sjc/01` and `sjc/02`. One pairing. Four slots: two anchor, two diverse.

```
Pairing "sjc/01|sjc/02"
  Slot 0 [anchor]  A: db-001...sjc01    B: db-001...sjc02
  Slot 1 [anchor]  A: db-001...sjc01    B: db-001...sjc02
  Slot 2 [diverse] A: web-001...sjc01   B: web-001...sjc02
  Slot 3 [diverse] A: web-002...sjc01   B: web-002...sjc02
```

Both anchor slots hold the same hosts by design. Their endpoints never move, so a change in their measurement is a change in the path. The diverse slots hold different hosts, so a single bad machine appears in one slot and not the others.

Eight directed tasks (4 slots × 2 directions), each running three probe types, spread across four source machines. Each machine runs only its own tasks.

## 3.5 Verify

```bash
curl -s localhost:9101/zones | jq '.zones'
curl -s localhost:9101/pairings | jq
curl -s localhost:9101/tasks | jq '.tasks[] | {dst, kind, mean: .stats.mean, loss: .stats.loss_ratio}'
```

---

# 4. Scenario B — Metro-level testing across a large fleet

**Situation:** 2,000 machines across 12 data centres in 6 metros. Data-centre-level pairing gives 66 pairings; that is fine. But you also want a metro view for capacity planning, and you want to reduce measurement volume.

## 4.1 Change one line

```yaml
zone:
  keys: [metro]
```

Six zones instead of twelve. Fifteen pairings instead of sixty-six. Same inventory file, same everything else.

## 4.2 Understanding the trade-off

At metro level, a slot in `sjc|iad` might hold `web-001...sjc01` on one side and `web-042...iad03` on the other. The measurement covers the metro-to-metro path but does not tell you which data centre within each metro was involved — except that the host labels do tell you, since they contain the site token.

Raise the slot count so that different data centres are represented:

```yaml
slots:
  count: 8
  anchor_ratio: 0.25
```

Eight slots per pairing: two anchor, six diverse. The diverse slots draw from across the metro, so multiple data centres end up represented. The scan's start offset spreads them further rather than clustering on the alphabetically first hosts.

## 4.3 Running both levels simultaneously

The zone rule is a single value, so one process gives one level. To measure both, run two instances on each node with different ports and different state files.

**File: `/etc/meshd/meshd-dc.yaml`**

```yaml
node_id: web-001.product.prod.sjc01.example.com
zone:
  keys: [metro, dc_instance]
slots:
  count: 4
probes:
  udp: { enabled: true, port: 8472, count: 10, interval: 1s, timeout: 1s, payload_bytes: 64 }
  tcp: { enabled: true, port: 9100, count: 5, interval: 5s, timeout: 2s, mode: connect }
  icmp: { enabled: true, count: 10, interval: 1s, timeout: 1s, payload_bytes: 56 }
responder:
  enabled: true
  udp_listen: "0.0.0.0:8472"
  tcp_listen: "0.0.0.0:9100"
persist:
  path: /var/lib/meshd/state-dc.json
api:
  listen: "0.0.0.0:9101"
providers:
  file: { enabled: true, path: /etc/meshd/inventory.json }
```

**File: `/etc/meshd/meshd-metro.yaml`**

```yaml
node_id: web-001.product.prod.sjc01.example.com
zone:
  keys: [metro]
slots:
  count: 8
  anchor_ratio: 0.25
probes:
  cycle: 60s
  udp: { enabled: true, port: 8473, count: 10, interval: 1s, timeout: 1s, payload_bytes: 64 }
  tcp: { enabled: false }
  icmp: { enabled: true, count: 10, interval: 1s, timeout: 1s, payload_bytes: 56 }
responder:
  enabled: true
  udp_listen: "0.0.0.0:8473"
  tcp_listen: "0.0.0.0:9101"
persist:
  path: /var/lib/meshd/state-metro.json
api:
  listen: "0.0.0.0:9102"
providers:
  file: { enabled: true, path: /etc/meshd/inventory.json }
```

Note what must differ between the two: UDP port, TCP port, API port, state file path. The inventory file is shared.

The metro instance uses a slower `cycle` (60s instead of 15s), because long-haul trends do not need 15-second granularity and the extra traffic is not free.

## 4.4 Excluding pairings

Not every pairing is interesting. If you never route traffic between two specific metros, skip them:

```yaml
pairings:
  exclude:
    - "fra|nrt"
    - "*|test"
```

Patterns use `path.Match` glob semantics against the pairing key. The pairing key is the two zone keys sorted alphabetically and joined with `|`, so write it in sorted order.

To measure only a specific set:

```yaml
pairings:
  include:
    - "sjc|*"
    - "iad|*"
```

The include list runs first when non-empty; the exclude list then filters what survived.

---

# 5. Scenario C — Kubernetes cluster, zone to zone

**Situation:** A cluster spanning three availability zones. You want inter-zone network quality, and you want the mesh to follow node scaling automatically.

## 5.1 Deployment shape

`meshd` runs as a DaemonSet. Every node gets one pod. Each pod discovers all nodes through the API server and measures the slots where its own node is an endpoint.

## 5.2 RBAC

**File: `k8s/rbac.yaml`**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: meshd
  namespace: monitoring
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: meshd
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: meshd
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: meshd
subjects:
  - kind: ServiceAccount
    name: meshd
    namespace: monitoring
```

Read-only access to nodes. Nothing more is needed.

## 5.3 Configuration

**File: `k8s/configmap.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: meshd-config
  namespace: monitoring
data:
  meshd.yaml: |
    # node_id is injected at runtime from the downward API,
    # see the DaemonSet below.

    providers:
      file:
        enabled: false
      http:
        enabled: false
      k8s:
        enabled: true
        cluster_name: prod-usw2
        resync: 300s
        debounce: 2s
        label_selector: ""
        address_order: [InternalIP]
        taint_deny:
          - node.kubernetes.io/unreachable
          - node.kubernetes.io/not-ready
          - node.kubernetes.io/unschedulable

    zone:
      keys: [k8s.region, k8s.zone]
      separator: "/"
      missing: exclude

    pairings:
      intra_zone: false
      max_pairings: 200

    slots:
      count: 4
      anchor_ratio: 0.5

    health:
      release_hold: 60s
      missing_grace: 60s
      initial_grace: 120s

    probes:
      cycle: 15s
      window: 60s
      icmp:
        enabled: false
      udp:
        enabled: true
        interval: 500ms
        count: 20
        payload_bytes: 64
        port: 8472
        timeout: 1s
      tcp:
        enabled: true
        interval: 2s
        count: 5
        port: 9100
        timeout: 2s
        mode: connect

    responder:
      enabled: true
      udp_listen: "0.0.0.0:8472"
      tcp_listen: "0.0.0.0:9100"

    persist:
      path: /var/lib/meshd/state.json
      debounce: 60s

    api:
      listen: "0.0.0.0:9101"

    log:
      level: info
      format: json
```

ICMP is disabled here. Granting `CAP_NET_RAW` in a container requires a `securityContext` change that many clusters forbid. UDP with a higher packet count (20 at 500ms) gives comparable RTT and loss data without the capability. If your cluster permits it, see §5.6.

## 5.4 DaemonSet

**File: `k8s/daemonset.yaml`**

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: meshd
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: meshd
  template:
    metadata:
      labels:
        app: meshd
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9101"
    spec:
      serviceAccountName: meshd
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      tolerations:
        - operator: Exists
      containers:
        - name: meshd
          image: your-registry/meshd:1.0
          args:
            - -config
            - /etc/meshd/meshd.yaml
          env:
            - name: MESH_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
          command:
            - /bin/sh
            - -c
            - |
              sed "s|^node_id:.*||" /etc/meshd/meshd.yaml > /tmp/meshd.yaml
              echo "node_id: k8s://prod-usw2/${MESH_NODE_NAME}" >> /tmp/meshd.yaml
              exec /usr/local/bin/meshd -config /tmp/meshd.yaml
          ports:
            - containerPort: 9101
              name: metrics
              hostPort: 9101
            - containerPort: 8472
              name: udp-probe
              protocol: UDP
              hostPort: 8472
            - containerPort: 9100
              name: tcp-probe
              hostPort: 9100
          volumeMounts:
            - name: config
              mountPath: /etc/meshd
            - name: state
              mountPath: /var/lib/meshd
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              memory: 256Mi
          livenessProbe:
            httpGet:
              path: /livez
              port: 9101
            initialDelaySeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: 9101
            initialDelaySeconds: 15
      volumes:
        - name: config
          configMap:
            name: meshd-config
        - name: state
          hostPath:
            path: /var/lib/meshd
            type: DirectoryOrCreate
```

### Why these choices

**`hostNetwork: true`** — Probes must measure the node network, not the pod overlay. Without it you would be measuring your CNI, which is a different and usually less interesting question.

**`node_id: k8s://<cluster>/<node>`** — The Kubernetes provider builds host IDs in exactly this form. The `node_id` must match it character for character, which is why the startup command constructs it from the downward API rather than hard-coding it.

**`hostPath` state volume** — The state file must survive pod restarts, otherwise every rollout reassigns every slot and breaks every time series. This is the single most important line in the manifest for metric continuity.

**`tolerations: [operator: Exists]`** — Measure every node, including tainted ones. Whether a tainted node is *eligible* for a slot is decided by `taint_deny` in the configuration, not by scheduling.

## 5.5 Using custom node labels

Standard topology labels are not the only option. If your nodes carry your own labels, use them:

```yaml
zone:
  keys: [k8s.label.rack]
```

Or combine:

```yaml
zone:
  keys: [k8s.label.datacenter, k8s.label.rack]
```

Every node label is available as `k8s.label.<name>`, with `/` and `.` replaced by `_`. So `topology.example.com/rack` becomes `k8s.label.topology_example_com_rack`.

To find out what is actually available on your nodes:

```bash
kubectl port-forward -n monitoring ds/meshd 9101:9101
curl -s localhost:9101/inventory | jq '.hosts[0].attributes'
```

That prints the complete attribute bag for one node. Write your zone rule against what you see there.

### Collapsing zones into regions with a transform

```yaml
zone:
  keys:
    - key: k8s.zone
      transform: "regex:^(.*)-[a-z]$:$1"
```

Turns `us-west-2a`, `us-west-2b`, `us-west-2c` all into `us-west-2`. Useful when you want region-level measurement but only have zone labels.

Available transforms:

| Transform | Effect |
|---|---|
| `lower`, `upper` | Case normalisation |
| `trim` | Strip whitespace |
| `prefix:3` | Keep the first 3 characters |
| `regex:<pattern>:<replacement>` | Full rewrite |

## 5.6 Enabling ICMP in Kubernetes

If your cluster permits added capabilities:

```yaml
          securityContext:
            capabilities:
              add: ["NET_RAW"]
```

And in the configuration:

```yaml
      icmp:
        enabled: true
        interval: 1s
        count: 10
        payload_bytes: 56
        timeout: 1s
```

Your container image must also carry the capability on the binary:

```dockerfile
RUN setcap cap_net_raw+ep /usr/local/bin/meshping
```

## 5.7 Node scaling behaviour

This is worth understanding before you deploy.

| Event | What happens |
|---|---|
| Node pool scales **up** | Zero slot changes. New nodes join the inventory and become candidates, but existing valid assignments are kept |
| Node pool scales **down** | Only the slot sides that held a removed node are cleared and refilled |
| Node becomes `NotReady` | Provider marks it ineligible immediately; its slot sides clear on the next reconcile |
| Node is cordoned | Same as `NotReady` — `spec.unschedulable` is a deny signal |
| Rolling node update | Debounced into one inventory update; only affected slot sides change |

Scaling up producing zero changes is deliberate. `rebalance_on_add` defaults to false, which favours metric continuity over optimal distribution. A new node will pick up slots as existing holders go away.

---

# 6. Scenario D — Hybrid: bare metal and Kubernetes in one mesh

**Situation:** You are migrating from bare metal to Kubernetes and want to measure the network between the two estates during the transition.

## 6.1 The problem

The two providers produce different attribute names. Bare metal hosts have `metro` and `dc_instance`. Kubernetes nodes have `k8s.region` and `k8s.zone`. A single zone rule cannot read both.

## 6.2 The solution

Both providers write into one inventory. Give the Kubernetes nodes the *same* attribute names the bare metal hosts use, by labelling them:

```bash
kubectl label nodes -l topology.kubernetes.io/zone=us-west-2a \
  metro=sjc dc_instance=01
kubectl label nodes -l topology.kubernetes.io/zone=us-west-2b \
  metro=sjc dc_instance=02
```

Then reference the label form in the zone rule. But `k8s.label.metro` and `metro` are still different keys, so use a rule that works for both by relying on the fact that the file provider can also be given the `k8s.` prefixed names — or simpler, add matching attributes to the file inventory.

**File: `/etc/meshd/inventory.json`** — add explicit attributes:

```json
{
  "version": 1,
  "site_table": {
    "sjc01": { "country": "us", "metro": "sjc", "dc_instance": "01" }
  },
  "hosts": [
    {
      "name": "web-001.product.prod.sjc01.example.com",
      "attributes": {
        "k8s.label.metro": "sjc",
        "k8s.label.dc_instance": "01"
      }
    }
  ]
}
```

Now both sources produce `k8s.label.metro` and `k8s.label.dc_instance`, and one rule works:

```yaml
zone:
  keys: [k8s.label.metro, k8s.label.dc_instance]
```

## 6.3 Configuration

```yaml
node_id: web-001.product.prod.sjc01.example.com

providers:
  file:
    enabled: true
    path: /etc/meshd/inventory.json
    interval: 60s
    priority: 10
  k8s:
    enabled: true
    cluster_name: prod-usw2
    kubeconfig: /etc/meshd/kubeconfig
    resync: 300s
    priority: 30

zone:
  keys: [k8s.label.metro, k8s.label.dc_instance]

slots:
  count: 6
  anchor_ratio: 0.34
```

`priority` resolves collisions if a host ID somehow appears from both sources. Higher wins. In practice they cannot collide, because Kubernetes host IDs start with `k8s://` and file host IDs are DNS names.

Raise the slot count in a hybrid, because you want both estates represented within a zone. With 6 slots and hosts drawn from a mixed zone, the scan naturally spreads across both.

## 6.4 Verifying the mix

```bash
curl -s localhost:9101/zones | jq '.zones[] | {zone, members}'
```

Each zone's member list should show both DNS names and `k8s://` IDs.

```bash
curl -s localhost:9101/state | jq '.state.pairings[].slots[] | {host_a, host_b}'
```

Check that some slots pair a bare metal host with a Kubernetes node.

---

# 7. Scenario E — Centralised inventory over HTTP

**Situation:** You have 2,000 nodes. Pushing an inventory file to all of them on every change is impractical. You want a single source of truth.

## 7.1 Serve the document

Publish the same JSON document from any HTTP server. Requirements:

- Serve `ETag` or `Last-Modified` headers, so conditional requests work.
- Serve it consistently — a partial or malformed document is rejected by every node, which is correct but means nobody gets an update.

## 7.2 Configuration

```yaml
providers:
  file:
    enabled: false
  http:
    enabled: true
    url: https://config.example.com/mesh/inventory.json
    interval: 60s
    timeout: 10s
    cache_path: /var/lib/meshd/inventory.json
    cache_max_age: 24h
    auth_header_file: /etc/meshd/token
    backoff_min: 5s
    backoff_max: 300s
    priority: 20
```

**File: `/etc/meshd/token`**

```
Bearer eyJhbGciOiJIUzI1NiIs...
```

Mode `0600`, owned by the `meshd` user. The file is re-read on every fetch, so rotating the token requires no restart.

## 7.3 The cache is the point

Understand this behaviour before deploying:

| Event | What happens |
|---|---|
| **Startup** | The cache is read and applied **before** the first HTTP fetch. The node is operational before the network is |
| **200 response** | Parse, validate, replace the set, rewrite the cache atomically |
| **304 response** | Keep the current set. Do not touch the cache |
| **Network failure** | Keep the current set. Back off exponentially. **Measurement continues** |
| **Malformed document** | Keep the current set. Never apply it. Increment a counter |
| **Cache older than `cache_max_age`** | Mark the set stale, report it through a gauge. **Do not clear slots** |

The config server going down does not stop measurement. It does not clear slots. It does not break time series. Nodes run on cache until the server returns.

Monitor it:

```promql
mesh_provider_cache_age_seconds{source="http"} > 3600
```

## 7.4 Combining HTTP and file

Use the file provider for a small set of hosts you want measured regardless of what the central inventory says:

```yaml
providers:
  file:
    enabled: true
    path: /etc/meshd/inventory-local.json
    priority: 50
  http:
    enabled: true
    url: https://config.example.com/mesh/inventory.json
    priority: 20
```

The file provider has higher priority, so any host appearing in both is taken from the local file. Useful for pinning an override during an incident.

## 7.5 Forcing a refresh

```bash
curl -X POST localhost:9101/refresh?source=http
```

Or across the fleet:

```bash
for h in $(cat nodes.txt); do
  curl -sX POST "http://$h:9101/refresh" &
done
wait
```

---

# 8. Scenario F — Full mesh for a small fleet

**Situation:** Twelve machines. You want every pair measured individually.

## 8.1 Configuration

```yaml
zone:
  keys: [fqdn]

pairings:
  intra_zone: false
  max_pairings: 500

slots:
  count: 1
  anchor_ratio: 1.0
```

`keys: [fqdn]` puts exactly one host in each zone, so a zone pairing *is* a host pairing. Full mesh is not a special code path; it is this rule.

`count: 1` because there is only one possible host pair per pairing. A second slot would hold the same two hosts, producing a duplicate measurement.

`anchor_ratio: 1.0` makes that single slot an anchor. This is semantically correct: the endpoints are fixed by definition, since each zone has exactly one member.

For Kubernetes:

```yaml
zone:
  keys: [k8s.node]
```

## 8.2 Scale limit

Full mesh grows as `n(n-1)/2`:

| Machines | Pairings | Tasks (3 protocols, both directions) |
|---|---|---|
| 10 | 45 | 270 |
| 20 | 190 | 1,140 |
| 50 | 1,225 | 7,350 |
| 100 | 4,950 | 29,700 |

`max_pairings` defaults to 5,000 and will abort the reconcile above that. **When it aborts, it keeps the previous assignment** — a mistaken zone rule cannot replace a working topology with an unworkable one.

Above roughly 30 machines, move to a zone-based rule. The whole design exists to avoid this growth curve.

---

# 9. Scenario G — Super hosts: one machine against a whole zone

**Situation:** You suspect one machine has a network problem, or you want to validate a new machine before putting it in production. You want to measure it against every machine in a remote zone at once.

## 9.1 What a super slot is

A super slot pairs one designated host with **every** eligible host in the far zone. One super host and a far zone of 20 machines produces 20 super slots.

This answers a question the anchor and diverse slots cannot: is this specific machine bad, and bad against everything, or bad against only some destinations?

## 9.2 Selection by canonical order

```yaml
slots:
  count: 4
  anchor_ratio: 0.5
  super_hosts: 1
  super_max_targets: 50
```

Picks the first eligible host in each zone by canonical order. Super hosts are **sticky**: once chosen, a super host stays chosen until it becomes ineligible, so the wide fan-out measurement does not move on every inventory change.

## 9.3 Selection by attribute

More useful in practice: designate the machine explicitly.

```yaml
slots:
  super_hosts: 1
  super_selector:
    role: canary
  super_max_targets: 50
```

Then label the machine:

```json
{
  "name": "web-042.product.prod.sjc01.example.com",
  "attributes": { "role": "canary" }
}
```

Or in Kubernetes:

```bash
kubectl label node worker-17 mesh-role=canary
```

```yaml
slots:
  super_selector:
    k8s.label.mesh-role: canary
```

Every key in `super_selector` must match. If the selector matches fewer hosts than `super_hosts`, the remainder is filled from canonical order.

## 9.4 Cardinality warning

Super slots multiply quickly. With 10 zones of 20 machines each, `super_hosts: 1` produces:

```
45 pairings × 2 directions × 20 targets = 1,800 super slots
```

Times three probe types times two directions gives around 10,800 additional tasks across the fleet.

Control it:

```yaml
slots:
  super_hosts: 1
  super_max_targets: 10     # cap targets per zone
pairings:
  include:
    - "sjc/01|*"            # only from the zone under investigation
```

Turn super testing off when the investigation is complete. Set `super_hosts: 0` and reload. All super slots are removed and their series forgotten.

## 9.5 Reading the results

```promql
# Is this one machine bad against everything?
avg by (host_src) (
  mesh_loss_ratio{class="super", zone_src="sjc/01"}
)

# Or bad against specific destinations only?
mesh_loss_ratio{class="super", host_src="web-042.product.prod.sjc01.example.com"}
```

If the first query shows uniform loss, the machine is bad. If the second shows loss against a subset, the problem is in the paths to those specific destinations.

---

# 10. Scenario H — Intra-zone testing

**Situation:** You want to measure within a data centre, not just between them. Top-of-rack problems, oversubscribed uplinks, and NIC issues show up here.

## 10.1 Configuration

```yaml
zone:
  keys: [metro, dc_instance]

pairings:
  intra_zone: true
```

This adds one pairing per zone where both sides are the same zone. Slot fill then requires two **distinct** hosts from that zone — the scanner explicitly excludes the far side's host when filling a side of an intra-zone pairing.

A zone with fewer than two members produces no intra-zone pairing.

## 10.2 Distinguishing the two in queries

```promql
# Within a data centre
mesh_rtt_mean_seconds{zone_src="sjc/01", zone_dst="sjc/01"}

# Between data centres
mesh_rtt_mean_seconds{zone_src="sjc/01", zone_dst="sjc/02"}
```

Intra-zone RTT should be an order of magnitude lower. When it approaches inter-zone RTT, something inside the data centre is wrong.

## 10.3 Separate tuning for intra-zone

Intra-zone paths are short and fast. If you want higher resolution there without paying for it on long-haul paths, run a second instance:

```yaml
# intra-zone instance
zone:
  keys: [metro, dc_instance]
pairings:
  intra_zone: true
  include:
    - "sjc/01|sjc/01"
    - "sjc/02|sjc/02"
probes:
  cycle: 5s
  window: 30s
  udp:
    enabled: true
    interval: 100ms
    count: 50
    port: 8474
```

Fifty packets at 100ms gives a five-second burst with fine-grained jitter data — appropriate for a sub-millisecond path, wasteful for a transatlantic one.

---

# 11. Protocol configuration in depth

## 11.1 Choosing protocols

| Protocol | Measures | Needs responder | Typical use |
|---|---|---|---|
| ICMP | Pure network path RTT and loss | No | Baseline path quality |
| UDP echo | RTT, loss, jitter, reorder | **Yes** | Highest-fidelity data |
| TCP connect | Handshake time, reachability | No | Service-level reachability |
| TCP echo | Handshake and payload RTT separately | **Yes** | Full stack behaviour |

Run all three where possible. Their disagreements are informative: ICMP clean but TCP failing means a firewall or a service problem, not a network path problem.

## 11.2 ICMP

```yaml
probes:
  icmp:
    enabled: true
    interval: 1s        # delay between packets
    count: 10           # packets per cycle
    payload_bytes: 56   # ICMP payload, not frame size
    timeout: 1s         # per-packet reply timeout
    ttl: 64
    df: false           # accepted but not applied
```

**`payload_bytes: 56`** matches the standard `ping` default. Total IPv4 packet size is `payload + 8 + 20 = 84` bytes.

**`count: 10` at `interval: 1s`** gives one loss-ratio resolution step of 10%. To detect 1% loss, use `count: 100`, which means a 100-second cycle — appropriate for a slow trend, not for alerting.

**`df`** is accepted in configuration and ignored. Setting the do-not-fragment bit is not portable through the Go ICMP library. Path MTU discovery would need platform-specific socket options.

### Larger packets

```yaml
    payload_bytes: 1400
```

Reveals MTU problems and fragmentation-related loss that a 56-byte packet does not. Run alongside the small-packet test, not instead of it, and compare loss ratios.

## 11.3 UDP

```yaml
probes:
  udp:
    enabled: true
    interval: 200ms
    count: 25
    payload_bytes: 64
    port: 8472
    timeout: 1s
```

Requires the responder on the far side.

**UDP gives the best data.** It measures jitter and reorder that ICMP and TCP connect cannot, and it is not subject to the ICMP rate limiting that many network devices apply.

**`interval: 200ms`, `count: 25`** gives a five-second burst with 25 samples, so jitter is computed over 24 consecutive differences. That is a meaningful jitter figure. Ten samples at one second apart is not — the samples are too far apart to reflect real jitter.

**Reorder detection:** a reply arriving after a higher sequence number already arrived counts as a reorder, not a loss. Persistent reorder indicates ECMP hashing across paths of unequal latency.

### Payload sizing for VoIP-like traffic

```yaml
    payload_bytes: 172   # G.711 at 20ms
    interval: 20ms
    count: 250           # 5 seconds of simulated call
```

Produces jitter and loss figures directly comparable to voice quality expectations.

## 11.4 TCP

### Connect mode

```yaml
probes:
  tcp:
    enabled: true
    interval: 5s
    count: 5
    port: 9100
    timeout: 2s
    mode: connect
```

Dials, measures the three-way handshake, closes immediately. **No payload is sent, so `payload_bytes` is ignored.**

Needs only a listener on the far side, not a responder. This makes it usable against any TCP service:

```yaml
    port: 443           # measure reachability of your TLS endpoint
```

Note that connect mode against a real service creates real connections in its accept queue. Keep `count` low and `interval` high when pointing at production services.

### Echo mode

```yaml
probes:
  tcp:
    enabled: true
    mode: echo
    payload_bytes: 1024
    port: 9100
    count: 5
    interval: 2s
    timeout: 3s
```

Dials, sends the payload, reads it back, and reports handshake time and payload round trip **separately**. This separation is the value: a slow handshake with a fast payload round trip means a control-plane problem, while both slow means a data-path problem.

Requires the responder.

Each iteration opens its own connection. A reused connection would measure only the payload path and would hide a handshake failure.

## 11.5 Cycle and window

```yaml
probes:
  cycle: 15s     # delay between cycles for one task
  window: 60s    # rolling aggregation duration
```

**`cycle`** controls how often each task measures. **`window`** controls how much history the statistics cover.

`window` must be at least `cycle` (validation enforces this). A window covering four cycles is a reasonable default: it smooths single-cycle noise without lagging real changes.

Task start times are offset by a hash of the task key, so all tasks do not fire simultaneously and produce a synchronised network burst.

### Tuning for different purposes

| Purpose | cycle | window | Rationale |
|---|---|---|---|
| Alerting on outages | 10s | 30s | Fast detection |
| Normal monitoring | 15s | 60s | The default |
| Capacity trends | 60s | 300s | Low overhead, smooth data |
| Incident investigation | 5s | 15s | High resolution, high cost |

## 11.6 Traffic volume calculation

Per task, per cycle:

```
bytes ≈ count × (payload_bytes + headers) × 2 directions
```

For UDP with `count: 25`, `payload_bytes: 64`:

```
25 × (64 + 42) × 2 ≈ 5.3 KB per cycle
```

At `cycle: 15s`, that is roughly 350 bytes per second per task. A node running 12 tasks generates about 4 KB/s. Negligible on any modern link, but worth calculating before setting `count: 250` on a large mesh.

---

# 12. Slot tuning

## 12.1 How many slots

```yaml
slots:
  count: 4
  anchor_ratio: 0.5
```

| Zone size | Suggested count | Reasoning |
|---|---|---|
| 2–3 hosts | 2 | More slots would force reuse |
| 4–10 hosts | 4 | Two anchor, two diverse, all distinct |
| 10–50 hosts | 4–6 | Diminishing returns above 6 |
| 50+ hosts | 6–8 | Better coverage of a large zone |

The value of N is fault isolation, not statistical power. Three independent slots are enough to distinguish "one endpoint is bad" from "the path is bad". Going to twelve does not improve that conclusion; it just costs more.

## 12.2 Anchor ratio

```yaml
  anchor_ratio: 0.5
  anchor_rounding: up
```

Anchor slots hold fixed endpoints. Their measurement changing means the *path* changed, because nothing else could have.

Diverse slots hold varying endpoints. Their spread tells you whether a problem is endpoint-specific.

| Ratio | Anchor | Diverse | Use when |
|---|---|---|---|
| 0.25 | 1 of 4 | 3 of 4 | Large zones; you want broad coverage |
| 0.5 | 2 of 4 | 2 of 4 | Default; balanced |
| 0.75 | 3 of 4 | 1 of 4 | You care most about path trend stability |
| 1.0 | All | None | Full mesh, where diversity is meaningless |

A non-zero ratio always produces at least one anchor slot, even at small counts, so the constant-endpoint measurement never disappears silently.

## 12.3 Reuse

```yaml
  allow_reuse: true
```

When a zone has fewer eligible hosts than slot sides, a host must hold more than one. Reuse is ranked:

| Rank | Meaning | Metric label |
|---|---|---|
| 1 | Not used elsewhere on this side of this pairing | `reuse_rank="1"` |
| 2 | Reused at the lowest current use count | `reuse_rank="2"` |
| 3 | Reused, no better candidate existed | `reuse_rank="3"` |

With `allow_reuse: false`, only rank 1 is accepted. Unfillable slots stay empty and `mesh_slots_unfilled` rises.

Prefer `true`. An empty slot measures nothing; a reused slot measures something, and the rank label tells you it is not fully independent.

Monitor:

```promql
# Slots that are not independent
count(mesh_rtt_mean_seconds{reuse_rank!="1"})
```

## 12.4 Rebalancing

```yaml
  rebalance_on_add: false
```

This setting is accepted and validated but **not currently acted upon**. It defaults to false, which is the behaviour you want: adding hosts does not disturb existing valid assignments.

Consequence: after a period of host churn, some slots may hold rank-2 or rank-3 assignments even though better candidates now exist. This is a deliberate trade of optimal distribution for metric continuity. Those slots correct themselves when their current holders go away.

---

# 13. Health and stability tuning

## 13.1 The state machine

```
unknown ──probe ok──> healthy
   │                     │
   │                  probe fails
   │                     v
   │                  suspect ──(unhealthy_after)──> pending
   │                     │                              │
   │                  probe ok                    (release_hold)
   │                     │                              v
   └─────────────────────┴──────────────────────>  unhealthy
                                                        │
                                                  slot sides clear
```

**Four states are eligible to hold a slot: `unknown`, `healthy`, `suspect`, `pending`.**

A host is not released the moment it fails. It must fail `unhealthy_after` cycles to reach `pending`, then wait `release_hold` to reach `unhealthy`. Only then does the reconcile clear its slot sides.

## 13.2 The critical setting

```yaml
health:
  release_hold: 60s
```

Without this delay, a ten-second network event rewrites slot assignments and breaks the time series in the exact window where the measurement matters most.

| Value | Effect |
|---|---|
| 0s | Every transient failure reassigns slots. Do not do this |
| 30s | Aggressive; tolerates brief events only |
| **60s** | Default. Tolerates most transient events |
| 300s | Very conservative; a genuinely dead host holds its slot for five minutes |

Set it longer than your typical transient event and shorter than your acceptable time-to-repair.

## 13.3 Full timer set

```yaml
health:
  unhealthy_after: 3          # failed cycles before marking
  release_hold: 60s           # mark to slot clear
  healthy_after: 2            # successful cycles before eligible again
  initial_grace: 90s          # new host protected from marking
  missing_grace: 60s          # grace for absence from a provider
  dns_grace: 120s             # grace for unresolvable address
  flap_threshold: 3           # transitions in the window
  flap_window: 10m
  flap_cooldown: 15m
  pairing_removal_hold: 300s  # vanished pairing survives this long
```

**`unhealthy_after: 3`** at `cycle: 15s` means 45 seconds of failure before the mark, then 60 seconds of hold. Total 105 seconds before a slot side clears.

**`initial_grace: 90s`** protects newly discovered hosts. Without it, a host discovered but not yet probed could be marked unhealthy before it ever had a chance.

**Flap detection** counts eligibility transitions. A host crossing the threshold is forced into `cooldown` and held out for `flap_cooldown`, so an unstable machine cannot cause repeated reassignment churn.

**`pairing_removal_hold: 300s`** means a zone that briefly disappears — a whole DC failing a health check, for example — keeps its pairing and its slot table for five minutes. If it returns within that window, nothing was lost.

## 13.4 Monitoring stability

```promql
# Assignment churn by cause
rate(mesh_slot_changes_total[1h])

# Repairs specifically
rate(mesh_slot_changes_total{reason="host_unhealthy"}[1h])
```

Sustained churn means either genuine instability, or `release_hold` set too low for your environment.

Reasons you may see:

| Reason | Meaning |
|---|---|
| `new_slot` | A slot side was filled for the first time |
| `host_gone` | The host left the inventory |
| `host_unhealthy` | The host failed probes past the hold |
| `zone_changed` | The host's attributes changed its zone |
| `class_changed` | Slot layout configuration changed |
| `anchor_reset` | The anchor host was replaced |
| `super_changed` | Super host or target set changed |

---

# 14. Operating the system

## 14.1 Daily inspection

```bash
# Is it healthy?
curl -s localhost:9101/readyz | jq

# What is it measuring?
curl -s localhost:9101/tasks | jq '.tasks[] | {
  dst: .dst, kind: .kind, class: .class,
  mean_ms: (.stats.mean / 1000000),
  loss: .stats.loss_ratio
}'

# Topology
curl -s localhost:9101/zones | jq '{
  zones: [.zones[] | {zone, count}],
  unresolved: (.unresolved | length)
}'
```

## 14.2 Configuration changes

Validate first, always:

```bash
meshd -check -config /etc/meshd/meshd.yaml
```

Prints `configuration is valid`, or lists **every** problem at once — not just the first.

Apply:

```bash
sudo systemctl reload meshd    # sends SIGHUP
```

A configuration that fails validation is **rejected** and the previous one stays active. You cannot break a running node with a bad configuration file.

### Changes that are disruptive

| Change | Effect |
|---|---|
| `zone.keys` | **Rebuilds all slots. All time series break** |
| `pairings.include`/`exclude` | Adds or removes pairings |
| `slots.count` or `anchor_ratio` | **Rebuilds all slots** |
| `probes.*` | Restarts tasks with new parameters; windows reset |
| `health.*` | Applies to timers immediately, no slot changes |
| `providers.*` | Restarts only the changed provider |
| `persist.*`, `api.*`, `log.*` | No measurement impact |

The system logs a warning when a reload changes the topology definition, because it knows this will break every series.

## 14.3 Forcing operations

```bash
# Force a reconcile, see what changed
curl -sX POST localhost:9101/reconcile | jq '.delta'

# Force a provider poll
curl -sX POST 'localhost:9101/refresh?source=file'
```

## 14.4 Restarts and continuity

The state file is what makes restarts free:

```bash
sudo systemctl restart meshd
curl -s localhost:9101/state | jq '.last_delta'
```

Expected: an empty delta. The same hosts hold the same slots. The same tasks start. No time series break.

**Never delete the state file casually.** Doing so forces a full reassignment and breaks every series on that node.

```bash
cat /var/lib/meshd/state.json | jq '.pairings | keys'
```

## 14.5 Rolling upgrades

1. Upgrade one node. Confirm `mesh_state_reset_total` did not increment.
2. Confirm `/state` shows an empty delta after restart.
3. Confirm `mesh_tasks_running` returned to its previous value.
4. Proceed with the rest.

If `mesh_state_reset_total` incremented, the state file was rejected — likely a schema version change. Expect a series break on that node and check the release notes.

## 14.6 Scaling the fleet

Adding machines requires no coordination:

1. Add them to the inventory document.
2. Deploy `meshd` to them with the correct `node_id`.
3. Wait for the poll interval.

Existing nodes see the new hosts as candidates. With `rebalance_on_add` false, **existing assignments do not change**. The new hosts become slot holders as existing ones go away.

Removing machines:

1. Set `"enabled": false` in the inventory, or delete the entry.
2. Slot sides holding them clear after the health hold.
3. Stop `meshd` on those machines.

---

# 15. Prometheus queries

## 15.1 Scrape configuration

```yaml
scrape_configs:
  - job_name: mesh
    scrape_interval: 30s
    file_sd_configs:
      - files: ['/etc/prometheus/mesh-targets.json']
```

For Kubernetes:

```yaml
  - job_name: mesh
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        action: keep
        regex: meshd
      - source_labels: [__address__]
        action: replace
        regex: '([^:]+)(:\d+)?'
        replacement: '${1}:9101'
        target_label: __address__
```

Scrape interval should be at or below `probes.window`, so no window's worth of data is missed.

## 15.2 Essential queries

**Zone-pair RTT** — aggregate across slots:

```promql
avg by (zone_src, zone_dst) (
  mesh_rtt_mean_seconds{probe="icmp"}
)
```

**Zone-pair loss:**

```promql
avg by (zone_src, zone_dst) (mesh_loss_ratio{probe="udp"})
```

**Is it the path or one host?** Compare anchor spread against diverse spread:

```promql
# Anchor slots have fixed endpoints; a change here is a path change
avg by (zone_src, zone_dst) (
  mesh_rtt_mean_seconds{class="anchor", probe="icmp"}
)

# Diverse slots vary; disagreement between them points at an endpoint
max by (zone_src, zone_dst) (mesh_loss_ratio{class="diverse"})
  -
min by (zone_src, zone_dst) (mesh_loss_ratio{class="diverse"})
```

A large diverse spread with a clean anchor means one endpoint is bad, not the path.

**Find the bad host:**

```promql
topk(10,
  avg by (host_dst) (mesh_loss_ratio{probe="udp"})
)
```

If one host appears at the top across many `zone_src` values, it is the host, not the paths to it.

**Jitter:**

```promql
avg by (zone_src, zone_dst) (mesh_jitter_seconds{probe="udp"})
```

**Tail latency:**

```promql
mesh_rtt_quantile_seconds{quantile="0.99", probe="icmp"}
```

**TCP handshake versus payload** — only meaningful in echo mode:

```promql
histogram_quantile(0.95, rate(mesh_tcp_connect_seconds_bucket[5m]))
histogram_quantile(0.95, rate(mesh_rtt_seconds_bucket{probe="tcp"}[5m]))
```

A slow handshake with a fast payload round trip means a control-plane problem.

**Protocol disagreement** — ICMP clean but TCP failing:

```promql
(avg by (zone_src, zone_dst) (mesh_loss_ratio{probe="tcp"}) > 0.1)
and
(avg by (zone_src, zone_dst) (mesh_loss_ratio{probe="icmp"}) < 0.01)
```

This pattern means a firewall or service problem, not a network path problem.

## 15.3 Alerting rules

**File: `mesh-alerts.yaml`**

```yaml
groups:
  - name: mesh
    rules:
      - alert: MeshHighLoss
        expr: |
          avg by (zone_src, zone_dst) (
            mesh_loss_ratio{probe="udp"}
          ) > 0.02
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Packet loss between {{ $labels.zone_src }} and {{ $labels.zone_dst }}"

      - alert: MeshHighLatency
        expr: |
          avg by (zone_src, zone_dst) (
            mesh_rtt_mean_seconds{probe="icmp", class="anchor"}
          ) > 0.2
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Anchor RTT elevated: {{ $labels.zone_src }} to {{ $labels.zone_dst }}"

      - alert: MeshZoneUnreachable
        expr: |
          avg by (zone_src, zone_dst) (
            mesh_loss_ratio{probe="udp"}
          ) >= 1.0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Total loss between {{ $labels.zone_src }} and {{ $labels.zone_dst }}"

      - alert: MeshSlotsUnfilled
        expr: mesh_slots_unfilled > 0
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "{{ $value }} slots have no host assigned"

      - alert: MeshInventoryStale
        expr: mesh_provider_cache_age_seconds > 7200
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Inventory cache is {{ $value | humanizeDuration }} old"

      - alert: MeshHostsUnresolved
        expr: mesh_hosts_unresolved > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "{{ $value }} hosts have no zone under the current rule"

      - alert: MeshAssignmentChurn
        expr: rate(mesh_slot_changes_total[1h]) > 0.01
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "Sustained slot reassignment; check release_hold and host stability"

      - alert: MeshCardinalityLimit
        expr: increase(mesh_series_dropped_total[10m]) > 0
        labels:
          severity: critical
        annotations:
          summary: "Metric series limit reached; samples are being dropped"
```

Note that `MeshHighLatency` filters on `class="anchor"`. Anchor slots have fixed endpoints, so an alert on them is an alert on the path. Alerting on all slots would fire when a diverse slot picked up a slower machine.

---

# 16. Troubleshooting

## 16.1 No metrics at all

```bash
curl -s localhost:9101/tasks | jq '.tasks | length'
```

If zero:

```bash
# What does this node think it is called?
curl -s localhost:9101/config | jq '.node_id'

# Is that name in the inventory?
curl -s localhost:9101/inventory | jq '.hosts[].id'
```

**`node_id` must match a host ID exactly.** This is the most common problem by a wide margin. For Kubernetes the form is `k8s://<cluster_name>/<node_name>`.

## 16.2 Hosts are unresolved

```bash
curl -s localhost:9101/zones | jq '.unresolved'
```

Output names the specific missing attribute:

```json
[{ "host_id": "web-005...", "missing_key": "dc_instance" }]
```

Then inspect what that host actually has:

```bash
curl -s localhost:9101/inventory | jq '.hosts[] | select(.id=="web-005...") | .attributes'
```

Either fix the attribute source, or change the zone rule to use an attribute that exists.

## 16.3 All UDP probes fail

```bash
# Is the responder up on the far side?
curl -s http://<remote>:9101/config | jq '.responder'

# Is it receiving anything?
curl -s http://<remote>:9101/metrics | grep mesh_responder_total
```

| Observation | Meaning |
|---|---|
| `udp_packets` is 0 | Nothing is arriving. Firewall or wrong port |
| `udp_rejected` rising | Something is arriving but failing the magic check. Port collision with another service |

Confirm the port matches on both sides:

```bash
curl -s localhost:9101/config | jq '.probes.udp.port, .responder.udp_listen'
```

## 16.4 ICMP unavailable

```
level=WARN msg=meshping message="meshping: listen ip4:icmp 0.0.0.0: socket: operation not permitted"
level=ERROR msg="icmp probing disabled; meshping has no permission"
```

```bash
getcap /usr/local/bin/meshping
sudo setcap cap_net_raw+ep /usr/local/bin/meshping
sudo systemctl restart meshd
```

In systemd, confirm `NoNewPrivileges=false`. In Kubernetes, add `NET_RAW` to the container capabilities and run `setcap` in the image build.

If you cannot grant it, set `probes.icmp.enabled: false` to remove the warning.

## 16.5 Slots are unfilled

```bash
curl -s localhost:9101/pairings | jq '.pairings[] | select(.filled < .slots)'
curl -s localhost:9101/health | jq '.counts'
```

Causes, in order of likelihood:

1. A zone has fewer eligible hosts than `slots.count` and `allow_reuse` is false. Set it true or lower the count.
2. Hosts are in `unhealthy`, `cooldown`, or `ineligible`. Check `/health` for which and why.
3. A zone has only one member and `intra_zone` is true. That pairing cannot be filled.

## 16.6 Time series broke unexpectedly

```bash
curl -s localhost:9101/state | jq '.last_delta.sides_changed'
```

The `reason` field on each change names the cause:

| Reason | What happened |
|---|---|
| `host_gone` | The host left the inventory |
| `host_unhealthy` | The host failed probes past the hold |
| `zone_changed` | A label or attribute changed the host's zone |
| `class_changed` | Slot configuration changed |

Also check:

```promql
increase(mesh_state_reset_total[1h])
```

If non-zero, the state file was discarded — corrupt, or a zone rule fingerprint mismatch after a configuration change.

## 16.7 Too many series

```bash
curl -s localhost:9101/metrics | grep -E 'mesh_series_(count|dropped)'
```

Reduce, in order of effect:

1. Raise the zone level: `[metro, dc_instance]` → `[metro]`.
2. Lower `slots.count`.
3. Set `super_hosts: 0` if super testing is enabled.
4. Lower `super_max_targets`.
5. Exclude uninteresting pairings.
6. Disable a probe type.

## 16.8 Reconcile is failing

```
level=ERROR msg="reconcile aborted, keeping previous assignment"
```

The pairing count exceeded `max_pairings`. The message names the counts. **The previous assignment is kept**, so nothing broke — but nothing new is being applied either.

Almost always this means a zone rule change produced far more zones than expected. Check:

```bash
curl -s localhost:9101/zones | jq '.zones | length'
```

Fix the rule, or raise the limit if the count is genuinely intended.

## 16.9 Debug logging

```yaml
log:
  level: debug
  format: text
```

Reload with `systemctl reload meshd`. Debug level shows each provider poll, each reconcile trigger, and each task set change. Return to `info` afterwards; debug is noisy on a large mesh.

