// Package k8s lists and watches Node objects and converts each node into
// a host record. It applies no schema of its own: every label becomes an
// attribute, and the zone rule decides which attributes matter.
package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/inventory"
	"github.com/example/mesh/internal/provider"
)

// Name is the source identifier.
const Name = "k8s"

// WellKnownLabels maps standard Kubernetes label names to the short
// attribute keys used by zone rules.
var WellKnownLabels = map[string]string{
	"topology.kubernetes.io/region":    "k8s.region",
	"topology.kubernetes.io/zone":      "k8s.zone",
	"node.kubernetes.io/instance-type": "k8s.instance-type",
	"kubernetes.io/arch":               "k8s.arch",
	"kubernetes.io/os":                 "k8s.os",
	"kubernetes.io/hostname":           "k8s.hostname",
}

// DefaultTaintDeny lists the taints that mark a node ineligible.
var DefaultTaintDeny = []string{
	"node.kubernetes.io/unreachable",
	"node.kubernetes.io/not-ready",
	"node.kubernetes.io/unschedulable",
}

// Provider watches Node objects.
type Provider struct {
	cfg       config.K8sConfig
	client    kubernetes.Interface
	selector  labels.Selector
	taintDeny map[string]bool
	annAllow  map[string]bool
	refresh   chan struct{}
	dirty     chan struct{}
}

// New builds a Provider using in-cluster credentials when available and
// the kubeconfig path otherwise.
func New(cfg config.K8sConfig) (*Provider, error) {
	restCfg, err := buildRestConfig(cfg.Kubeconfig)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("k8s: build client: %w", err)
	}
	sel := labels.Everything()
	if cfg.LabelSelector != "" {
		sel, err = labels.Parse(cfg.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("k8s: parse label_selector: %w", err)
		}
	}
	deny := make(map[string]bool)
	list := cfg.TaintDeny
	if len(list) == 0 {
		list = DefaultTaintDeny
	}
	for _, t := range list {
		deny[t] = true
	}
	allow := make(map[string]bool, len(cfg.AnnotationAllow))
	for _, a := range cfg.AnnotationAllow {
		allow[a] = true
	}
	return &Provider{
		cfg:       cfg,
		client:    client,
		selector:  sel,
		taintDeny: deny,
		annAllow:  allow,
		refresh:   make(chan struct{}, 1),
		dirty:     make(chan struct{}, 1),
	}, nil
}

func (p *Provider) Name() string { return Name }

// Refresh requests an immediate emission of the current node set.
func (p *Provider) Refresh() {
	select {
	case p.refresh <- struct{}{}:
	default:
	}
}

// Run starts the informer and emits a complete set on every resync and
// on every add, update, or delete event, with a short debounce so that a
// rolling node update does not produce one set per node.
func (p *Provider) Run(ctx context.Context, sink provider.Sink) error {
	resync := p.cfg.Resync.D()
	if resync <= 0 {
		resync = 300 * time.Second
	}
	factory := informers.NewSharedInformerFactoryWithOptions(
		p.client, resync,
		informers.WithTweakListOptions(func(o *metaListOptions) {
			o.LabelSelector = p.cfg.LabelSelector
		}),
	)
	informer := factory.Core().V1().Nodes()
	lister := informer.Lister()

	mark := func(any) { p.markDirty() }
	if _, err := informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    mark,
		UpdateFunc: func(any, any) { p.markDirty() },
		DeleteFunc: mark,
	}); err != nil {
		return fmt.Errorf("k8s: add event handler: %w", err)
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.Informer().HasSynced) {
		return fmt.Errorf("k8s: node cache did not sync")
	}

	debounce := p.cfg.Debounce.D()
	if debounce <= 0 {
		debounce = 2 * time.Second
	}
	interval := p.cfg.Interval.D()
	if interval <= 0 {
		interval = 60 * time.Second
	}

	emit := func() {
		nodes, err := lister.List(p.selector)
		if err != nil {
			slog.Error("k8s node list failed", "error", err)
			return
		}
		set := p.build(nodes)
		sink.Replace(set)
		slog.Debug("k8s node set emitted", "nodes", len(set.Hosts))
	}
	emit()

	timer := time.NewTimer(interval)
	defer timer.Stop()
	var pending *time.Timer
	defer func() {
		if pending != nil {
			pending.Stop()
		}
	}()
	pendingC := make(<-chan time.Time)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.dirty:
			if pending != nil {
				pending.Stop()
			}
			pending = time.NewTimer(debounce)
			pendingC = pending.C
		case <-pendingC:
			pendingC = make(<-chan time.Time)
			emit()
		case <-p.refresh:
			emit()
		case <-timer.C:
			emit()
			timer.Reset(interval)
		}
	}
}

func (p *Provider) markDirty() {
	select {
	case p.dirty <- struct{}{}:
	default:
	}
}

// build converts a node list into a host set.
func (p *Provider) build(nodes []*corev1.Node) inventory.Set {
	now := time.Now()
	set := inventory.Set{Source: Name, FetchAt: now}
	set.Hosts = make([]inventory.HostRecord, 0, len(nodes))
	for _, n := range nodes {
		rec, err := p.convert(n)
		if err != nil {
			slog.Debug("k8s node skipped", "node", n.Name, "error", err)
			continue
		}
		rec.SeenAt = now
		set.Hosts = append(set.Hosts, rec)
	}
	return set
}

// convert turns one Node into a host record.
func (p *Provider) convert(n *corev1.Node) (inventory.HostRecord, error) {
	addr, err := p.address(n)
	if err != nil {
		return inventory.HostRecord{}, err
	}
	healthy, reason := p.health(n)
	return inventory.HostRecord{
		ID:         p.hostID(n.Name),
		Address:    addr,
		Attributes: p.attributes(n),
		Source:     Name,
		Healthy:    healthy,
		Reason:     reason,
		Priority:   p.cfg.Priority,
	}, nil
}

// attributes extracts the attribute map from a Node. Every label is
// copied with a prefix so that the origin stays visible, and the
// well-known labels are additionally copied to short keys.
func (p *Provider) attributes(n *corev1.Node) map[string]string {
	attrs := make(map[string]string, len(n.Labels)+8)
	attrs["k8s.cluster"] = p.cfg.ClusterName
	attrs["k8s.node"] = n.Name

	for k, v := range n.Labels {
		attrs["k8s.label."+sanitizeKey(k)] = v
		if short, ok := WellKnownLabels[k]; ok {
			attrs[short] = v
		}
	}
	for k, v := range n.Annotations {
		if p.annAllow[k] {
			attrs["k8s.annotation."+sanitizeKey(k)] = v
		}
	}
	if n.Spec.ProviderID != "" {
		attrs["k8s.provider-id"] = n.Spec.ProviderID
	}
	if n.Status.NodeInfo.KubeletVersion != "" {
		attrs["k8s.kubelet"] = n.Status.NodeInfo.KubeletVersion
	}
	return attrs
}

// address selects the node address using the configured preference
// order.
func (p *Provider) address(n *corev1.Node) (string, error) {
	order := p.cfg.AddressOrder
	if len(order) == 0 {
		order = []string{"InternalIP", "Hostname"}
	}
	for _, want := range order {
		for _, a := range n.Status.Addresses {
			if strings.EqualFold(string(a.Type), want) && a.Address != "" {
				return a.Address, nil
			}
		}
	}
	return "", fmt.Errorf("no address of types %v", order)
}

// health evaluates the Ready condition, the unschedulable field, the
// taints, and the deletion timestamp. Provider ineligibility is
// authoritative and does not pass through the probe hysteresis.
func (p *Provider) health(n *corev1.Node) (bool, string) {
	if n.DeletionTimestamp != nil {
		return false, "node is being deleted"
	}
	if n.Spec.Unschedulable {
		return false, "node is unschedulable"
	}
	for _, t := range n.Spec.Taints {
		if p.taintDeny[t.Key] {
			return false, "denied taint " + t.Key
		}
	}
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			if c.Status != corev1.ConditionTrue {
				return false, "node is not ready"
			}
			return true, ""
		}
	}
	return false, "node has no Ready condition"
}

// hostID returns k8s://<cluster>/<node>.
func (p *Provider) hostID(name string) string {
	return "k8s://" + p.cfg.ClusterName + "/" + name
}

// sanitizeKey replaces characters that are not valid in an attribute
// key.
func sanitizeKey(s string) string {
	return strings.NewReplacer("/", "_", ".", "_", " ", "_").Replace(s)
}

// buildRestConfig prefers in-cluster credentials and falls back to a
// kubeconfig path or the standard client configuration rules.
func buildRestConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		if cfg, err := rest.InClusterConfig(); err == nil {
			return cfg, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s: build rest config: %w", err)
	}
	return cfg, nil
}

