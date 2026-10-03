package operator

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
	"github.com/gma1k/podtrace/internal/fleet"
)

// errNoTracerConfig reports that nothing resolved for a node.
//
// This used to be silent: resolveTracerConfig returned (nil, nil) when the
// default config was missing, and buildSessionJobSpec then produced a Job
// with an empty image. The apiserver rejects that with a message about
// container spec validation, which says nothing about the actual cause, so
// the failure surfaces as a broken Job rather than a broken configuration.
type errNoTracerConfig struct {
	node    string
	pinned  string
	reason  string
	configs []string
}

func (e *errNoTracerConfig) Error() string {
	if e.pinned != "" {
		return fmt.Sprintf("spec.tracerConfigRef %q: %s", e.pinned, e.reason)
	}
	return fmt.Sprintf(
		"no TracerConfig resolves for node %q: %s. Existing configs: %v. "+
			"Either label the node into a fleet, create a TracerConfig named %q, "+
			"or pin one with spec.tracerConfigRef",
		e.node, e.reason, e.configs, DefaultTracerConfigName)
}

// sessionTracerConfigs holds the per-node resolution for one session.
type sessionTracerConfigs struct {
	byNode     map[string]*podtracev1alpha1.TracerConfig
	namespaces []string
	// systemNSByNode pins the system namespace a node's Job lives in
	// when the resolution was driven by a frozen target snapshot, so a
	// later edit of the TracerConfig's systemNamespace cannot move Job
	// (re)creation for a snapshotted node.
	systemNSByNode map[string]string
}

// forNode returns the config resolved for a node, or nil when the node was
// not part of the resolution.
func (s sessionTracerConfigs) forNode(node string) *podtracev1alpha1.TracerConfig {
	if s.byNode == nil {
		return nil
	}
	return s.byNode[node]
}

// namespaceForNode returns the system namespace a node's Job belongs in.
func (s sessionTracerConfigs) namespaceForNode(node, fallback string) string {
	if s.systemNSByNode != nil {
		if ns, ok := s.systemNSByNode[node]; ok && ns != "" {
			return ns
		}
	}
	return systemNamespaceForSession(s.forNode(node), fallback)
}

// resolveSessionTracerConfigs works out which TracerConfig governs each of a
// session's target nodes.
//
// Order per node:
//
//  1. spec.tracerConfigRef, when set — an explicit pin wins everywhere, so a
//     session can be made reproducible regardless of where its pods land.
//  2. The fleet whose scheduling constraints target that node, taken from
//     the same partition the TracerConfig reconciler reports on. When two
//     fleets contest a node, the partition's precedence rule (priority, then
//     age, then name) picks one — the same answer the Conflict condition
//     advertises, so the session agrees with what the operator told the user.
//  3. The config named "default".
//
// Exhausting all three is an error, not a nil config: a Job built without a
// TracerConfig has no image.
func (r *PodTraceSessionReconciler) resolveSessionTracerConfigs(
	ctx context.Context,
	s *podtracev1alpha1.PodTraceSession,
	nodes []string,
) (sessionTracerConfigs, error) {
	out := sessionTracerConfigs{byNode: map[string]*podtracev1alpha1.TracerConfig{}}

	if ref := s.Spec.TracerConfigRef; ref != nil && ref.Name != "" {
		pinned, err := r.resolvePinnedTracerConfig(ctx, s)
		if err != nil {
			return out, err
		}
		for _, node := range nodes {
			out.byNode[node] = pinned
		}
		out.namespaces = distinctNamespaces(out.byNode, r.SystemNamespace)
		return out, nil
	}

	inv, err := r.gatherTracerConfigInventory(ctx)
	if err != nil {
		return out, err
	}
	for _, node := range nodes {
		tc, err := inv.resolveNode(node)
		if err != nil {
			return out, err
		}
		out.byNode[node] = tc
	}

	out.namespaces = distinctNamespaces(out.byNode, r.SystemNamespace)
	return out, nil
}

// resolvePinnedTracerConfig loads the spec.tracerConfigRef config and
// verifies it still admits sessions from the session's namespace.
func (r *PodTraceSessionReconciler) resolvePinnedTracerConfig(
	ctx context.Context,
	s *podtracev1alpha1.PodTraceSession,
) (*podtracev1alpha1.TracerConfig, error) {
	ref := s.Spec.TracerConfigRef
	var pinned podtracev1alpha1.TracerConfig
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name}, &pinned); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &errNoTracerConfig{pinned: ref.Name, reason: "TracerConfig not found"}
		}
		return nil, fmt.Errorf("get TracerConfig %q: %w", ref.Name, err)
	}
	if !podtracev1alpha1.TracerConfigAllowsSessionFrom(&pinned, s.Namespace, r.SystemNamespace) {
		return nil, &errNoTracerConfig{
			pinned: ref.Name,
			reason: fmt.Sprintf("not entitled: TracerConfig %q does not grant sessions from namespace %q (set annotation %s on the config)",
				ref.Name, s.Namespace, podtracev1alpha1.AllowSessionsFromAnnotation),
		}
	}
	return &pinned, nil
}

// tracerConfigInventory is the cached view all non-pinned per-node
// resolution reads from within one reconcile pass.
type tracerConfigInventory struct {
	byName map[string]*podtracev1alpha1.TracerConfig
	names  []string
	winner map[string]string
}

// gatherTracerConfigInventory lists every TracerConfig and computes the
// fleet partition. Node objects are only needed to run the partition:
// losing that read is not fatal and falls through to "default".
func (r *PodTraceSessionReconciler) gatherTracerConfigInventory(ctx context.Context) (*tracerConfigInventory, error) {
	var configs podtracev1alpha1.TracerConfigList
	if err := r.List(ctx, &configs); err != nil {
		return nil, fmt.Errorf("list TracerConfigs: %w", err)
	}
	inv := &tracerConfigInventory{
		byName: map[string]*podtracev1alpha1.TracerConfig{},
		winner: map[string]string{},
	}
	for i := range configs.Items {
		inv.byName[configs.Items[i].Name] = &configs.Items[i]
		inv.names = append(inv.names, configs.Items[i].Name)
	}
	sort.Strings(inv.names)

	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err == nil {
		inv.winner = fleet.ComputePartition(configs.Items, nodeList.Items).Winner
	}
	return inv, nil
}

// resolveNode applies the fleet-partition-then-default order for one
// node that is not governed by an explicit spec.tracerConfigRef.
func (inv *tracerConfigInventory) resolveNode(node string) (*podtracev1alpha1.TracerConfig, error) {
	if name, ok := inv.winner[node]; ok {
		if tc := inv.byName[name]; tc != nil {
			return tc, nil
		}
	}
	if tc := inv.byName[DefaultTracerConfigName]; tc != nil {
		return tc, nil
	}
	reason := "no fleet targets it and there is no default TracerConfig"
	if len(inv.names) == 0 {
		reason = "the cluster has no TracerConfig at all"
	}
	return nil, &errNoTracerConfig{node: node, reason: reason, configs: inv.names}
}

// resolveSnapshotTracerConfigs resolves TracerConfigs strictly against a
// frozen target snapshot. A node whose snapshot record already pins a
// TracerConfig name always resolves to that exact config — relabels,
// fleet partition changes, or a new default config cannot move it; if
// that config is gone and the node's Job already completed, the missing
// config is tolerated, otherwise it is the same terminal configuration
// error as a fresh resolve. Nodes recorded without a pinned name (the
// brief pre-enrichment window after the snapshot is persisted) resolve
// once via the normal order and are returned for enrichment; the
// snapshot slice itself is updated in place by the caller.
func (r *PodTraceSessionReconciler) resolveSnapshotTracerConfigs(
	ctx context.Context,
	s *podtracev1alpha1.PodTraceSession,
	snap *podtracev1alpha1.SessionTargetSnapshot,
	completedNodes map[string]struct{},
) (sessionTracerConfigs, error) {
	out := sessionTracerConfigs{byNode: map[string]*podtracev1alpha1.TracerConfig{}}

	needPinned := s.Spec.TracerConfigRef != nil && s.Spec.TracerConfigRef.Name != ""
	var pinned *podtracev1alpha1.TracerConfig
	if needPinned {
		p, err := r.resolvePinnedTracerConfig(ctx, s)
		if err != nil {
			var missing *errNoTracerConfig
			allDone := len(completedNodes) > 0 && len(completedNodes) == len(snap.Nodes)
			if errors.As(err, &missing) && missing.pinned != "" && allDone {
				// The pinned config is gone but every snapshotted node
				// already has a terminal Job: the session must not be
				// reopened or failed, and no Job spec will rebuild.
				p = nil
			} else {
				return out, err
			}
		}
		pinned = p
	}
	var inv *tracerConfigInventory
	if !needPinned {
		i, err := r.gatherTracerConfigInventory(ctx)
		if err != nil {
			return out, err
		}
		inv = i
	}

	nsSeen := map[string]struct{}{}
	out.systemNSByNode = map[string]string{}
	for i := range snap.Nodes {
		rec := &snap.Nodes[i]
		var tc *podtracev1alpha1.TracerConfig
		nodeNS := ""
		switch {
		case rec.TracerConfigName != "" && needPinned && pinned != nil && rec.TracerConfigName == pinned.Name:
			tc = pinned
		case rec.TracerConfigName != "":
			if inv != nil {
				tc = inv.byName[rec.TracerConfigName]
			}
			if tc == nil {
				if _, done := completedNodes[rec.Name]; done {
					// The Job for this node is already terminal; nothing
					// will rebuild its spec, so a since-deleted config
					// must not reopen or fail the session.
					tc = nil
					break
				}
				return out, &errNoTracerConfig{
					pinned: rec.TracerConfigName,
					reason: "TracerConfig not found after the session target snapshot was taken",
				}
			}
		case needPinned:
			tc = pinned
		default:
			t, err := inv.resolveNode(rec.Name)
			if err != nil {
				return out, err
			}
			tc = t
		}
		out.byNode[rec.Name] = tc
		if rec.SystemNamespace != "" {
			nodeNS = rec.SystemNamespace
		} else if tc != nil {
			nodeNS = systemNamespaceForSession(tc, r.SystemNamespace)
		} else {
			nodeNS = r.SystemNamespace
		}
		if nodeNS != "" {
			nsSeen[nodeNS] = struct{}{}
			out.systemNSByNode[rec.Name] = nodeNS
		}
	}

	if len(nsSeen) == 0 && r.SystemNamespace != "" {
		nsSeen[r.SystemNamespace] = struct{}{}
	}
	out.namespaces = make([]string, 0, len(nsSeen))
	for ns := range nsSeen {
		out.namespaces = append(out.namespaces, ns)
	}
	sort.Strings(out.namespaces)
	return out, nil
}

// distinctNamespaces returns the sorted set of system namespaces the
// resolved configs map to.
func distinctNamespaces(byNode map[string]*podtracev1alpha1.TracerConfig, fallback string) []string {
	seen := map[string]struct{}{}
	for _, tc := range byNode {
		seen[systemNamespaceForSession(tc, fallback)] = struct{}{}
	}
	if len(seen) == 0 {
		seen[fallback] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for ns := range seen {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// primary returns the config used for session-wide decisions that are not
// per-node, such as the concurrency cap. Deterministic: the config of the
// lowest-sorted node that still has one (nodes whose Jobs completed before
// a since-deleted TracerConfig can carry nil).
func (s sessionTracerConfigs) primary() *podtracev1alpha1.TracerConfig {
	nodes := make([]string, 0, len(s.byNode))
	for node := range s.byNode {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		if s.byNode[node] != nil {
			return s.byNode[node]
		}
	}
	return nil
}
