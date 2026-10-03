package operator

import (
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
)

// newSessionTargetSnapshot freezes the first non-empty, grant-authorized
// target resolution. Per-node TracerConfig/system-namespace fields are
// filled in by enrichSnapshotNodes once the configs resolve; a snapshot
// can briefly exist without them (it is persisted before any child
// object is created), in which case later passes resolve and enrich
// them from the frozen node set only.
func newSessionTargetSnapshot(targets sessionTargets, now time.Time) *podtracev1alpha1.SessionTargetSnapshot {
	nodes := make([]podtracev1alpha1.SessionTargetNode, 0, len(targets.Nodes))
	for _, n := range targets.Nodes {
		nodes = append(nodes, podtracev1alpha1.SessionTargetNode{Name: n})
	}
	resolvedAt := metav1.NewTime(now)
	return &podtracev1alpha1.SessionTargetSnapshot{
		Nodes:            nodes,
		Namespaces:       append([]string(nil), targets.Namespaces...),
		Pods:             append([]podtracev1alpha1.SessionTargetPod(nil), targets.Pods...),
		PodRefs:          append([]podtracev1alpha1.PodRef(nil), targets.PodRefs...),
		DeniedNamespaces: append([]string(nil), targets.DeniedNamespaces...),
		ResolvedAt:       &resolvedAt,
	}
}

// migrateSnapshotFromStatus recovers a frozen target set for a session
// that started on an older operator release (no snapshot in status, but
// per-node Jobs already recorded). The node set is seeded from the
// recorded Jobs rather than the live selector so a rollout during the
// upgrade cannot add or remove nodes; currently-visible pods on those
// nodes are captured for backfilled Job specs. Best effort, once.
func migrateSnapshotFromStatus(s *podtracev1alpha1.PodTraceSession, targets sessionTargets, now time.Time) *podtracev1alpha1.SessionTargetSnapshot {
	recorded := map[string]struct{}{}
	for i := range s.Status.Jobs {
		if s.Status.Jobs[i].Node != "" {
			recorded[s.Status.Jobs[i].Node] = struct{}{}
		}
	}
	if len(recorded) == 0 {
		// Defensive: migration is only called with recorded Job refs;
		// fall back to the live set rather than producing an empty
		// snapshot if that invariant ever changes.
		for _, n := range targets.Nodes {
			recorded[n] = struct{}{}
		}
	}
	nodes := make([]string, 0, len(recorded))
	for n := range recorded {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	allowed := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		allowed[n] = struct{}{}
	}
	pods := make([]podtracev1alpha1.SessionTargetPod, 0, len(targets.Pods))
	for _, p := range targets.Pods {
		if _, ok := allowed[p.Node]; ok {
			pods = append(pods, p)
		}
	}

	out := newSessionTargetSnapshot(sessionTargets{
		Nodes:            nodes,
		Namespaces:       targets.Namespaces,
		PodRefs:          targets.PodRefs,
		DeniedNamespaces: targets.DeniedNamespaces,
		Pods:             pods,
	}, now)
	// Job refs carry neither TracerConfig nor system namespace, so the
	// per-node records are enriched by the normal resolution on the next
	// pass — strictly against this recovered node set.
	return out
}

// targetsFromSnapshot rebuilds the internal sessionTargets view every
// downstream consumer reads from. After freezing, the live selector is
// never consulted again.
func targetsFromSnapshot(snap *podtracev1alpha1.SessionTargetSnapshot) sessionTargets {
	nodes := make([]string, 0, len(snap.Nodes))
	for i := range snap.Nodes {
		nodes = append(nodes, snap.Nodes[i].Name)
	}
	pods := make([]podtracev1alpha1.SessionTargetPod, 0, len(snap.Pods))
	for i := range snap.Pods {
		pods = append(pods, snap.Pods[i])
	}
	return sessionTargets{
		Nodes:            nodes,
		Namespaces:       append([]string(nil), snap.Namespaces...),
		PodRefs:          append([]podtracev1alpha1.PodRef(nil), snap.PodRefs...),
		DeniedNamespaces: append([]string(nil), snap.DeniedNamespaces...),
		Pods:             pods,
	}
}

// snapshotPrimarySystemNamespace is the deterministic system namespace
// for session-wide decisions (report-location harvest): the one
// recorded on the lowest-sorted snapshot node.
func snapshotPrimarySystemNamespace(snap *podtracev1alpha1.SessionTargetSnapshot, fallback string) string {
	for i := range snap.Nodes {
		if snap.Nodes[i].SystemNamespace != "" {
			return snap.Nodes[i].SystemNamespace
		}
	}
	return fallback
}

// enrichSnapshotNodes fills the TracerConfig name and system namespace
// recorded for snapshot nodes that have not been enriched yet, without
// ever changing a record that is already populated. The snapshot slice
// is re-sorted by node name for a stable status merge key.
func enrichSnapshotNodes(snap *podtracev1alpha1.SessionTargetSnapshot, resolved sessionTracerConfigs, fallback string) {
	for i := range snap.Nodes {
		rec := &snap.Nodes[i]
		if rec.TracerConfigName == "" {
			if tc := resolved.forNode(rec.Name); tc != nil {
				rec.TracerConfigName = tc.Name
			}
		}
		if rec.SystemNamespace == "" {
			if tc := resolved.forNode(rec.Name); tc != nil {
				rec.SystemNamespace = systemNamespaceForSession(tc, fallback)
			} else if fallback != "" {
				rec.SystemNamespace = fallback
			}
		}
	}
	sort.Slice(snap.Nodes, func(i, j int) bool { return snap.Nodes[i].Name < snap.Nodes[j].Name })
}
