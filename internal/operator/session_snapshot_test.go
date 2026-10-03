package operator

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
)

const (
	snapSysNS   = "snap-sys"
	snapUID     = "uid-snap-0001"
	snapSession = "snap-session"
)

func snapSessionObject(mutate func(*podtracev1alpha1.PodTraceSession)) *podtracev1alpha1.PodTraceSession {
	s := &podtracev1alpha1.PodTraceSession{
		ObjectMeta: metav1.ObjectMeta{
			Name:       snapSession,
			Namespace:  "default",
			UID:        snapUID,
			Finalizers: []string{FinalizerCleanup},
		},
		Spec: podtracev1alpha1.PodTraceSessionSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Duration: metav1.Duration{Duration: time.Minute},
		},
	}
	if mutate != nil {
		mutate(s)
	}
	return s
}

func snapPod(name, node string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func snapDefaultTC(systemNS string) *podtracev1alpha1.TracerConfig {
	return &podtracev1alpha1.TracerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultTracerConfigName},
		Spec: podtracev1alpha1.TracerConfigSpec{
			Image:           "ghcr.io/gma1k/podtrace:test",
			SystemNamespace: systemNS,
		},
	}
}

func newSnapReconciler(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) (*PodTraceSessionReconciler, client.WithWatch) {
	t.Helper()
	scheme := newOperatorScheme(t)
	builder := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&podtracev1alpha1.PodTraceSession{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs)
	c := builder.Build()
	return &PodTraceSessionReconciler{Client: c, APIReader: c, Scheme: scheme, SystemNamespace: snapSysNS}, c
}

func snapReconcile(t *testing.T, r *PodTraceSessionReconciler) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: snapSession, Namespace: "default"},
	})
}

func snapSessionGet(t *testing.T, c client.Reader) *podtracev1alpha1.PodTraceSession {
	t.Helper()
	var s podtracev1alpha1.PodTraceSession
	if err := c.Get(context.Background(), types.NamespacedName{Name: snapSession, Namespace: "default"}, &s); err != nil {
		t.Fatalf("get session: %v", err)
	}
	return &s
}

func snapSessionJobs(t *testing.T, c client.Reader) map[string]batchv1.Job {
	t.Helper()
	var list batchv1.JobList
	if err := c.List(context.Background(), &list, client.MatchingLabels{
		LabelManagedBy:   ManagedByValue,
		LabelComponent:   ComponentSession,
		LabelSessionName: snapSession,
		LabelSessionNS:   "default",
	}); err != nil {
		t.Fatalf("list session jobs: %v", err)
	}
	out := map[string]batchv1.Job{}
	for i := range list.Items {
		out[list.Items[i].Labels[LabelNodeName]] = list.Items[i]
	}
	return out
}

// TestSnapshotReconcile_FreezesOnFirstMatchAndIgnoresRollout is the core
// scenario: after the first non-empty resolution, pods rolling to a new
// node neither add Jobs nor let the session complete early, and Job specs
// enumerate the frozen pods instead of re-expanding the selector.
func TestSnapshotReconcile_FreezesOnFirstMatchAndIgnoresRollout(t *testing.T) {
	s := snapSessionObject(nil)
	tc := snapDefaultTC(snapSysNS)
	podA := snapPod("web-a", "node-a", map[string]string{"app": "web"})
	podB := snapPod("web-b", "node-b", map[string]string{"app": "web"})
	r, c := newSnapReconciler(t, interceptor.Funcs{}, s, tc, podA, podB)

	if _, err := snapReconcile(t, r); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	got := snapSessionGet(t, c)
	if got.Status.TargetSnapshot == nil {
		t.Fatal("target snapshot must be persisted on first non-empty resolution")
	}
	if len(got.Status.TargetSnapshot.Nodes) != 2 {
		t.Fatalf("snapshot nodes = %v, want node-a,node-b", got.Status.TargetSnapshot.Nodes)
	}
	joinedPods := []string{}
	for _, p := range got.Status.TargetSnapshot.Pods {
		joinedPods = append(joinedPods, p.Namespace+"/"+p.Name+"@"+p.Node)
	}
	if len(joinedPods) != 2 {
		t.Fatalf("snapshot pods = %v, want two frozen pods", joinedPods)
	}

	jobs := snapSessionJobs(t, c)
	if len(jobs) != 2 {
		t.Fatalf("jobs = %v, want one per initial node", jobs)
	}
	jobA := jobs["node-a"]
	var args string
	for _, ct := range jobA.Spec.Template.Spec.Containers {
		if ct.Name == "podtrace" {
			args = strings.Join(ct.Args, " ")
		}
	}
	if !strings.Contains(args, "--pods default/web-a,default/web-b") {
		t.Errorf("job args must enumerate frozen pods, got %v", args)
	}
	if strings.Contains(args, "--pod-selector") {
		t.Errorf("frozen job must not carry a live selector, got %v", args)
	}

	// Deployment rolls: both original pods disappear, a new pod lands on
	// a brand new node that matches the very same selector.
	if err := c.Delete(context.Background(), podA); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), podB); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), snapPod("web-c", "node-c", map[string]string{"app": "web"})); err != nil {
		t.Fatal(err)
	}

	// Simulate an operator restart: a brand new reconciler over the same
	// persisted state.
	r2 := &PodTraceSessionReconciler{Client: c, APIReader: c, Scheme: r.Scheme, SystemNamespace: snapSysNS}
	if _, err := snapReconcile(t, r2); err != nil {
		t.Fatalf("post-restart reconcile: %v", err)
	}

	jobs = snapSessionJobs(t, c)
	if _, ok := jobs["node-c"]; ok {
		t.Error("node-c appeared only after the snapshot; it must not be absorbed into this session")
	}
	if len(jobs) != 2 {
		t.Fatalf("jobs after rollout = %v, want still exactly node-a,node-b", jobs)
	}

	got = snapSessionGet(t, c)
	if got.Status.State == podtracev1alpha1.SessionStateCompleted {
		t.Fatal("session must not complete early when the live selector shrank")
	}
	if len(got.Status.TargetSnapshot.Nodes) != 2 {
		t.Fatalf("snapshot mutated after rollout: %+v", got.Status.TargetSnapshot.Nodes)
	}
}

// TestSnapshotReconcile_TransientJobFailureBackfillsOnlySnapshotNode:
// a Job whose create hit a transient API error before the snapshot was
// enriched is retried, but a node showing up only after recovery is not
// admitted.
func TestSnapshotReconcile_TransientJobFailureBackfillsOnlySnapshotNode(t *testing.T) {
	s := snapSessionObject(nil)
	tc := snapDefaultTC(snapSysNS)
	podA := snapPod("web-a", "node-a", map[string]string{"app": "web"})
	podB := snapPod("web-b", "node-b", map[string]string{"app": "web"})

	failNodeB := true
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if job, ok := obj.(*batchv1.Job); ok && job.Name == SessionJobName(snapUID, "node-b") && failNodeB {
				return errInternal()
			}
			return cl.Create(ctx, obj, opts...)
		},
	}
	r, c := newSnapReconciler(t, funcs, s, tc, podA, podB)

	if _, err := snapReconcile(t, r); err == nil {
		t.Fatal("transient Job create failure must surface as a reconcile error")
	}
	jobs := snapSessionJobs(t, c)
	if _, ok := jobs["node-a"]; !ok {
		t.Error("node-a Job should exist before the node-b failure")
	}
	if _, ok := jobs["node-b"]; ok {
		t.Error("node-b Job must not exist after a failed create")
	}
	if snapSessionGet(t, c).Status.TargetSnapshot == nil {
		t.Fatal("snapshot must be persisted even though Job creation then failed")
	}

	// Recovery: the API works again; meanwhile the rollout put a matching
	// pod onto node-c.
	failNodeB = false
	if err := c.Create(context.Background(), snapPod("web-c", "node-c", map[string]string{"app": "web"})); err != nil {
		t.Fatal(err)
	}
	if _, err := snapReconcile(t, r); err != nil {
		t.Fatalf("backfill reconcile: %v", err)
	}

	jobs = snapSessionJobs(t, c)
	if _, ok := jobs["node-b"]; !ok {
		t.Error("node-b Job must be backfilled after recovery")
	}
	if _, ok := jobs["node-c"]; ok {
		t.Error("node-c appeared after the snapshot; recovery must not admit it")
	}
}

// TestSnapshotReconcile_CompletedJobNotReopenedWhenPodVanishes locks the
// completion denominator and Job skip logic to the snapshot: a terminal
// node stays terminal even though nothing matches anymore, and a missing
// node is still (re)created.
func TestSnapshotReconcile_CompletedJobNotReopenedWhenPodVanishes(t *testing.T) {
	resolvedAt := metav1.NewTime(time.Now().Add(-time.Minute))
	s := snapSessionObject(func(s *podtracev1alpha1.PodTraceSession) {
		s.Status = podtracev1alpha1.PodTraceSessionStatus{
			Jobs: []podtracev1alpha1.SessionJobRef{
				{Node: "node-a", Name: SessionJobName(snapUID, "node-a"), Completed: true},
			},
			TargetSnapshot: &podtracev1alpha1.SessionTargetSnapshot{
				Nodes: []podtracev1alpha1.SessionTargetNode{
					{Name: "node-a", TracerConfigName: DefaultTracerConfigName, SystemNamespace: snapSysNS},
					{Name: "node-b", TracerConfigName: DefaultTracerConfigName, SystemNamespace: snapSysNS},
				},
				ResolvedAt: &resolvedAt,
			},
		}
	})
	tc := snapDefaultTC(snapSysNS)
	completedJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SessionJobName(snapUID, "node-a"),
			Namespace: snapSysNS,
			Labels: map[string]string{
				LabelManagedBy:   ManagedByValue,
				LabelComponent:   ComponentSession,
				LabelSessionName: snapSession,
				LabelSessionNS:   "default",
				LabelNodeName:    "node-a",
			},
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}

	createCount := map[string]int{}
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if job, ok := obj.(*batchv1.Job); ok {
				createCount[job.Labels[LabelNodeName]]++
			}
			return cl.Create(ctx, obj, opts...)
		},
	}
	r, c := newSnapReconciler(t, funcs, s, tc, completedJob)
	// No matching pods exist at all: the original workload is gone.

	if _, err := snapReconcile(t, r); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	jobs := snapSessionJobs(t, c)
	if createCount["node-a"] != 0 {
		t.Errorf("completed node-a Job must not be recreated, create count=%d", createCount["node-a"])
	}
	if _, ok := jobs["node-b"]; !ok {
		t.Error("node-b Job must still be created from the snapshot denominator")
	}
	got := snapSessionGet(t, c)
	if got.Status.State == podtracev1alpha1.SessionStateCompleted {
		t.Fatal("1/2 nodes done must not be reported Completed")
	}

	if _, err := snapReconcile(t, r); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if createCount["node-a"] != 0 {
		t.Errorf("completed node-a Job must not be recreated on later passes, count=%d", createCount["node-a"])
	}
}

// TestSnapshotReconcile_DeniedNamespaceReportedButNotSnapshotted keeps
// the existing consent behavior at first resolution: denied namespaces
// surface in condition + snapshot.deniedNamespaces but never produce a
// node, Job, or frozen target.
func TestSnapshotReconcile_DeniedNamespaceReportedButNotSnapshotted(t *testing.T) {
	s := snapSessionObject(func(s *podtracev1alpha1.PodTraceSession) {
		s.Spec.Selector = nil
		s.Spec.PodRefs = []podtracev1alpha1.PodRef{
			{Name: "app-0"},
			{Namespace: "team-b", Name: "ext"},
		}
	})
	tc := snapDefaultTC(snapSysNS)
	running := snapPod("app-0", "node-a", nil)
	r, c := newSnapReconciler(t, interceptor.Funcs{}, s, tc, running)

	if _, err := snapReconcile(t, r); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := snapSessionGet(t, c)
	if got.Status.TargetSnapshot == nil {
		t.Fatal("granted non-empty targets must be snapshotted")
	}
	if len(got.Status.TargetSnapshot.Nodes) != 1 || got.Status.TargetSnapshot.Nodes[0].Name != "node-a" {
		t.Fatalf("snapshot nodes = %+v, want only node-a", got.Status.TargetSnapshot.Nodes)
	}
	denied := got.Status.TargetSnapshot.DeniedNamespaces
	if len(denied) != 1 || denied[0] != "team-b" {
		t.Fatalf("denied namespaces = %v, want [team-b]", denied)
	}
	for _, p := range got.Status.TargetSnapshot.Pods {
		if p.Namespace == "team-b" {
			t.Fatalf("denied pod must never enter the snapshot: %+v", p)
		}
	}
	jobs := snapSessionJobs(t, c)
	if len(jobs) != 1 {
		t.Fatalf("jobs = %v, want exactly one for node-a", jobs)
	}
	cond := conditionForTest(got, ConditionReconciled)
	if cond == nil || !strings.Contains(cond.Message, "team-b") {
		t.Fatalf("reconciled condition must still report denied namespace, got %+v", cond)
	}

	// The team-b namespace grant is created later and its pods appear:
	// a subsequent reconcile must still report the frozen verdict and
	// must not admit team-b into this already-started session.
	teamB := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:        "team-b",
		Annotations: map[string]string{podtracev1alpha1.AllowTracingFromAnnotation: "default"},
	}}
	if err := c.Create(context.Background(), teamB); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: "team-b"},
		Spec:       corev1.PodSpec{NodeName: "node-z"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := snapReconcile(t, r); err != nil {
		t.Fatalf("reconcile after grant: %v", err)
	}
	got = snapSessionGet(t, c)
	denied = got.Status.TargetSnapshot.DeniedNamespaces
	if len(denied) != 1 || denied[0] != "team-b" {
		t.Fatalf("frozen denied verdict changed: %v", denied)
	}
	jobs = snapSessionJobs(t, c)
	if _, ok := jobs["node-z"]; ok {
		t.Error("newly granted namespace must not be absorbed after freeze")
	}
}

// TestSnapshotHelpers_RoundTripAndUpgradeMigration covers the pure
// helpers: a snapshot rebuilds the same target view, and an upgrade-time
// migration seeds the node set from recorded Job refs without admitting
// nodes the live rollout only just produced.
func TestSnapshotHelpers_RoundTripAndUpgradeMigration(t *testing.T) {
	now := time.Now()
	targets := sessionTargets{
		Nodes:            []string{"node-b", "node-a"},
		Namespaces:       []string{"team-a", "team-b"},
		DeniedNamespaces: []string{"team-c"},
		Pods: []podtracev1alpha1.SessionTargetPod{
			{Namespace: "team-a", Name: "app-0", Node: "node-a"},
			{Namespace: "team-b", Name: "app-1", Node: "node-b"},
		},
	}
	snap := newSessionTargetSnapshot(targets, now)
	if len(snap.Nodes) != 2 || snap.Nodes[0].Name != "node-b" || snap.Nodes[1].Name != "node-a" {
		t.Fatalf("snapshot nodes keep resolve order for stable merge: %+v", snap.Nodes)
	}
	rebuilt := targetsFromSnapshot(snap)
	if len(rebuilt.Nodes) != 2 || rebuilt.Nodes[0] != "node-b" || rebuilt.Nodes[1] != "node-a" {
		t.Fatalf("rebuilt nodes = %v", rebuilt.Nodes)
	}
	if len(rebuilt.Pods) != 2 || rebuilt.Pods[0].Name != "app-0" {
		t.Fatalf("rebuilt pods = %+v", rebuilt.Pods)
	}
	if len(rebuilt.DeniedNamespaces) != 1 || rebuilt.DeniedNamespaces[0] != "team-c" {
		t.Fatalf("rebuilt denied = %v", rebuilt.DeniedNamespaces)
	}

	// Upgrade migration: session recorded node-a/node-b jobs; the live
	// selector now matches a pod on a brand-new node-c and nothing on
	// node-a. Migration keeps the recorded nodes and only captures pods
	// on admitted nodes.
	old := &podtracev1alpha1.PodTraceSession{
		Status: podtracev1alpha1.PodTraceSessionStatus{
			Jobs: []podtracev1alpha1.SessionJobRef{
				{Node: "node-a", Name: "j-a", Completed: true},
				{Node: "node-b", Name: "j-b"},
			},
		},
	}
	live := sessionTargets{
		Nodes:      []string{"node-b", "node-c"},
		Pods:       []podtracev1alpha1.SessionTargetPod{{Namespace: "default", Name: "new", Node: "node-c"}},
		PodRefs:    nil,
		Namespaces: nil,
	}
	migrated := migrateSnapshotFromStatus(old, live, now)
	wantNodes := map[string]bool{"node-a": true, "node-b": true}
	if len(migrated.Nodes) != 2 {
		t.Fatalf("migration nodes = %+v, want recorded nodes only (node-a,node-b)", migrated.Nodes)
	}
	for _, n := range migrated.Nodes {
		if !wantNodes[n.Name] {
			t.Errorf("unexpected migrated node %q", n.Name)
		}
	}
	if len(migrated.Pods) != 0 {
		t.Fatalf("pods on live-only node-c must not be captured, got %+v", migrated.Pods)
	}
}

func conditionForTest(s *podtracev1alpha1.PodTraceSession, typ string) *metav1.Condition {
	for i := range s.Status.Conditions {
		if s.Status.Conditions[i].Type == typ {
			return &s.Status.Conditions[i]
		}
	}
	return nil
}

// TestSnapshotReconcile_ZeroMatchStaysPendingWithoutSnapshot preserves
// the pre-snapshot Pending wait and deadline behavior.
func TestSnapshotReconcile_ZeroMatchStaysPendingWithoutSnapshot(t *testing.T) {
	s := snapSessionObject(nil)
	tc := snapDefaultTC(snapSysNS)
	r, c := newSnapReconciler(t, interceptor.Funcs{}, s, tc)

	res, err := snapReconcile(t, r)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("zero-match session must requeue waiting for pods")
	}
	got := snapSessionGet(t, c)
	if got.Status.State != podtracev1alpha1.SessionStatePending {
		t.Fatalf("state = %q, want Pending", got.Status.State)
	}
	if got.Status.TargetSnapshot != nil {
		t.Fatal("no snapshot may be persisted before the first non-empty match")
	}
}

// TestSnapshotReconcile_SystemNamespaceFrozen: editing the resolved
// TracerConfig's systemNamespace after the snapshot cannot move the
// session's children to the new namespace.
func TestSnapshotReconcile_SystemNamespaceFrozen(t *testing.T) {
	s := snapSessionObject(nil)
	tc := snapDefaultTC("sys-a")
	podA := snapPod("web-a", "node-a", map[string]string{"app": "web"})
	r, c := newSnapReconciler(t, interceptor.Funcs{}, s, tc, podA)

	if _, err := snapReconcile(t, r); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	jobs := snapSessionJobs(t, c)
	if jobs["node-a"].Namespace != "sys-a" {
		t.Fatalf("job namespace = %q, want sys-a", jobs["node-a"].Namespace)
	}

	// The TracerConfig moves to another system namespace after freeze.
	var fresh podtracev1alpha1.TracerConfig
	if err := c.Get(context.Background(), client.ObjectKey{Name: DefaultTracerConfigName}, &fresh); err != nil {
		t.Fatal(err)
	}
	fresh.Spec.SystemNamespace = "sys-b"
	if err := c.Update(context.Background(), &fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := snapReconcile(t, r); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	var sysBJobs batchv1.JobList
	if err := c.List(context.Background(), &sysBJobs, client.InNamespace("sys-b")); err != nil {
		t.Fatal(err)
	}
	if len(sysBJobs.Items) != 0 {
		t.Fatalf("no session children may move to sys-b after the snapshot, got %d jobs", len(sysBJobs.Items))
	}
	var saB corev1.ServiceAccount
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "sys-b", Name: SessionServiceAccountName(snapUID)}, &saB)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("session SA must not be created in sys-b, err=%v", err)
	}
	got := snapSessionGet(t, c)
	for _, n := range got.Status.TargetSnapshot.Nodes {
		if n.SystemNamespace != "sys-a" {
			t.Fatalf("snapshot system namespace changed to %q", n.SystemNamespace)
		}
	}
}
