package operator

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
)

// terminalObjectStoreSession builds a Completed session whose TTL has
// already elapsed, fanned out to two nodes.
func terminalObjectStoreSession(status podtracev1alpha1.PodTraceSessionStatus) *podtracev1alpha1.PodTraceSession {
	zero := int32(0)
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	status.State = podtracev1alpha1.SessionStateCompleted
	status.CompletionTime = &past
	return &podtracev1alpha1.PodTraceSession{
		ObjectMeta: metav1.ObjectMeta{Name: "diag", Namespace: "default", UID: "u-term"},
		Spec: podtracev1alpha1.PodTraceSessionSpec{
			Duration:              metav1.Duration{Duration: time.Minute},
			TTLSecondsAfterFinished: &zero,
			ReportRef: &podtracev1alpha1.ReportReference{
				ObjectStore: &podtracev1alpha1.ObjectStoreReference{URI: "s3://b/reports/"},
			},
		},
		Status: status,
	}
}

func twoNodeJobRefs() []podtracev1alpha1.SessionJobRef {
	return []podtracev1alpha1.SessionJobRef{
		{Node: "node-a", Name: "j-a", Completed: true},
		{Node: "node-b", Name: "j-b", Completed: true},
	}
}

func uploaderPod(name, node string, phase corev1.PodPhase, state corev1.ContainerState, restarts int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "podtrace-system",
			Labels: map[string]string{
				LabelSessionName: "diag",
				LabelSessionNS:   "default",
				LabelNodeName:    node,
			},
		},
		Spec: corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase: phase,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name:         reportUploaderContainerName,
				RestartCount: restarts,
				State:        state,
			}},
		},
	}
}

func terminatedOK(uri string) corev1.ContainerState {
	return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: uri}}
}

func terminatedFail(msg string) corev1.ContainerState {
	return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: msg}}
}

// TestTerminalSession_TTLHeldWhileAnyUploaderUnresolved: a finished
// session with one node uploaded and another uploader still running
// must NOT be TTL-deleted; the successful location is retained and the
// aggregate condition stays Unknown.
func TestTerminalSession_TTLHeldWhileAnyUploaderUnresolved(t *testing.T) {
	resetReportMetrics()
	defer resetReportMetrics()

	s := terminalObjectStoreSession(podtracev1alpha1.PodTraceSessionStatus{Jobs: twoNodeJobRefs()})
	podA := uploaderPod("p-a", "node-a", corev1.PodSucceeded, terminatedOK("s3://b/reports/a.txt"), 0)
	podB := uploaderPod("p-b", "node-b", corev1.PodSucceeded,
		corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}, 0)

	c := fake.NewClientBuilder().WithScheme(newOperatorScheme(t)).
		WithObjects(s, podA, podB).
		WithStatusSubresource(s).
		Build()
	r := &PodTraceSessionReconciler{Client: c, Scheme: newOperatorScheme(t), SystemNamespace: "podtrace-system"}

	res, err := r.reconcileTerminalSession(context.Background(), s)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != uploadReportPollInterval {
		t.Errorf("requeue = %v, want %v while awaiting node-b", res.RequeueAfter, uploadReportPollInterval)
	}

	var got podtracev1alpha1.PodTraceSession
	if err := c.Get(context.Background(), types.NamespacedName{Name: "diag", Namespace: "default"}, &got); err != nil {
		t.Fatalf("session must still exist: %v", err)
	}
	cond := findCondition(got.Status.Conditions, ConditionReportUploaded)
	if cond == nil || cond.Status != metav1.ConditionUnknown {
		t.Fatalf("ReportUploaded = %v, want Unknown while node-b uploads", cond)
	}
	byNode := map[string]podtracev1alpha1.SessionNodeReport{}
	for i := range got.Status.NodeReports {
		byNode[got.Status.NodeReports[i].Node] = got.Status.NodeReports[i]
	}
	if byNode["node-a"].State != podtracev1alpha1.NodeReportSucceeded ||
		byNode["node-a"].Location != "s3://b/reports/a.txt" {
		t.Errorf("node-a success lost: %+v", byNode["node-a"])
	}
	if byNode["node-b"].State != podtracev1alpha1.NodeReportPending {
		t.Errorf("node-b = %v, want Pending", byNode["node-b"].State)
	}
	if got := counterTotal(t, "s3", "success", ""); got != 1 {
		t.Errorf("success counter = %v, want 1 (node-a only)", got)
	}
}

// TestTerminalSession_ConvergesAfterLateNodeThenDeletes continues the
// scenario: once node-b's uploader terminates successfully the
// condition converges True and the elapsed TTL may delete the session.
func TestTerminalSession_ConvergesAfterLateNodeThenDeletes(t *testing.T) {
	resetReportMetrics()
	defer resetReportMetrics()

	s := terminalObjectStoreSession(podtracev1alpha1.PodTraceSessionStatus{Jobs: twoNodeJobRefs()})
	podA := uploaderPod("p-a", "node-a", corev1.PodSucceeded, terminatedOK("s3://b/reports/a.txt"), 0)
	podB := uploaderPod("p-b", "node-b", corev1.PodSucceeded,
		corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}, 0)

	scheme := newOperatorScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(s, podA, podB).
		WithStatusSubresource(s).
		Build()
	r := &PodTraceSessionReconciler{Client: c, Scheme: scheme, SystemNamespace: "podtrace-system"}

	if _, err := r.reconcileTerminalSession(context.Background(), s); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// node-b's upload finishes.
	updated := podB.DeepCopy()
	updated.Status.InitContainerStatuses[0].State = terminatedOK("s3://b/reports/b.txt")
	if err := c.Status().Update(context.Background(), updated); err != nil {
		t.Fatalf("update pod-b status: %v", err)
	}
	var reloaded podtracev1alpha1.PodTraceSession
	if err := c.Get(context.Background(), types.NamespacedName{Name: "diag", Namespace: "default"}, &reloaded); err != nil {
		t.Fatalf("reload session: %v", err)
	}
	if _, err := r.reconcileTerminalSession(context.Background(), &reloaded); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	cond := findCondition(reloaded.Status.Conditions, ConditionReportUploaded)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("ReportUploaded = %v, want True after both nodes succeed", cond)
	}
	err := c.Get(context.Background(), types.NamespacedName{Name: "diag", Namespace: "default"}, &podtracev1alpha1.PodTraceSession{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("converged session past TTL must be deleted, got %v", err)
	}
	if got := counterTotal(t, "s3", "success", ""); got != 2 {
		t.Errorf("success counter = %v, want exactly 2 (one per node)", got)
	}
}

// TestTerminalSession_PartialFailureKeepsBothSides: with one success
// and one stable failure the condition converges False while both the
// successful object location and the failed node's stable reason stay
// in status.nodeReports. The session is then allowed to TTL-delete.
func TestTerminalSession_PartialFailureKeepsBothSides(t *testing.T) {
	resetReportMetrics()
	defer resetReportMetrics()

	s := terminalObjectStoreSession(podtracev1alpha1.PodTraceSessionStatus{Jobs: twoNodeJobRefs()})
	podA := uploaderPod("p-a", "node-a", corev1.PodSucceeded, terminatedOK("s3://b/reports/a.txt"), 0)
	podB := uploaderPod("p-b", "node-b", corev1.PodFailed,
		terminatedFail("operation error S3: PutObject, 403 AccessDenied"), 1)

	scheme := newOperatorScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(s, podA, podB).
		WithStatusSubresource(s).
		Build()
	r := &PodTraceSessionReconciler{Client: c, Scheme: scheme, SystemNamespace: "podtrace-system"}

	if _, err := r.reconcileTerminalSession(context.Background(), s); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	cond := findCondition(s.Status.Conditions, ConditionReportUploaded)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ObjectStoreUploadFailed" {
		t.Fatalf("ReportUploaded = %+v, want False/ObjectStoreUploadFailed", cond)
	}
	byNode := map[string]podtracev1alpha1.SessionNodeReport{}
	for i := range s.Status.NodeReports {
		byNode[s.Status.NodeReports[i].Node] = s.Status.NodeReports[i]
	}
	if byNode["node-a"].State != podtracev1alpha1.NodeReportSucceeded ||
		byNode["node-a"].Location != "s3://b/reports/a.txt" {
		t.Errorf("successful node-a location must be retained on partial failure: %+v", byNode["node-a"])
	}
	if byNode["node-b"].State != podtracev1alpha1.NodeReportFailed ||
		byNode["node-b"].FailureReason != podtracev1alpha1.ReportFailureReasonAccessDenied {
		t.Errorf("node-b failure reason not retained: %+v", byNode["node-b"])
	}
	if got := counterTotal(t, "s3", "success", ""); got != 1 {
		t.Errorf("success counter = %v, want 1", got)
	}
	if got := counterTotal(t, "s3", "failure", "AccessDenied"); got != 1 {
		t.Errorf("failure counter = %v, want 1", got)
	}
	err := c.Get(context.Background(), types.NamespacedName{Name: "diag", Namespace: "default"}, &podtracev1alpha1.PodTraceSession{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("converged (False) session past TTL must be deleted, got %v", err)
	}
}

// TestTerminalSession_RestartRecoversFromPersistedState: after an
// operator restart with uploader Pods already TTL-garbage-collected,
// the persisted nodeReports alone must reproduce the same True
// conclusion, emit no duplicate metrics, and allow TTL deletion.
func TestTerminalSession_RestartRecoversFromPersistedState(t *testing.T) {
	resetReportMetrics()
	defer resetReportMetrics()

	status := podtracev1alpha1.PodTraceSessionStatus{
		Jobs: twoNodeJobRefs(),
		NodeReports: []podtracev1alpha1.SessionNodeReport{
			{Node: "node-a", State: podtracev1alpha1.NodeReportSucceeded, Location: "s3://b/reports/a.txt", Message: "s3://b/reports/a.txt"},
			{Node: "node-b", State: podtracev1alpha1.NodeReportSucceeded, Location: "s3://b/reports/b.txt", Message: "s3://b/reports/b.txt"},
		},
	}
	s := terminalObjectStoreSession(status)
	upserted := upsertCondition(nil, metav1.Condition{
		Type: ConditionReportUploaded, Status: metav1.ConditionTrue, Reason: "ObjectStoreUploadSucceeded",
	})
	s.Status.Conditions = upserted

	scheme := newOperatorScheme(t)
	// No Pod objects: the only evidence is the persisted status.
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(s).
		WithStatusSubresource(s).
		Build()
	r := &PodTraceSessionReconciler{Client: c, Scheme: scheme, SystemNamespace: "podtrace-system"}

	if res, err := r.reconcileTerminalSession(context.Background(), s); err != nil || res != (ctrl.Result{}) {
		t.Fatalf("reconcile = (%+v, %v), want empty result converging straight to TTL delete", res, err)
	}
	err := c.Get(context.Background(), types.NamespacedName{Name: "diag", Namespace: "default"}, &podtracev1alpha1.PodTraceSession{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("session recovered as converged must be deleted, got %v", err)
	}
	if got := counterTotal(t, "s3", "success", ""); got != 0 {
		t.Errorf("restart recovery must not re-count historical outcomes, got %v", got)
	}
}

// TestTerminalSession_NonObjectStoreStillTTLDeletes guards the
// backwards-compatible path: sessions without an objectStore keep the
// old TTL-only behavior even in Completed/Failed.
func TestTerminalSession_NonObjectStoreStillTTLDeletes(t *testing.T) {
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	s := &podtracev1alpha1.PodTraceSession{
		ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "default", UID: "u-plain"},
		Spec:       podtracev1alpha1.PodTraceSessionSpec{},
		Status:     podtracev1alpha1.PodTraceSessionStatus{State: podtracev1alpha1.SessionStateCompleted, CompletionTime: &past},
	}
	scheme := newOperatorScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s).WithStatusSubresource(s).Build()
	r := &PodTraceSessionReconciler{Client: c, Scheme: scheme, SystemNamespace: "podtrace-system"}
	if _, err := r.reconcileTerminalSession(context.Background(), s); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "plain", Namespace: "default"}, &podtracev1alpha1.PodTraceSession{}); !apierrors.IsNotFound(err) {
		t.Fatalf("non-objectStore session past TTL must delete unchanged, got %v", err)
	}
}
