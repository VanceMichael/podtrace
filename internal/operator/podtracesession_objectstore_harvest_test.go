package operator

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
)

func harvestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := podtracev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("podtrace scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 scheme: %v", err)
	}
	return scheme
}

func harvestPod(name, node string, phase corev1.PodPhase, cs corev1.ContainerStatus) *corev1.Pod {
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
			Phase:                 phase,
			InitContainerStatuses: []corev1.ContainerStatus{cs},
		},
	}
}

func uploaderStatus(state corev1.ContainerState, restarts int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: reportUploaderContainerName, RestartCount: restarts, State: state}
}

func TestHarvestNodeReports_ListError(t *testing.T) {
	scheme := harvestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return apierrors.NewInternalError(errors.New("synthetic list failure"))
		},
	}).Build()

	if _, err := harvestNodeReportObservations(context.Background(), c, sessionWithObjectStore(""), []string{"podtrace-system"}); err == nil {
		t.Fatal("expected error when listing session pods fails")
	}
}

func TestHarvestNodeReports_SkipsNonUploaderAndTracksRestarts(t *testing.T) {
	scheme := harvestScheme(t)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "diag-pod",
			Namespace: "podtrace-system",
			Labels: map[string]string{
				LabelSessionName: "diag",
				LabelSessionNS:   "default",
				LabelNodeName:    "node-a",
			},
		},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{Name: "unrelated-init", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				uploaderStatus(corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, 3),
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()

	obs, err := harvestNodeReportObservations(context.Background(), c, sessionWithObjectStore(""), []string{"podtrace-system"})
	if err != nil {
		t.Fatalf("harvest: %v", err)
	}
	got, ok := obs["node-a"]
	if !ok {
		t.Fatalf("node-a missing from observations: %+v", obs)
	}
	if got.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3 (highest restart count of the uploader)", got.Attempts)
	}
	if got.Rank != observationPending {
		t.Errorf("Rank = %d, want pending while the uploader is running", got.Rank)
	}
	if got.Message != "" {
		t.Errorf("running uploader must not carry a message: %q", got.Message)
	}
}

// TestHarvestNodeReports_TransientVsFinalFailure pins the distinction
// the old first-terminated-wins logic missed: a non-zero exit while the
// Pod is still Running is a restartable attempt (Pending), the same
// exit once the Pod is no longer Running is the stable failure.
func TestHarvestNodeReports_TransientVsFinalFailure(t *testing.T) {
	scheme := harvestScheme(t)
	running := harvestPod("p-run", "node-a", corev1.PodRunning,
		uploaderStatus(terminatedFail("403 AccessDenied"), 2))
	finished := harvestPod("p-fin", "node-b", corev1.PodSucceeded,
		uploaderStatus(terminatedFail("403 AccessDenied"), 2))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(running, finished).Build()

	obs, err := harvestNodeReportObservations(context.Background(), c, sessionWithObjectStore(""), []string{"podtrace-system"})
	if err != nil {
		t.Fatalf("harvest: %v", err)
	}
	if obs["node-a"].Rank != observationTransientFailure {
		t.Errorf("non-zero exit in a Running Pod must be transient, got rank %d", obs["node-a"].Rank)
	}
	if obs["node-b"].Rank != observationFinalFailure {
		t.Errorf("non-zero exit after Pod finished must be final, got rank %d", obs["node-b"].Rank)
	}
}

// TestHarvestNodeReports_OrderIndependent proves the folded
// observation is identical no matter how the API orders the Pod list,
// including multiple Pods per node (replacements) with mixed signals.
func TestHarvestNodeReports_OrderIndependent(t *testing.T) {
	scheme := harvestScheme(t)
	mkPods := func() []client.Object {
		// node-a: an old replacement Pod failed transiently while
		// Running, its current Pod carries the final success.
		oldA := harvestPod("p-a-old", "node-a", corev1.PodRunning,
			uploaderStatus(terminatedFail("no such host"), 1))
		newA := harvestPod("p-a-new", "node-a", corev1.PodSucceeded,
			uploaderStatus(terminatedOK("s3://b/reports/a.txt"), 2))
		// node-b: final failure.
		b := harvestPod("p-b", "node-b", corev1.PodFailed,
			uploaderStatus(terminatedFail("404 NoSuchBucket"), 0))
		return []client.Object{oldA, newA, b}
	}

	harvest := func(objs []client.Object) map[string]nodeUploadObservation {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		obs, err := harvestNodeReportObservations(context.Background(), c, sessionWithObjectStore(""), []string{"podtrace-system"})
		if err != nil {
			t.Fatalf("harvest: %v", err)
		}
		return obs
	}

	first := harvest(mkPods())
	reversed := mkPods()
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	second := harvest(reversed)

	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("expected two nodes regardless of order: %+v vs %+v", first, second)
	}
	for node := range first {
		a, b := first[node], second[node]
		if a.Rank != b.Rank || a.Attempts != b.Attempts || a.Message != b.Message {
			t.Errorf("node %s observation depends on Pod order: %+v vs %+v", node, a, b)
		}
	}
	if first["node-a"].Rank != observationSucceeded || first["node-a"].Message != "s3://b/reports/a.txt" || first["node-a"].Attempts != 2 {
		t.Errorf("node-a must fold to success across replacement Pods: %+v", first["node-a"])
	}
	if first["node-b"].Rank != observationFinalFailure {
		t.Errorf("node-b must fold to final failure: %+v", first["node-b"])
	}
}

// TestHarvestNodeReports_WaitingUploaderOnFinishedPodIsFinalFailure
// covers an uploader that never comes up (e.g. CrashLoopBackOff at pod
// shutdown): a finished Pod must turn this into a stable failure
// instead of stranding the session on Unknown forever.
func TestHarvestNodeReports_WaitingUploaderOnFinishedPodIsFinalFailure(t *testing.T) {
	scheme := harvestScheme(t)
	pod := harvestPod("p", "node-a", corev1.PodFailed, corev1.ContainerStatus{
		Name:  reportUploaderContainerName,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off"}},
	})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()

	obs, err := harvestNodeReportObservations(context.Background(), c, sessionWithObjectStore(""), []string{"podtrace-system"})
	if err != nil {
		t.Fatalf("harvest: %v", err)
	}
	if obs["node-a"].Rank != observationFinalFailure {
		t.Fatalf("rank = %d, want final failure, message=%q", obs["node-a"].Rank, obs["node-a"].Message)
	}
	if !strings.Contains(obs["node-a"].Message, "CrashLoopBackOff") {
		t.Errorf("waiting reason not carried into message: %q", obs["node-a"].Message)
	}
}

func TestHarvestNodeReports_MultiNamespaceUnion(t *testing.T) {
	scheme := harvestScheme(t)
	podA := harvestPod("p-a", "node-a", corev1.PodSucceeded,
		uploaderStatus(terminatedOK("s3://b/reports/a.txt"), 0))
	podA.Namespace = "podtrace-system"
	podB := harvestPod("p-b", "node-b", corev1.PodSucceeded,
		uploaderStatus(terminatedOK("s3://b/reports/b.txt"), 0))
	podB.Namespace = "podtrace-other"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(podA, podB).Build()

	obs, err := harvestNodeReportObservations(context.Background(), c, sessionWithObjectStore(""),
		[]string{"podtrace-system", "", "podtrace-other", "podtrace-system"})
	if err != nil {
		t.Fatalf("harvest: %v", err)
	}
	if obs["node-a"].Message != "s3://b/reports/a.txt" || obs["node-b"].Message != "s3://b/reports/b.txt" {
		t.Errorf("multi-namespace/dedup harvest lost nodes: %+v", obs)
	}
}
