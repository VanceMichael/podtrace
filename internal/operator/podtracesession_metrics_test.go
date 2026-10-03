package operator

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
)

func resetReportMetrics() {
	reportUploadAttempts.Reset()
	reportUploadDuration.Reset()
}

func metricsSession() *podtracev1alpha1.PodTraceSession {
	s := &podtracev1alpha1.PodTraceSession{}
	s.Name, s.Namespace = "s", "ns"
	s.Spec.ReportRef = &podtracev1alpha1.ReportReference{
		ObjectStore: &podtracev1alpha1.ObjectStoreReference{URI: "s3://b/k/"},
	}
	return s
}

func TestObserveNodeReportMetrics_RecordsPerNodeOutcomes(t *testing.T) {
	resetReportMetrics()
	defer resetReportMetrics()

	s := metricsSession()
	observeNodeReportMetrics(s, []nodeReportTransition{
		{Node: "node-a", Succeeded: true, Message: "s3://b/k/a.txt"},
		{Node: "node-b", Succeeded: false, Message: "operation error S3: PutObject, 404 NoSuchBucket"},
	})

	if got := counterTotal(t, "s3", "success", ""); got != 1 {
		t.Errorf("success counter = %v, want 1 (one node)", got)
	}
	if got := counterTotal(t, "s3", "failure", "BucketNotFound"); got != 1 {
		t.Errorf("failure counter = %v, want 1 (one node, classified)", got)
	}
}

func TestObserveNodeReportMetrics_DropsEmptyAndNonObjectStore(t *testing.T) {
	resetReportMetrics()
	defer resetReportMetrics()

	// No transitions: repeat reconciles with no node-level change must
	// contribute nothing.
	observeNodeReportMetrics(metricsSession(), nil)

	cmSink := &podtracev1alpha1.PodTraceSession{}
	cmSink.Name, cmSink.Namespace = "cm", "ns"
	observeNodeReportMetrics(cmSink, []nodeReportTransition{{Succeeded: true}})

	if got := counterTotal(t, "s3", "success", ""); got != 0 {
		t.Errorf("no-transition reconcile must not contribute, got %v", got)
	}
}

// TestApplyReportUploadStatus_TransitionsOnlyOnRealNodeChange is the
// idempotency guarantee behind metric accounting: a repeated reconcile
// (or an operator restart) over the same persisted per-node state must
// produce zero transitions, so the caller never double-counts.
func TestApplyReportUploadStatus_TransitionsOnlyOnRealNodeChange(t *testing.T) {
	resetReportMetrics()
	defer resetReportMetrics()

	s := metricsSession()
	s.Status.Jobs = []podtracev1alpha1.SessionJobRef{
		{Node: "node-a", Name: "j-a", Completed: true},
		{Node: "node-b", Name: "j-b", Completed: true},
	}
	obs := map[string]nodeUploadObservation{
		"node-a": {Node: "node-a", Rank: observationSucceeded, Message: "s3://b/k/a.txt"},
		"node-b": {Node: "node-b", Rank: observationFinalFailure, Message: "403 AccessDenied"},
	}

	first := applyReportUploadStatus(s, obs)
	if len(first) != 2 {
		t.Fatalf("first reconcile transitions = %d, want 2 (one per node)", len(first))
	}
	observeNodeReportMetrics(s, first)

	// Simulate another reconcile over the already-persisted status.
	second := applyReportUploadStatus(s, obs)
	if len(second) != 0 {
		t.Fatalf("repeat reconcile transitions = %d, want 0: %+v", len(second), second)
	}
	observeNodeReportMetrics(s, second)

	// Simulate an operator restart: status reloaded from Kubernetes
	// (terminal nodeReports persisted), Pods re-harvested identically.
	reloaded := metricsSession()
	reloaded.Status.Jobs = s.Status.Jobs
	reloaded.Status.NodeReports = s.Status.NodeReports
	afterRestart := applyReportUploadStatus(reloaded, obs)
	if len(afterRestart) != 0 {
		t.Fatalf("post-restart reconcile transitions = %d, want 0", len(afterRestart))
	}
	observeNodeReportMetrics(reloaded, afterRestart)

	if got := counterTotal(t, "s3", "success", ""); got != 1 {
		t.Errorf("success counter = %v, want exactly 1 across repeats and restart", got)
	}
	if got := counterTotal(t, "s3", "failure", "AccessDenied"); got != 1 {
		t.Errorf("failure counter = %v, want exactly 1 across repeats and restart", got)
	}
}

// TestApplyReportUploadMetrics_LateNodeDoesNotRetriggerSettledNodes
// verifies that a node concluding later only records its own outcome;
// already-terminal nodes stay silent.
func TestApplyReportUploadMetrics_LateNodeDoesNotRetriggerSettledNodes(t *testing.T) {
	resetReportMetrics()
	defer resetReportMetrics()

	s := metricsSession()
	s.Status.Jobs = []podtracev1alpha1.SessionJobRef{
		{Node: "node-a", Name: "j-a", Completed: true},
		{Node: "node-b", Name: "j-b", Completed: true},
	}

	first := applyReportUploadStatus(s, map[string]nodeUploadObservation{
		"node-a": {Node: "node-a", Rank: observationSucceeded, Message: "s3://b/k/a.txt"},
		"node-b": {Node: "node-b", Rank: observationPending, Attempts: 2},
	})
	if len(first) != 1 || !first[0].Succeeded {
		t.Fatalf("want one success transition, got %+v", first)
	}
	observeNodeReportMetrics(s, first)

	second := applyReportUploadStatus(s, map[string]nodeUploadObservation{
		"node-a": {Node: "node-a", Rank: observationSucceeded, Message: "s3://b/k/a.txt"},
		"node-b": {Node: "node-b", Rank: observationFinalFailure, Message: "dial tcp: no such host"},
	})
	if len(second) != 1 || second[0].Succeeded {
		t.Fatalf("want one failure transition for node-b, got %+v", second)
	}
	observeNodeReportMetrics(s, second)

	if got := counterTotal(t, "s3", "success", ""); got != 1 {
		t.Errorf("success counter = %v, want 1", got)
	}
	if got := counterTotal(t, "s3", "failure", "NetworkTimeout"); got != 1 {
		t.Errorf("failure counter = %v, want 1", got)
	}
}

func TestObserveNodeReportMetrics_DurationSampledPerNode(t *testing.T) {
	resetReportMetrics()
	defer resetReportMetrics()

	s := metricsSession()
	start := metav1.NewTime(time.Now().Add(-2 * time.Second))
	s.Status.StartTime = &start
	observeNodeReportMetrics(s, []nodeReportTransition{{Node: "node-a", Succeeded: true, Message: "s3://b/k/a.txt"}})
	if got := durationSampleCount("s3", "success"); got != 1 {
		t.Errorf("duration sample count = %d, want 1 per terminal node", got)
	}
}

func durationSampleCount(backend, result string) uint64 {
	obs, err := reportUploadDuration.GetMetricWithLabelValues(backend, result)
	if err != nil {
		return 0
	}
	m, ok := obs.(prometheus.Metric)
	if !ok {
		return 0
	}
	var pb dto.Metric
	if err := m.Write(&pb); err != nil {
		return 0
	}
	return pb.GetHistogram().GetSampleCount()
}

func TestBackendFromURI(t *testing.T) {
	cases := map[string]string{
		"s3://b/k":             "s3",
		"gs://b/k":             "gs",
		"azblob://acct/cont/k": "azblob",
		"":                     "unknown",
		"http://b/k":           "unknown",
	}
	for uri, want := range cases {
		t.Run(strings.ReplaceAll(uri, "/", "_"), func(t *testing.T) {
			if got := backendFromURI(uri); got != want {
				t.Errorf("backendFromURI(%q) = %q, want %q", uri, got, want)
			}
		})
	}
}

func counterTotal(t *testing.T, backend, result, reason string) float64 {
	t.Helper()
	m, err := reportUploadAttempts.GetMetricWithLabelValues(backend, result, reason)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	var pb dto.Metric
	if err := m.Write(&pb); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return pb.GetCounter().GetValue()
}
