package operator

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
	"github.com/gma1k/podtrace/internal/reportsink/objectstore"
)

// Session-objectStore Prometheus surface.
var (
	reportUploadAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "podtrace_operator",
		Name:      "session_report_upload_attempts_total",
		Help: "Terminal per-node outcomes of the objectStore uploader sidecar. " +
			"backend ∈ {s3,gs,azblob}, result ∈ {success,failure}, " +
			"reason ∈ ReportFailureReason enum (empty when result=success). " +
			"Exactly one sample is recorded per node when that node's upload " +
			"state first becomes Succeeded or Failed; repeated reconciles and " +
			"operator restarts do not re-count it. " +
		"Compare rate(result=failure) vs rate(result=success) for an SLO; " +
			"sum by (reason) to find the dominant failure mode.",
	}, []string{"backend", "result", "reason"})

	reportUploadDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "podtrace_operator",
		Name:      "session_report_upload_duration_seconds",
		Help: "Time from session start to a per-node terminal objectStore " +
			"sidecar outcome. Buckets sized for typical small reports (under " +
			"100KB) with headroom for occasional slow tails. Use " +
			"histogram_quantile(0.95, …) to track the long tail.",
		Buckets: []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{"backend", "result"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(reportUploadAttempts, reportUploadDuration)
}

// observeNodeReportMetrics records one Prometheus observation per
// node-level terminal transition. Callers pass only the transitions a
// reconcile actually produced (derived by comparing the prior,
// already-persisted per-node status with the merged status), and only
// after the new status persists successfully. That makes recording
// idempotent across repeated reconciles AND operator restarts with no
// in-memory dedup state: a re-run after a failed/conflicted status
// update recomputes transitions against the unmodified persisted state
// and emits nothing twice.
func observeNodeReportMetrics(s *podtracev1alpha1.PodTraceSession, transitions []nodeReportTransition) {
	if s == nil || s.Spec.ReportRef == nil || s.Spec.ReportRef.ObjectStore == nil {
		return
	}
	backend := backendFromURI(s.Spec.ReportRef.ObjectStore.URI)
	for i := range transitions {
		t := &transitions[i]
		result := "success"
		reason := ""
		if !t.Succeeded {
			result = "failure"
			reason = string(classifyUploadFailure(t.Message))
		}
		reportUploadAttempts.WithLabelValues(backend, result, reason).Inc()
		if duration := uploadDurationSeconds(s); duration > 0 {
			reportUploadDuration.WithLabelValues(backend, result).Observe(duration)
		}
	}
}

// backendFromURI extracts the scheme prefix for use as a metric
// label. Falls back to "unknown" for malformed URIs so the label
// cardinality stays bounded to the supported scheme set + 1.
func backendFromURI(uri string) string {
	switch {
	case strings.HasPrefix(uri, objectstore.SchemeS3+"://"):
		return objectstore.SchemeS3
	case strings.HasPrefix(uri, objectstore.SchemeGCS+"://"):
		return objectstore.SchemeGCS
	case strings.HasPrefix(uri, objectstore.SchemeAzure+"://"):
		return objectstore.SchemeAzure
	default:
		return "unknown"
	}
}

// uploadDurationSeconds returns the wall-clock seconds between
// status.startTime and now for the duration histogram.
func uploadDurationSeconds(s *podtracev1alpha1.PodTraceSession) float64 {
	if s.Status.StartTime == nil {
		return 0
	}
	return time.Since(s.Status.StartTime.Time).Seconds()
}
