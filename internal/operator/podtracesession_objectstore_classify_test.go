package operator

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
)

// TestBestTerminationMessage_PrefersInformativeOverKubeletShutdown is
// the regression test for a real bug found during manual e2e: when a
// native sidecar's parent pod terminates while the sidecar is still
// running, the kubelet overwrites the sidecar's /dev/termination-log
// with a generic "container could not be located when the pod was
// terminated" message.
func TestBestTerminationMessage_PrefersInformativeOverKubeletShutdown(t *testing.T) {
	cases := []struct {
		name     string
		current  string
		previous string
		want     string
	}{
		{
			name:     "current_informative_preferred",
			current:  "s3://b/k/r.txt",
			previous: "stale earlier message",
			want:     "s3://b/k/r.txt",
		},
		{
			name:     "kubelet_shutdown_falls_back",
			current:  "The container could not be located when the pod was terminated",
			previous: "Error: 403 InvalidAccessKeyId",
			want:     "Error: 403 InvalidAccessKeyId",
		},
		{
			name:     "empty_current_falls_back",
			current:  "",
			previous: "Error: bucket not found",
			want:     "Error: bucket not found",
		},
		{
			name:     "both_empty",
			current:  "",
			previous: "",
			want:     "",
		},
		{
			name:     "kubelet_shutdown_with_no_previous",
			current:  "The container could not be located when the pod was terminated",
			previous: "",
			want:     "The container could not be located when the pod was terminated",
		},
		{
			name:     "node_was_lost_falls_back",
			current:  "The node was lost",
			previous: "Error: connection refused",
			want:     "Error: connection refused",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs := corev1.ContainerStatus{
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Message: tc.current},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Message: tc.previous},
				},
			}
			if got := bestTerminationMessage(cs); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestBestTerminationMessage_NilStatesAreSafe documents the
// nil-tolerance contract of the helper: a ContainerStatus where the
// kubelet has not populated either terminated state must not panic.
func TestBestTerminationMessage_NilStatesAreSafe(t *testing.T) {
	if got := bestTerminationMessage(corev1.ContainerStatus{}); got != "" {
		t.Errorf("empty ContainerStatus: got %q, want empty", got)
	}
}

func TestClassifyUploadFailure(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want podtracev1alpha1.ReportFailureReason
	}{
		{name: "s3_no_such_bucket", msg: "operation error S3: PutObject, https response error StatusCode: 404, NoSuchBucket: The specified bucket does not exist", want: podtracev1alpha1.ReportFailureReasonBucketNotFound},
		{name: "gcs_bucket_doesnt_exist", msg: "googleapi: Error 404: storage: bucket doesn't exist", want: podtracev1alpha1.ReportFailureReasonBucketNotFound},
		{name: "azure_container_not_found", msg: "RESPONSE 404 ContainerNotFound", want: podtracev1alpha1.ReportFailureReasonBucketNotFound},

		{name: "s3_access_denied", msg: "operation error S3: PutObject, https response error StatusCode: 403, AccessDenied: Access Denied", want: podtracev1alpha1.ReportFailureReasonAccessDenied},
		{name: "s3_signature", msg: "SignatureDoesNotMatch: The request signature we calculated does not match the signature you provided", want: podtracev1alpha1.ReportFailureReasonAccessDenied},
		{name: "azure_authorization_failed", msg: "RESPONSE 403 AuthorizationFailed", want: podtracev1alpha1.ReportFailureReasonAccessDenied},

		{name: "aws_no_credentials", msg: "failed to retrieve credentials, no credentials found", want: podtracev1alpha1.ReportFailureReasonCredentialMissing},
		{name: "gcs_default_credentials", msg: "google: could not find default credentials", want: podtracev1alpha1.ReportFailureReasonCredentialMissing},

		{name: "ctx_deadline", msg: "context deadline exceeded", want: podtracev1alpha1.ReportFailureReasonNetworkTimeout},
		{name: "tls_handshake", msg: "net/http: TLS handshake timeout", want: podtracev1alpha1.ReportFailureReasonNetworkTimeout},
		{name: "dns_no_such_host", msg: "dial tcp: lookup s3.bogus.example: no such host", want: podtracev1alpha1.ReportFailureReasonNetworkTimeout},
		{name: "connection_refused", msg: "dial tcp 127.0.0.1:9000: connect: connection refused", want: podtracev1alpha1.ReportFailureReasonNetworkTimeout},

		{name: "unsupported_scheme", msg: `unsupported URI scheme "ftp" (want s3, gs, or azblob)`, want: podtracev1alpha1.ReportFailureReasonInvalidURI},
		{name: "no_host", msg: `URI "s3://" must include scheme and host`, want: podtracev1alpha1.ReportFailureReasonInvalidURI},
		{name: "empty_key", msg: "s3: resolved object key is empty (URI must include a key or prefix)", want: podtracev1alpha1.ReportFailureReasonInvalidURI},

		{name: "empty", msg: "", want: podtracev1alpha1.ReportFailureReasonUnknown},
		{name: "novel_sdk_wording", msg: "ECONNRESET: kernel said no", want: podtracev1alpha1.ReportFailureReasonUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyUploadFailure(tc.msg)
			if got != tc.want {
				t.Errorf("classifyUploadFailure(%q) = %q, want %q", tc.msg, got, tc.want)
			}
		})
	}
}

func TestApplyReportUploadStatus_AttemptsAndReason(t *testing.T) {
	session := func(prev podtracev1alpha1.ReportFailureReason) *podtracev1alpha1.PodTraceSession {
		s := &podtracev1alpha1.PodTraceSession{
			Spec: podtracev1alpha1.PodTraceSessionSpec{
				ReportRef: &podtracev1alpha1.ReportReference{
					ObjectStore: &podtracev1alpha1.ObjectStoreReference{URI: "s3://b/k"},
				},
			},
			Status: podtracev1alpha1.PodTraceSessionStatus{
				Jobs:                []podtracev1alpha1.SessionJobRef{{Node: "node-a", Name: "j-a", Completed: true}},
				ReportFailureReason: prev,
			},
		}
		return s
	}
	pending := map[string]nodeUploadObservation{"node-a": {Node: "node-a", Rank: observationPending, Attempts: 2}}
	success := map[string]nodeUploadObservation{"node-a": {
		Node: "node-a", Rank: observationSucceeded, Attempts: 1, Message: "s3://b/k/r.txt",
	}}
	failure := map[string]nodeUploadObservation{"node-a": {
		Node: "node-a", Rank: observationFinalFailure, Attempts: 4,
		Message: "operation error S3: PutObject, 403 AccessDenied",
	}}

	t.Run("pending clears prior reason", func(t *testing.T) {
		s := session(podtracev1alpha1.ReportFailureReasonNetworkTimeout)
		applyReportUploadStatus(s, pending)
		if s.Status.ReportFailureReason != "" {
			t.Errorf("pending must clear reason, got %q", s.Status.ReportFailureReason)
		}
		if s.Status.ReportAttempts != 2 {
			t.Errorf("attempts = %d, want 2", s.Status.ReportAttempts)
		}
	})

	t.Run("success clears reason and stamps location", func(t *testing.T) {
		s := session(podtracev1alpha1.ReportFailureReasonAccessDenied)
		applyReportUploadStatus(s, success)
		if s.Status.ReportFailureReason != "" {
			t.Errorf("success must clear reason, got %q", s.Status.ReportFailureReason)
		}
		if s.Status.ReportLocation != "s3://b/k/r.txt" {
			t.Errorf("location not stamped, got %q", s.Status.ReportLocation)
		}
	})

	t.Run("failure sets reason from classifier", func(t *testing.T) {
		s := session("")
		applyReportUploadStatus(s, failure)
		if s.Status.ReportFailureReason != podtracev1alpha1.ReportFailureReasonAccessDenied {
			t.Errorf("reason = %q, want AccessDenied", s.Status.ReportFailureReason)
		}
		if s.Status.ReportAttempts != 4 {
			t.Errorf("attempts = %d, want 4", s.Status.ReportAttempts)
		}
	})

	t.Run("non-objectstore session is no-op", func(t *testing.T) {
		s := &podtracev1alpha1.PodTraceSession{} // no ReportRef.ObjectStore
		applyReportUploadStatus(s, failure)
		if s.Status.ReportAttempts != 0 {
			t.Errorf("non-objectstore session must not touch status, attempts=%d", s.Status.ReportAttempts)
		}
	})
}

// TestApplyReportUploadStatus_StickyTerminalAndPartialFailure covers
// the multi-node state machine: early successes survive later Pod
// observations, the condition cannot converge on the first terminated
// node, and a partial failure keeps both locations and reasons.
func TestApplyReportUploadStatus_StickyTerminalAndPartialFailure(t *testing.T) {
	newSession := func() *podtracev1alpha1.PodTraceSession {
		return &podtracev1alpha1.PodTraceSession{
			Spec: podtracev1alpha1.PodTraceSessionSpec{
				ReportRef: &podtracev1alpha1.ReportReference{
					ObjectStore: &podtracev1alpha1.ObjectStoreReference{URI: "s3://b/k"},
				},
			},
			Status: podtracev1alpha1.PodTraceSessionStatus{Jobs: []podtracev1alpha1.SessionJobRef{
				{Node: "node-a", Name: "j-a", Completed: true},
				{Node: "node-b", Name: "j-b", Completed: true},
			}},
		}
	}
	condStatus := func(s *podtracev1alpha1.PodTraceSession) metav1.ConditionStatus {
		for i := range s.Status.Conditions {
			if s.Status.Conditions[i].Type == ConditionReportUploaded {
				return s.Status.Conditions[i].Status
			}
		}
		return ""
	}

	t.Run("first node succeeded does not converge the session", func(t *testing.T) {
		s := newSession()
		tr := applyReportUploadStatus(s, map[string]nodeUploadObservation{
			"node-a": {Node: "node-a", Rank: observationSucceeded, Message: "s3://b/k/a.txt"},
			"node-b": {Node: "node-b", Rank: observationPending, Attempts: 1},
		})
		if len(tr) != 1 || !tr[0].Succeeded {
			t.Fatalf("transitions = %+v, want one success for node-a", tr)
		}
		if condStatus(s) != metav1.ConditionUnknown {
			t.Errorf("condition = %s, want Unknown while node-b uploads", condStatus(s))
		}
	})

	t.Run("success retained while sibling fails; converges False", func(t *testing.T) {
		s := newSession()
		applyReportUploadStatus(s, map[string]nodeUploadObservation{
			"node-a": {Node: "node-a", Rank: observationSucceeded, Message: "s3://b/k/a.txt"},
			"node-b": {Node: "node-b", Rank: observationPending, Attempts: 1},
		})
		tr := applyReportUploadStatus(s, map[string]nodeUploadObservation{
			"node-a": {Node: "node-a", Rank: observationSucceeded, Message: "s3://b/k/a.txt"},
			"node-b": {Node: "node-b", Rank: observationFinalFailure, Message: "404 NoSuchBucket"},
		})
		if len(tr) != 1 || tr[0].Succeeded {
			t.Fatalf("transitions = %+v, want only node-b's failure", tr)
		}
		if condStatus(s) != metav1.ConditionFalse {
			t.Errorf("condition = %s, want False on partial failure", condStatus(s))
		}
		byNode := map[string]podtracev1alpha1.SessionNodeReport{}
		for i := range s.Status.NodeReports {
			byNode[s.Status.NodeReports[i].Node] = s.Status.NodeReports[i]
		}
		if byNode["node-a"].Location != "s3://b/k/a.txt" || byNode["node-a"].State != podtracev1alpha1.NodeReportSucceeded {
			t.Errorf("node-a success not retained: %+v", byNode["node-a"])
		}
		if byNode["node-b"].FailureReason != podtracev1alpha1.ReportFailureReasonBucketNotFound {
			t.Errorf("node-b reason = %q, want BucketNotFound", byNode["node-b"].FailureReason)
		}
		if s.Status.ReportLocation != "" {
			t.Errorf("multi-node scalar reportLocation must stay empty, got %q", s.Status.ReportLocation)
		}
	})

	t.Run("vanished pod cannot downgrade a concluded node", func(t *testing.T) {
		s := newSession()
		applyReportUploadStatus(s, map[string]nodeUploadObservation{
			"node-a": {Node: "node-a", Rank: observationSucceeded, Message: "s3://b/k/a.txt"},
			"node-b": {Node: "node-b", Rank: observationSucceeded, Message: "s3://b/k/b.txt"},
		})
		// Pods TTL-GC'd: no live observations at all.
		applyReportUploadStatus(s, map[string]nodeUploadObservation{})
		if condStatus(s) != metav1.ConditionTrue {
			t.Errorf("condition = %s, must stay True after evidence Pods vanish", condStatus(s))
		}
		for i := range s.Status.NodeReports {
			if s.Status.NodeReports[i].State != podtracev1alpha1.NodeReportSucceeded {
				t.Errorf("node %s downgraded: %+v", s.Status.NodeReports[i].Node, s.Status.NodeReports[i])
			}
		}
	})

	t.Run("all nodes succeed converges True", func(t *testing.T) {
		s := newSession()
		applyReportUploadStatus(s, map[string]nodeUploadObservation{
			"node-a": {Node: "node-a", Rank: observationSucceeded, Message: "s3://b/k/a.txt"},
			"node-b": {Node: "node-b", Rank: observationSucceeded, Message: "s3://b/k/b.txt"},
		})
		if condStatus(s) != metav1.ConditionTrue {
			t.Errorf("condition = %s, want True", condStatus(s))
		}
	})

	t.Run("transient failure while pod Running is not final", func(t *testing.T) {
		s := newSession()
		applyReportUploadStatus(s, map[string]nodeUploadObservation{
			"node-a": {Node: "node-a", Rank: observationSucceeded, Message: "s3://b/k/a.txt"},
			"node-b": {Node: "node-b", Rank: observationTransientFailure, Attempts: 2, Message: "403 AccessDenied"},
		})
		if condStatus(s) != metav1.ConditionUnknown {
			t.Errorf("condition = %s, a restartable uploader must keep Unknown", condStatus(s))
		}
		byNode := map[string]podtracev1alpha1.SessionNodeReport{}
		for i := range s.Status.NodeReports {
			byNode[s.Status.NodeReports[i].Node] = s.Status.NodeReports[i]
		}
		if byNode["node-b"].State != podtracev1alpha1.NodeReportPending {
			t.Errorf("transient failure must stay Pending, got %q", byNode["node-b"].State)
		}
	})
}

// TestApplyReportUploadStatus_LegacyScalarMigration ensures an
// operator upgraded mid-single-node-session recovers the already
// concluded result from the legacy scalar status even if the uploader
// Pod is gone.
func TestApplyReportUploadStatus_LegacyScalarMigration(t *testing.T) {
	s := &podtracev1alpha1.PodTraceSession{
		Spec: podtracev1alpha1.PodTraceSessionSpec{
			ReportRef: &podtracev1alpha1.ReportReference{
				ObjectStore: &podtracev1alpha1.ObjectStoreReference{URI: "s3://b/k"},
			},
		},
		Status: podtracev1alpha1.PodTraceSessionStatus{
			Jobs:           []podtracev1alpha1.SessionJobRef{{Node: "node-a", Name: "j-a", Completed: true}},
			ReportLocation: "s3://b/k/a.txt",
			ReportAttempts: 1,
			Conditions: []metav1.Condition{{
				Type:   ConditionReportUploaded,
				Status: metav1.ConditionTrue,
				Reason: "ObjectStoreUploadSucceeded",
			}},
		},
	}
	tr := applyReportUploadStatus(s, map[string]nodeUploadObservation{})
	if len(tr) != 0 {
		t.Errorf("already-concluded migrated node must not produce transitions: %+v", tr)
	}
	found := false
	for i := range s.Status.NodeReports {
		if s.Status.NodeReports[i].Node == "node-a" &&
			s.Status.NodeReports[i].State == podtracev1alpha1.NodeReportSucceeded &&
			s.Status.NodeReports[i].Location == "s3://b/k/a.txt" {
			found = true
		}
	}
	if !found {
		t.Errorf("legacy scalar success not migrated into nodeReports: %+v", s.Status.NodeReports)
	}
	cond := (*metav1.Condition)(nil)
	for i := range s.Status.Conditions {
		if s.Status.Conditions[i].Type == ConditionReportUploaded {
			cond = &s.Status.Conditions[i]
		}
	}
	if cond != nil && cond.Status != metav1.ConditionTrue {
		t.Errorf("migrated condition must be True, got %s", cond.Status)
	}
}

