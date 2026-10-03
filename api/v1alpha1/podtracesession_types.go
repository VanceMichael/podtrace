package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SessionState represents the high-level lifecycle state of a
// PodTraceSession.
// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
type SessionState string

// ReportFailureReason is the stable enum the operator stamps onto
// status.reportFailureReason when an objectStore sidecar terminates
// non-zero. Operators consume this for alerting and runbooks; the
// free-text cause stays in conditions[type=ReportUploaded].message.
// +kubebuilder:validation:Enum=InvalidURI;CredentialMissing;BucketNotFound;AccessDenied;NetworkTimeout;Unknown
type ReportFailureReason string

var (
	ReportFailureReasonInvalidURI        = ReportFailureReason("InvalidURI")
	ReportFailureReasonCredentialMissing = ReportFailureReason("CredentialMissing")
	ReportFailureReasonBucketNotFound    = ReportFailureReason("BucketNotFound")
	ReportFailureReasonAccessDenied      = ReportFailureReason("AccessDenied")
	ReportFailureReasonNetworkTimeout    = ReportFailureReason("NetworkTimeout")
	ReportFailureReasonUnknown           = ReportFailureReason("Unknown")
)

const (
	SessionStatePending   SessionState = "Pending"
	SessionStateRunning   SessionState = "Running"
	SessionStateCompleted SessionState = "Completed"
	SessionStateFailed    SessionState = "Failed"
)

// PodTraceSessionSpec defines a bounded diagnose-mode trace. The operator
// reconciles this into one privileged Job per node hosting matched pods.
// +kubebuilder:validation:XValidation:rule="[has(self.selector), has(self.podRefs)].filter(x, x).size() == 1",message="exactly one of spec.selector or spec.podRefs must be set"
// +kubebuilder:validation:XValidation:rule="has(self.reportRef) || (has(self.exporterRef) && has(self.exporterRef.name))",message="at least one of spec.exporterRef or spec.reportRef must be set, otherwise the session has nowhere to put its output"
type PodTraceSessionSpec struct {
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`

	// +optional
	PodRefs []PodRef `json:"podRefs,omitempty"`

	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`

	// +optional
	ContainerName string `json:"containerName,omitempty"`

	// +kubebuilder:validation:Required
	Duration metav1.Duration `json:"duration"`

	// +optional
	Filters []EventFilter `json:"filters,omitempty"`

	// ExporterRef names the ExporterConfig this session ships spans to.
	// +optional
	ExporterRef corev1.LocalObjectReference `json:"exporterRef,omitempty"`

	// TracerConfigRef pins every Job this session spawns to one
	// TracerConfig, overriding the per-node fleet lookup.
	// +optional
	TracerConfigRef *corev1.LocalObjectReference `json:"tracerConfigRef,omitempty"`

	// +optional
	Thresholds *Thresholds `json:"thresholds,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	SamplePercent *int32 `json:"samplePercent,omitempty"`

	// +optional
	ReportRef *ReportReference `json:"reportRef,omitempty"`

	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// SessionJobRef describes a child Job created by the session reconciler.
type SessionJobRef struct {
	// +kubebuilder:validation:Required
	Node string `json:"node"`

	// +kubebuilder:validation:Required
	Name string `json:"name"`

	Completed bool `json:"completed"`

	// +optional
	TotalEvents int64 `json:"totalEvents,omitempty"`

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// +optional
	Message string `json:"message,omitempty"`
}

// SessionTargetPod identifies one pod captured in a session's frozen
// target snapshot.
type SessionTargetPod struct {
	// +kubebuilder:validation:Required
	Namespace string `json:"namespace"`

	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Node is the node the pod was scheduled on when the snapshot was
	// taken. The session fan-out is pinned to this node set for the rest
	// of the session's life.
	// +kubebuilder:validation:Required
	Node string `json:"node"`
}

// SessionTargetNode records one fan-out node together with the
// TracerConfig and system namespace resolved for it when the snapshot
// was taken, so later relabels or TracerConfig changes cannot move the
// node's Job to another namespace or config.
type SessionTargetNode struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// +optional
	TracerConfigName string `json:"tracerConfigName,omitempty"`

	// +optional
	SystemNamespace string `json:"systemNamespace,omitempty"`
}

// SessionTargetSnapshot is the immutable, grant-authorized target set a
// session resolved on its first non-empty match. Once persisted, every
// downstream decision — per-node TracerConfig and system namespace,
// exporter/object-store resources, pod-read RBAC, capacity checks, Job
// creation and backfill, the completion denominator, and the summary —
// is derived from this snapshot instead of the live selector, so a
// workload rollout or pod/node disappearing mid-session can neither
// absorb new pods nor complete the session early.
//
// Namespaces rejected by the consent check at first resolution are
// reported through DeniedNamespaces but never enter Nodes/Namespaces.
type SessionTargetSnapshot struct {
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Nodes []SessionTargetNode `json:"nodes,omitempty" patchStrategy:"merge" patchMergeKey:"name"`

	// Namespaces is the frozen selector allowlist (the namespaces the
	// in-Job CLI was authorized to read), as resolved at snapshot time.
	// +optional
	Namespaces []string `json:"namespaces,omitempty"`

	// Pods is the frozen set of grant-authorized, running pods the
	// session attached to. Job specs enumerate these explicitly instead
	// of re-expanding the live selector.
	// +optional
	Pods []SessionTargetPod `json:"pods,omitempty"`

	// PodRefs is the frozen grant-authorized spec.podRefs list (podRefs
	// sessions only). Unlike selector-discovered pods, an explicitly
	// named ref is a user-requested target: if it is not running yet at
	// snapshot time but appears later, the existing podRefs behavior is
	// preserved and the Job may still attach to it.
	// +optional
	PodRefs []PodRef `json:"podRefs,omitempty"`

	// +optional
	DeniedNamespaces []string `json:"deniedNamespaces,omitempty"`

	// +optional
	ResolvedAt *metav1.Time `json:"resolvedAt,omitempty"`
}

// SessionSummary is a compact roll-up of what the session observed.
type SessionSummary struct {
	TotalEvents int64 `json:"totalEvents"`

	// +optional
	EventsByFilter map[string]int64 `json:"eventsByFilter,omitempty"`

	// +optional
	ErrorsDetected int32 `json:"errorsDetected,omitempty"`
}

// PodTraceSessionStatus reflects the observed state of a PodTraceSession.
type PodTraceSessionStatus struct {
	// +optional
	State SessionState `json:"state,omitempty"`

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// +optional
	// +patchMergeKey=node
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=node
	Jobs []SessionJobRef `json:"jobs,omitempty" patchStrategy:"merge" patchMergeKey:"node"`

	// +optional
	Summary *SessionSummary `json:"summary,omitempty"`

	TargetNamespaces []string `json:"targetNamespaces,omitempty"`

	// TargetSnapshot freezes the grant-authorized target set on the
	// first non-empty resolution and drives every downstream decision
	// for the rest of the session's life.
	// +optional
	TargetSnapshot *SessionTargetSnapshot `json:"targetSnapshot,omitempty"`

	// +optional
	ReportLocation string `json:"reportLocation,omitempty"`

	// +optional
	ReportFailureReason ReportFailureReason `json:"reportFailureReason,omitempty"`

	// +optional
	ReportAttempts int32 `json:"reportAttempts,omitempty"`

	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	Policy *PolicyStatus `json:"policy,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pts,categories=podtrace
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Duration",type=string,JSONPath=`.spec.duration`
// +kubebuilder:printcolumn:name="Exporter",type=string,JSONPath=`.spec.exporterRef.name`
// +kubebuilder:printcolumn:name="Events",type=integer,JSONPath=`.status.summary.totalEvents`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PodTraceSession is a bounded, diagnose-mode trace. Running a session
// creates one privileged Job per node hosting a matched pod, each invoking
// `podtrace --diagnose <duration>` against the local subset.
type PodTraceSession struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PodTraceSessionSpec   `json:"spec,omitempty"`
	Status PodTraceSessionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PodTraceSessionList contains a list of PodTraceSession.
type PodTraceSessionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PodTraceSession `json:"items"`
}

func init() {
	SchemeBuilder.Register(addKnownTypes(&PodTraceSession{}, &PodTraceSessionList{}))
}
