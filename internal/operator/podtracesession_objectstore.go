package operator

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
	"github.com/gma1k/podtrace/internal/reportsink/objectstore"
)

const reportUploaderContainerName = "report-uploader"

// maxReportConditionMessage caps the ReportUploaded condition message
// well under Kubernetes' 32KiB condition-message limit so a session
// with many failing nodes can never be rejected on status update.
const maxReportConditionMessage = 16 * 1024

// nodeUploadObservation is the operator's view of one node's
// report-uploader sidecar, folded from every Pod of the node's session
// Job. Rank encodes observation strength so folding is independent of
// Pod list order: stronger conclusions always win.
type nodeUploadObservation struct {
	Node string

	// Attempts is the highest uploader restart count seen on any of
	// the node's Pods.
	Attempts int32

	// rank:
	//
	//	1 — uploader not terminated yet (running / waiting / absent
	//	    status while the Pod is alive)
	//	2 — uploader terminated non-zero while the Pod is still
	//	    Running: the kubelet will restart a RestartPolicy=Always
	//	    native sidecar, so this is a transient attempt, not a
	//	    stable conclusion
	//	3 — uploader finally failed (terminated non-zero on a Pod that
	//	    is no longer Running; no further restart is coming)
	//	4 — uploader terminated zero with a resolved object URI
	Rank int

	// Message carries the best termination message for this rank:
	// the resolved object URI on success, the error text on failure.
	Message string
}

const (
	observationPending          = 1
	observationTransientFailure = 2
	observationFinalFailure     = 3
	observationSucceeded        = 4
)

func (o nodeUploadObservation) succeeded() bool { return o.Rank == observationSucceeded }
func (o nodeUploadObservation) failed() bool    { return o.Rank == observationFinalFailure }

// harvestNodeReportObservations lists the session's Pods in every
// system namespace the session may fan out to and folds their
// report-uploader container statuses into a node-keyed observation
// map. The map is the only per-reconcile input; persistence lives in
// PodTraceSession.status.nodeReports (see mergeNodeReports).
func harvestNodeReportObservations(ctx context.Context, c client.Client, s *podtracev1alpha1.PodTraceSession, namespaces []string) (map[string]nodeUploadObservation, error) {
	if s == nil || s.Spec.ReportRef == nil || s.Spec.ReportRef.ObjectStore == nil {
		return nil, nil
	}

	out := map[string]nodeUploadObservation{}
	for _, ns := range dedupNonEmpty(namespaces) {
		var pods corev1.PodList
		if err := c.List(ctx, &pods,
			client.InNamespace(ns),
			client.MatchingLabels{
				LabelSessionName: s.Name,
				LabelSessionNS:   s.Namespace,
			},
		); err != nil {
			return nil, fmt.Errorf("list session pods in %s: %w", ns, err)
		}
		for i := range pods.Items {
			if o, ok := observePodUploader(&pods.Items[i]); ok {
				foldNodeObservation(out, o)
			}
		}
	}
	return out, nil
}

// observePodUploader extracts the uploader observation from one Pod.
// Pods without a report-uploader init container (sidecar disabled,
// direct-upload sessions) contribute nothing.
func observePodUploader(pod *corev1.Pod) (nodeUploadObservation, bool) {
	var cs *corev1.ContainerStatus
	for i := range pod.Status.InitContainerStatuses {
		if pod.Status.InitContainerStatuses[i].Name == reportUploaderContainerName {
			cs = &pod.Status.InitContainerStatuses[i]
			break
		}
	}
	if cs == nil {
		return nodeUploadObservation{}, false
	}

	node := pod.Labels[LabelNodeName]
	if node == "" {
		node = pod.Spec.NodeName
	}
	o := nodeUploadObservation{Node: node, Attempts: cs.RestartCount, Message: uploaderStatusMessage(*cs)}

	switch {
	case cs.State.Running != nil:
		// Still uploading (or waiting for the main container's
		// SIGTERM trigger); the conclusion is not in yet.
		o.Rank = observationPending
		o.Message = ""
	case cs.State.Terminated != nil && cs.State.Terminated.ExitCode == 0 && strings.TrimSpace(o.Message) != "":
		o.Rank = observationSucceeded
	case pod.Status.Phase == corev1.PodRunning:
		// Pod still Running: a terminated non-zero uploader (or a
		// zero exit without a resolved URI) is restarted by the
		// RestartPolicy=Always native sidecar. Transient attempt,
		// not a stable conclusion.
		o.Rank = observationTransientFailure
	case cs.State.Terminated != nil || podTerminal(pod.Status.Phase):
		// Pod is over (Succeeded/Failed/Unknown) and the kubelet
		// will not run this uploader again: a terminated non-zero,
		// or a Waiting/unknown state on a finished Pod (e.g.
		// CrashLoopBackOff at shutdown), is the stable failure.
		o.Rank = observationFinalFailure
	default:
		o.Rank = observationPending
		o.Message = ""
	}
	return o, true
}

// podTerminal reports whether a Pod has left its active lifecycle,
// where the uploader cannot be (re)started by the kubelet.
func podTerminal(phase corev1.PodPhase) bool {
	return phase == corev1.PodSucceeded ||
		phase == corev1.PodFailed ||
		phase == corev1.PodUnknown
}

// uploaderStatusMessage renders the most informative message available
// on an uploader container status: current termination message, its
// previous-run fallback, or a Waiting state such as CrashLoopBackOff.
func uploaderStatusMessage(cs corev1.ContainerStatus) string {
	if msg := bestTerminationMessage(cs); msg != "" {
		return msg
	}
	if cs.State.Waiting != nil {
		return strings.TrimSpace(cs.State.Waiting.Reason + " " + cs.State.Waiting.Message)
	}
	return ""
}

// foldNodeObservation merges one Pod's observation into the node map,
// keeping the strongest rank regardless of Pod list order. Equal
// ranks tie-break deterministically (more attempts, then the more
// informative message), so two reconciles over differently ordered
// lists converge on identical state.
func foldNodeObservation(acc map[string]nodeUploadObservation, o nodeUploadObservation) {
	if o.Node == "" {
		return
	}
	existing, ok := acc[o.Node]
	if !ok {
		acc[o.Node] = o
		return
	}
	if o.Attempts > existing.Attempts {
		existing.Attempts = o.Attempts
	}
	switch {
	case o.Rank > existing.Rank:
		existing.Rank = o.Rank
		existing.Message = o.Message
	case o.Rank == existing.Rank && len(o.Message) > len(existing.Message):
		existing.Message = o.Message
	}
	acc[o.Node] = existing
}

// nodeReportTransition records one node reaching a terminal upload
// conclusion during a reconcile. Metrics are emitted once per
// transition, and transitions are derived purely from prior
// (persisted) status vs the merged status — so repeat reconciles and
// operator restarts never double-count.
type nodeReportTransition struct {
	Node      string
	Succeeded bool
	Message   string
}

// applyReportUploadStatus merges live per-node uploader observations
// into the session's persisted status (keyed by node) and recomputes
// the ReportUploaded condition. It returns the node-level terminal
// transitions this reconcile produced; callers emit metrics for those
// only after the status update succeeds.
//
// Merge rules:
//
//   - expected nodes = the nodes the session originally fanned out to
//     (status.jobs) union observed nodes; every expected node gets an
//     entry, Pending until observed terminally.
//   - Succeeded/Failed entries are sticky: later Pod observations
//     (including TTL-garbage-collected Pods vanishing from the list)
//     never downgrade them; successful locations and stable failure
//     reasons are both retained on partial failure.
//   - the condition leaves Unknown only once every expected node is
//     terminal; all-success → True, any failure → False.
//
// Sessions without an objectStore, or without the uploader sidecar
// (direct-upload sessions), are left untouched.
func applyReportUploadStatus(s *podtracev1alpha1.PodTraceSession, observed map[string]nodeUploadObservation) []nodeReportTransition {
	if s == nil || s.Spec.ReportRef == nil || s.Spec.ReportRef.ObjectStore == nil {
		return nil
	}
	// No sidecar evidence this reconcile and none persisted: the
	// session uploads directly from the main container (or no Job has
	// taken the sidecar path yet in a way this harvest can see). Keep
	// the legacy scalar/condition behavior's input surface untouched,
	// unless pre-nodeReports status already records a terminal
	// conclusion we must migrate.
	if len(observed) == 0 && len(s.Status.NodeReports) == 0 && legacyTerminalReportCondition(s) == nil {
		return nil
	}

	expected := expectedReportNodes(s, observed)
	migrated := migrateLegacyScalarReport(s, expected)
	prev := indexNodeReports(migrated)
	merged := mergeNodeReports(migrated, expected, observed)
	s.Status.NodeReports = merged

	applyScalarReportFields(s, expected, merged)

	transitions := make([]nodeReportTransition, 0)
	curByNode := indexNodeReports(merged)
	for _, node := range expected {
		// A missing prior entry has the zero State "", which is
		// non-terminal, so the first terminal observation transitions.
		cur := curByNode[node]
		if !nodeReportTerminal(prev[node].State) && nodeReportTerminal(cur.State) {
			transitions = append(transitions, nodeReportTransition{
				Node:      node,
				Succeeded: cur.State == podtracev1alpha1.NodeReportSucceeded,
				Message:   cur.Message,
			})
		}
	}

	s.Status.Conditions = upsertCondition(s.Status.Conditions, reportUploadedCondition(s.Generation, expected, merged))
	return transitions
}

// expectedReportNodes is the sorted set of nodes that should produce a
// report: the nodes of Jobs the session originally created (persisted
// in status.jobs, including carried-forward GC'd refs) union the nodes
// seen on live Pods and prior per-node status.
func expectedReportNodes(s *podtracev1alpha1.PodTraceSession, observed map[string]nodeUploadObservation) []string {
	set := map[string]struct{}{}
	for i := range s.Status.Jobs {
		if s.Status.Jobs[i].Node != "" {
			set[s.Status.Jobs[i].Node] = struct{}{}
		}
	}
	for node := range observed {
		if node != "" {
			set[node] = struct{}{}
		}
	}
	for i := range s.Status.NodeReports {
		if s.Status.NodeReports[i].Node != "" {
			set[s.Status.NodeReports[i].Node] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for node := range set {
		out = append(out, node)
	}
	sort.Strings(out)
	return out
}

// mergeNodeReports folds live observations into the persisted
// per-node entries. The output is sorted by node and never depends on
// iteration order of either input.
func mergeNodeReports(prior []podtracev1alpha1.SessionNodeReport, expected []string, observed map[string]nodeUploadObservation) []podtracev1alpha1.SessionNodeReport {
	byNode := make(map[string]podtracev1alpha1.SessionNodeReport, len(expected))
	for i := range prior {
		byNode[prior[i].Node] = prior[i]
	}

	for _, node := range expected {
		cur := byNode[node]
		if cur.Node == "" {
			cur = podtracev1alpha1.SessionNodeReport{Node: node, State: podtracev1alpha1.NodeReportPending}
		}
		o, live := observed[node]
		if o.Attempts > cur.Attempts {
			cur.Attempts = o.Attempts
		}
		// Terminal persisted state is sticky: a Pod disappearing under
		// TTL GC, a kubelet restart, or a later transient status must
		// never erase a concluded location or stable failure reason.
		if !nodeReportTerminal(cur.State) && live {
			switch {
			case o.succeeded():
				cur.State = podtracev1alpha1.NodeReportSucceeded
				cur.Location = o.Message
				cur.Message = o.Message
				cur.FailureReason = ""
			case o.failed():
				cur.State = podtracev1alpha1.NodeReportFailed
				cur.FailureReason = classifyUploadFailure(o.Message)
				cur.Message = o.Message
			default:
				cur.State = podtracev1alpha1.NodeReportPending
			}
		}
		byNode[node] = cur
	}

	out := make([]podtracev1alpha1.SessionNodeReport, 0, len(expected))
	for _, node := range expected {
		out = append(out, byNode[node])
	}
	return out
}

// applyScalarReportFields keeps the single-node scalar status
// (reportLocation / reportFailureReason / reportAttempts) identical to
// the historical behavior. Multi-node sessions carry per-node detail
// in nodeReports; a single scalar string cannot name several objects,
// so the scalars stay empty there to avoid ambiguous evidence.
func applyScalarReportFields(s *podtracev1alpha1.PodTraceSession, expected []string, merged []podtracev1alpha1.SessionNodeReport) {
	if len(expected) != 1 {
		s.Status.ReportLocation = ""
		s.Status.ReportFailureReason = ""
		s.Status.ReportAttempts = 0
		return
	}
	only := merged[0]
	s.Status.ReportAttempts = only.Attempts
	switch only.State {
	case podtracev1alpha1.NodeReportSucceeded:
		s.Status.ReportLocation = only.Location
		s.Status.ReportFailureReason = ""
	case podtracev1alpha1.NodeReportFailed:
		s.Status.ReportFailureReason = only.FailureReason
	default:
		s.Status.ReportFailureReason = ""
	}
}

// reportUploadedCondition computes the aggregate condition from the
// per-node entries.
func reportUploadedCondition(generation int64, expected []string, merged []podtracev1alpha1.SessionNodeReport) metav1.Condition {
	now := metav1.Now()
	byNode := indexNodeReports(merged)

	succeeded, failed := 0, 0
	for _, node := range expected {
		switch byNode[node].State {
		case podtracev1alpha1.NodeReportSucceeded:
			succeeded++
		case podtracev1alpha1.NodeReportFailed:
			failed++
		}
	}

	switch {
	case len(expected) > 0 && failed == 0 && succeeded == len(expected):
		return metav1.Condition{
			Type:               ConditionReportUploaded,
			Status:             metav1.ConditionTrue,
			Reason:             "ObjectStoreUploadSucceeded",
			Message:            successMessage(expected, byNode),
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	case len(expected) > 0 && succeeded+failed == len(expected):
		return metav1.Condition{
			Type:               ConditionReportUploaded,
			Status:             metav1.ConditionFalse,
			Reason:             "ObjectStoreUploadFailed",
			Message:            failureMessage(expected, byNode, succeeded, failed),
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	default:
		return metav1.Condition{
			Type:               ConditionReportUploaded,
			Status:             metav1.ConditionUnknown,
			Reason:             "UploadPending",
			Message:            pendingMessage(expected, byNode, succeeded, failed),
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	}
}

func successMessage(expected []string, byNode map[string]podtracev1alpha1.SessionNodeReport) string {
	if len(expected) == 1 {
		return byNode[expected[0]].Location
	}
	parts := make([]string, 0, len(expected))
	for _, node := range expected {
		parts = append(parts, node+"="+byNode[node].Location)
	}
	return truncateReportMessage(strings.Join(parts, "; "))
}

func failureMessage(expected []string, byNode map[string]podtracev1alpha1.SessionNodeReport, succeeded, failed int) string {
	if len(expected) == 1 {
		return truncateReportMessage(byNode[expected[0]].Message)
	}
	parts := make([]string, 0, len(expected))
	if succeeded > 0 {
		ok := make([]string, 0, succeeded)
		for _, node := range expected {
			if byNode[node].State == podtracev1alpha1.NodeReportSucceeded {
				ok = append(ok, node+"="+byNode[node].Location)
			}
		}
		parts = append(parts, "succeeded: "+strings.Join(ok, ", "))
	}
	bad := make([]string, 0, failed)
	for _, node := range expected {
		nr := byNode[node]
		if nr.State == podtracev1alpha1.NodeReportFailed {
			bad = append(bad, fmt.Sprintf("%s failed(%s): %s", node, nr.FailureReason, nr.Message))
		}
	}
	parts = append(parts, "failed: "+strings.Join(bad, "; "))
	return truncateReportMessage(strings.Join(parts, "; "))
}

func pendingMessage(expected []string, byNode map[string]podtracev1alpha1.SessionNodeReport, succeeded, failed int) string {
	if len(expected) <= 1 {
		return "sidecar has not terminated yet"
	}
	waiting := make([]string, 0, len(expected)-succeeded-failed)
	for _, node := range expected {
		if !nodeReportTerminal(byNode[node].State) {
			waiting = append(waiting, node)
		}
	}
	return truncateReportMessage(fmt.Sprintf(
		"%d/%d node(s) terminal; waiting on uploader(s): %s",
		succeeded+failed, len(expected), strings.Join(waiting, ", ")))
}

func truncateReportMessage(msg string) string {
	if len(msg) <= maxReportConditionMessage {
		return msg
	}
	return msg[:maxReportConditionMessage-len("…(truncated)")] + "…(truncated)"
}

func nodeReportTerminal(state podtracev1alpha1.NodeReportUploadState) bool {
	return state == podtracev1alpha1.NodeReportSucceeded || state == podtracev1alpha1.NodeReportFailed
}

func indexNodeReports(reports []podtracev1alpha1.SessionNodeReport) map[string]podtracev1alpha1.SessionNodeReport {
	byNode := make(map[string]podtracev1alpha1.SessionNodeReport, len(reports))
	for i := range reports {
		byNode[reports[i].Node] = reports[i]
	}
	return byNode
}

// legacyTerminalReportCondition returns the pre-nodeReports
// ReportUploaded condition when it already carries a terminal verdict
// (True/False), or nil otherwise. Scalar fields alone are not trusted:
// the historical reconciler cleared reportFailureReason on every
// pending observation, so a non-empty scalar without a terminal
// condition is stale input, not a conclusion.
func legacyTerminalReportCondition(s *podtracev1alpha1.PodTraceSession) *metav1.Condition {
	for i := range s.Status.Conditions {
		if s.Status.Conditions[i].Type == ConditionReportUploaded &&
			(s.Status.Conditions[i].Status == metav1.ConditionTrue ||
				s.Status.Conditions[i].Status == metav1.ConditionFalse) {
			return &s.Status.Conditions[i]
		}
	}
	return nil
}

// migrateLegacyScalarReport seeds the per-node view from the
// pre-nodeReports scalar status when an operator upgrade lands in the
// middle of a single-node session: without it the already-concluded
// result would be forgotten until the (possibly TTL-GC'd) Pod could be
// re-harvested. Multi-node scalar state was ambiguous by construction
// and is intentionally not migrated.
func migrateLegacyScalarReport(s *podtracev1alpha1.PodTraceSession, expected []string) []podtracev1alpha1.SessionNodeReport {
	if len(s.Status.NodeReports) > 0 || len(expected) != 1 {
		return s.Status.NodeReports
	}
	cond := legacyTerminalReportCondition(s)
	if cond == nil {
		return s.Status.NodeReports
	}
	node := expected[0]
	r := podtracev1alpha1.SessionNodeReport{
		Node:     node,
		Attempts: s.Status.ReportAttempts,
	}
	switch {
	case cond.Status == metav1.ConditionTrue && s.Status.ReportLocation != "":
		r.State = podtracev1alpha1.NodeReportSucceeded
		r.Location = s.Status.ReportLocation
		r.Message = s.Status.ReportLocation
	case cond.Status == metav1.ConditionFalse && s.Status.ReportFailureReason != "":
		r.State = podtracev1alpha1.NodeReportFailed
		r.FailureReason = s.Status.ReportFailureReason
		r.Message = cond.Message
	default:
		return s.Status.NodeReports
	}
	return []podtracev1alpha1.SessionNodeReport{r}
}

// reportUploadConverged reports whether the session's report result
// has left Unknown. Sessions with no ReportUploaded condition have no
// objectStore sidecar result to wait for and count as converged.
func reportUploadConverged(s *podtracev1alpha1.PodTraceSession) bool {
	for i := range s.Status.Conditions {
		if s.Status.Conditions[i].Type == ConditionReportUploaded {
			return s.Status.Conditions[i].Status != metav1.ConditionUnknown
		}
	}
	return true
}

// reportUploadAwaited reports whether evidence of sidecar uploads
// exists (status.nodeReports) while the aggregate result is still
// Unknown. While true, TTL cleanup must not delete the session and its
// child Jobs/Pods — those Pods are the sole evidence for the nodes not
// yet concluded.
func reportUploadAwaited(s *podtracev1alpha1.PodTraceSession) bool {
	return s.Spec.ReportRef != nil &&
		s.Spec.ReportRef.ObjectStore != nil &&
		len(s.Status.NodeReports) > 0 &&
		!reportUploadConverged(s)
}

func dedupNonEmpty(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// classifyUploadFailure maps a sidecar's stderr / termination message
// into a stable reason enum. The mapping is intentionally pattern-based
// rather than typed-error-based because the message arrives as a string
// from another process (the sidecar) — there is no in-process error
// chain to .Is() / .As() against.
func classifyUploadFailure(message string) podtracev1alpha1.ReportFailureReason {
	if message == "" {
		return podtracev1alpha1.ReportFailureReasonUnknown
	}
	m := strings.ToLower(message)

	switch {
	case strings.Contains(m, "no such bucket"),
		strings.Contains(m, "nosuchbucket"),
		strings.Contains(m, "404"),
		strings.Contains(m, "storage: bucket doesn't exist"),
		strings.Contains(m, "containernotfound"):
		return podtracev1alpha1.ReportFailureReasonBucketNotFound
	case strings.Contains(m, "access denied"),
		strings.Contains(m, "accessdenied"),
		strings.Contains(m, "forbidden"),
		strings.Contains(m, "403"),
		strings.Contains(m, "authorizationfailed"),
		strings.Contains(m, "signaturedoesnotmatch"):
		return podtracev1alpha1.ReportFailureReasonAccessDenied
	case strings.Contains(m, "credential"),
		strings.Contains(m, "unauthorized"),
		strings.Contains(m, "401"),
		strings.Contains(m, "no credentials"),
		strings.Contains(m, "missing"+"credentials"),
		strings.Contains(m, "default credentials"):
		return podtracev1alpha1.ReportFailureReasonCredentialMissing
	case strings.Contains(m, "timeout"),
		strings.Contains(m, "deadline exceeded"),
		strings.Contains(m, "i/o timeout"),
		strings.Contains(m, "connection refused"),
		strings.Contains(m, "no such host"),
		strings.Contains(m, "tls handshake"):
		return podtracev1alpha1.ReportFailureReasonNetworkTimeout
	case isInvalidURIMessage(m):
		return podtracev1alpha1.ReportFailureReasonInvalidURI
	default:
		return podtracev1alpha1.ReportFailureReasonUnknown
	}
}

func isInvalidURIMessage(lowered string) bool {
	return strings.Contains(lowered, "unsupported uri scheme") ||
		strings.Contains(lowered, "must include scheme and host") ||
		strings.Contains(lowered, "must include a container") ||
		strings.Contains(lowered, "parse uri") ||
		strings.Contains(lowered, "object key is empty")
}

var _ = objectstore.SchemeS3

// bestTerminationMessage picks the most informative termination
// message between the container's current and previous terminated
// states.
func bestTerminationMessage(cs corev1.ContainerStatus) string {
	current := ""
	if cs.State.Terminated != nil {
		current = strings.TrimSpace(cs.State.Terminated.Message)
	}
	previous := ""
	if cs.LastTerminationState.Terminated != nil {
		previous = strings.TrimSpace(cs.LastTerminationState.Terminated.Message)
	}

	if current != "" && !isKubeletShutdownMessage(current) {
		return current
	}
	if previous != "" {
		return previous
	}
	return current
}

// isKubeletShutdownMessage reports whether a message looks like the
// generic placeholder kubelet writes when it SIGKILLs a sidecar
// during pod shutdown rather than wording produced by our uploader.
func isKubeletShutdownMessage(msg string) bool {
	lc := strings.ToLower(msg)
	return strings.Contains(lc, "container could not be located when the pod was terminated") ||
		strings.Contains(lc, "the node was lost") ||
		strings.Contains(lc, "containerstatusunknown")
}
