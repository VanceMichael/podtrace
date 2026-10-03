package operator

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
	"github.com/gma1k/podtrace/internal/alerting"
)

// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// alertEventPredicate limits the Event watch to podtrace alert Events so the
// controller is not woken by every Event in the cluster.
func alertEventPredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		ev, ok := obj.(*corev1.Event)
		return ok && ev.Reason == alerting.EventReasonAlert
	})
}

// mapEventToTriggerSchedules enqueues every trigger-mode schedule when a
// podtrace alert Event fires.
func (r *PodTraceScheduleReconciler) mapEventToTriggerSchedules(ctx context.Context, obj client.Object) []reconcile.Request {
	ev, ok := obj.(*corev1.Event)
	if !ok || ev.Reason != alerting.EventReasonAlert {
		return nil
	}
	var schedules podtracev1alpha1.PodTraceScheduleList
	if err := r.List(ctx, &schedules); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range schedules.Items {
		if schedules.Items[i].Spec.Trigger == nil {
			continue
		}
		reqs = append(reqs, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&schedules.Items[i]),
		})
	}
	return reqs
}

// Trigger-mode defaults. Vars (not consts) so envtests can shorten them.
var (
	defaultTriggerCooldown = 10 * time.Minute

	defaultTriggerMaxSessionsPerHour int32 = 6

	triggerRateWindow = time.Hour

	triggerResyncInterval = 60 * time.Second
)

// triggerSourceKindForAlertSource maps an agent alert Source token to the
// TriggerSourceKind a schedule selects on.
func triggerSourceKindForAlertSource(source string) (podtracev1alpha1.TriggerSourceKind, bool) {
	switch source {
	case alerting.AlertSourceResourceMonitor, alerting.AlertSourceResourceMonitorBPF:
		return podtracev1alpha1.TriggerSourceResourceAlert, true
	case alerting.AlertSourceOOM:
		return podtracev1alpha1.TriggerSourceOOMKill, true
	case alerting.AlertSourceErrorRate:
		return podtracev1alpha1.TriggerSourceErrorRate, true
	case alerting.AlertSourceIssue:
		return podtracev1alpha1.TriggerSourceIssue, true
	default:
		return "", false
	}
}

// severityRank orders alert severities. Higher fires when a source's
// MinSeverity is lower-or-equal. "error" sits below "warning" so it never
// satisfies a warning-or-higher gate.
func severityRank(sev string) int {
	switch sev {
	case string(alerting.SeverityFatal):
		return 3
	case string(alerting.SeverityCritical):
		return 2
	case string(alerting.SeverityWarning):
		return 1
	default:
		return 0
	}
}

// effectiveMinSeverity resolves an empty MinSeverity to the safe default
// (critical), so a warning does not spawn a privileged Job unless opted in.
func effectiveMinSeverity(s string) string {
	if s == "" {
		return string(alerting.SeverityCritical)
	}
	return s
}

// Receipt annotation names. The child session carries the receipt of the
// exact Event occurrence it consumed, so a firing can be recovered from the
// session alone when the schedule status write was lost.
const (
	AnnotationTriggerEventKey = "podtrace.io/trigger-event-key"

	annotationTriggeredBy     = "podtrace.io/triggered-by"
	annotationTriggerSeverity = "podtrace.io/trigger-severity"
	annotationTriggerPod      = "podtrace.io/trigger-pod"
	annotationTriggeredAt     = "podtrace.io/triggered-at"
)

// alertEvent is the parsed, trigger-relevant view of a Kubernetes Event.
type alertEvent struct {
	Namespace string
	PodName   string
	Kind      podtracev1alpha1.TriggerSourceKind
	Severity  string
	At        time.Time

	// Key is the stable identity of this Event occurrence: identical across
	// watch replays, list fallback and ResourceVersion/LastTimestamp updates
	// of the same object; distinct for genuinely new occurrences.
	Key string
}

// parseAlertEvent extracts the trigger view from a raw Event, or false when
// the Event is not a podtrace alert or lacks the fields a trigger needs.
func parseAlertEvent(ev *corev1.Event) (alertEvent, bool) {
	if ev == nil || ev.Reason != alerting.EventReasonAlert {
		return alertEvent{}, false
	}
	if ev.InvolvedObject.Kind != "Pod" || ev.InvolvedObject.Name == "" {
		return alertEvent{}, false
	}
	kind, ok := triggerSourceKindForAlertSource(ev.Annotations[alerting.AnnotationAlertSource])
	if !ok {
		return alertEvent{}, false
	}
	ns := ev.InvolvedObject.Namespace
	if ns == "" {
		ns = ev.Namespace
	}
	return alertEvent{
		Namespace: ns,
		PodName:   ev.InvolvedObject.Name,
		Kind:      kind,
		Severity:  ev.Annotations[alerting.AnnotationAlertSeverity],
		At:        eventTime(ev),
		Key:       eventOccurrenceKey(ev, ns, ev.InvolvedObject.Name, kind),
	}, true
}

// eventOccurrenceKey establishes a stable identity for one triggerable
// Kubernetes Event occurrence. The apiserver always assigns a UID, so the
// key survives watch replays, list fallback and ResourceVersion updates —
// including LastTimestamp/Count mutations of the same aggregated Event.
// Objects without a UID (test fakes) fall back to a content composite built
// from immutable timestamps, never the mutable LastTimestamp alone.
func eventOccurrenceKey(ev *corev1.Event, namespace, pod string, kind podtracev1alpha1.TriggerSourceKind) string {
	if len(ev.UID) > 0 {
		return "uid:" + string(ev.UID)
	}
	stamp := eventTime(ev)
	switch {
	case !ev.CreationTimestamp.IsZero():
		stamp = ev.CreationTimestamp.Time
	case !ev.FirstTimestamp.IsZero():
		stamp = ev.FirstTimestamp.Time
	}
	return "composite:" + namespace + "/" + pod + "/" + string(kind) + "/" + stamp.UTC().Format(time.RFC3339Nano)
}

// parseTriggerPodRef recovers the alerting pod coordinates from a session's
// trigger-pod receipt ("namespace/name").
func parseTriggerPodRef(ref string) (namespace, pod string) {
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", ""
	}
	return parts[0], parts[1]
}

// firingByKey returns the ledger entry that consumed the given occurrence,
// or nil.
func firingByKey(firings []podtracev1alpha1.TriggerFiring, key string) *podtracev1alpha1.TriggerFiring {
	for i := range firings {
		if firings[i].OccurrenceKey == key {
			return &firings[i]
		}
	}
	return nil
}

// buildTriggerLedger merges firing evidence recoverable from the schedule's
// child sessions with the persisted recentFirings into a single deduplicated,
// time-ordered ledger. A child session's receipt is authoritative proof that
// its occurrence was consumed — this is what recovers a firing whose status
// write was lost. Status entries with no matching child are kept for sessions
// already garbage-collected after their receipt was persisted.
func buildTriggerLedger(children []podtracev1alpha1.PodTraceSession, persisted []podtracev1alpha1.TriggerFiring) []podtracev1alpha1.TriggerFiring {
	seen := map[string]struct{}{}
	var out []podtracev1alpha1.TriggerFiring
	for i := range children {
		s := &children[i]
		key := s.Annotations[AnnotationTriggerEventKey]
		if key == "" {
			continue
		}
		ns, pod := parseTriggerPodRef(s.Annotations[annotationTriggerPod])
		if ns == "" || pod == "" {
			continue
		}
		out = append(out, podtracev1alpha1.TriggerFiring{
			OccurrenceKey: key,
			Namespace:     ns,
			PodName:       pod,
			Time:          metav1.NewTime(s.CreationTimestamp.Time),
			SessionName:   s.Name,
		})
		seen[key] = struct{}{}
	}
	for i := range persisted {
		f := persisted[i]
		if f.OccurrenceKey != "" {
			if _, ok := seen[f.OccurrenceKey]; ok {
				continue
			}
			seen[f.OccurrenceKey] = struct{}{}
		}
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.Before(&out[j].Time) })
	return out
}

// eventTime returns the most recent observation time recorded on the Event.
func eventTime(ev *corev1.Event) time.Time {
	if !ev.LastTimestamp.IsZero() {
		return ev.LastTimestamp.Time
	}
	if !ev.EventTime.IsZero() {
		return ev.EventTime.Time
	}
	return ev.CreationTimestamp.Time
}

// matchesTriggerSources reports whether the event's kind+severity satisfies
// any of the trigger's configured sources.
func matchesTriggerSources(ev alertEvent, sources []podtracev1alpha1.TriggerSource) bool {
	got := severityRank(ev.Severity)
	for _, src := range sources {
		if src.Kind != ev.Kind {
			continue
		}
		if got >= severityRank(effectiveMinSeverity(src.MinSeverity)) {
			return true
		}
	}
	return false
}

// lastFiringForPod returns the most recent firing recorded for a pod, or nil.
func lastFiringForPod(firings []podtracev1alpha1.TriggerFiring, namespace, pod string) *podtracev1alpha1.TriggerFiring {
	var latest *podtracev1alpha1.TriggerFiring
	for i := range firings {
		f := &firings[i]
		if f.Namespace != namespace || f.PodName != pod {
			continue
		}
		if latest == nil || f.Time.After(latest.Time.Time) {
			latest = f
		}
	}
	return latest
}

// eligibleToFire reports whether a session may fire for an alert event given
// the pod's last firing and the cooldown.
func eligibleToFire(last *podtracev1alpha1.TriggerFiring, eventAt, now time.Time, cooldown time.Duration) bool {
	if last == nil {
		return true
	}
	if !eventAt.After(last.Time.Time) {
		return false
	}
	return now.Sub(last.Time.Time) >= cooldown
}

// countFiringsInWindow counts firings whose time falls within [now-window, now].
func countFiringsInWindow(firings []podtracev1alpha1.TriggerFiring, now time.Time, window time.Duration) int {
	cutoff := now.Add(-window)
	n := 0
	for i := range firings {
		if firings[i].Time.After(cutoff) {
			n++
		}
	}
	return n
}

// pruneFirings drops firings older than the retention horizon so the status
// log stays bounded.
func pruneFirings(firings []podtracev1alpha1.TriggerFiring, now time.Time, cooldown time.Duration) []podtracev1alpha1.TriggerFiring {
	horizon := triggerRateWindow
	if cooldown > horizon {
		horizon = cooldown
	}
	cutoff := now.Add(-horizon)
	out := make([]podtracev1alpha1.TriggerFiring, 0, len(firings))
	for i := range firings {
		if firings[i].Time.After(cutoff) {
			out = append(out, firings[i])
		}
	}
	return out
}

func triggerCooldown(t *podtracev1alpha1.TriggerSpec) time.Duration {
	if t.Cooldown != nil && t.Cooldown.Duration > 0 {
		return t.Cooldown.Duration
	}
	return defaultTriggerCooldown
}

func triggerMaxPerHour(t *podtracev1alpha1.TriggerSpec) int32 {
	if t.MaxSessionsPerHour != nil && *t.MaxSessionsPerHour > 0 {
		return *t.MaxSessionsPerHour
	}
	return defaultTriggerMaxSessionsPerHour
}

func triggerConcurrency(t *podtracev1alpha1.TriggerSpec) podtracev1alpha1.ConcurrencyPolicy {
	if t.ConcurrencyPolicy == "" {
		return podtracev1alpha1.ForbidConcurrent
	}
	return t.ConcurrencyPolicy
}

// reconcileTrigger drives event-driven ("flight recorder") session creation.
// Every triggerable Event occurrence is consumed at most once: replays only
// associate with the session that consumed it, while cooldown and the
// rolling-hour cap are computed from a single merged ledger.
func (r *PodTraceScheduleReconciler) reconcileTrigger(ctx context.Context, sch *podtracev1alpha1.PodTraceSchedule, active, succeeded, failed []podtracev1alpha1.PodTraceSession, now time.Time) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	tr := sch.Spec.Trigger
	cooldown := triggerCooldown(tr)

	// GC first; its guard never removes a session while it is the sole
	// recovery evidence for an occurrence whose receipt is not yet
	// persisted and can still be replayed.
	if err := r.applyHistoryLimits(ctx, sch, succeeded, failed); err != nil {
		return ctrl.Result{}, err
	}

	// Single ledger: receipts recovered from every existing child session
	// merged with the persisted firing log. Building it before any other
	// step ensures a firing lost from status is recovered on this pass even
	// when a later branch returns early.
	ledger := buildTriggerLedger(
		append(append(append([]podtracev1alpha1.PodTraceSession{}, active...), succeeded...), failed...),
		triggerFirings(sch))

	namespaces, deniedNamespaces, err := r.triggerNamespaces(ctx, sch)
	if err != nil {
		r.setCondition(sch, ConditionDegraded, metav1.ConditionTrue, "NamespaceSelectorInvalid", err.Error())
		return ctrl.Result{RequeueAfter: triggerResyncInterval}, r.finishTriggerStatus(ctx, sch, active, ledger, now, cooldown)
	}
	if len(deniedNamespaces) > 0 {
		r.setCondition(sch, ConditionDegraded, metav1.ConditionFalse, "SomeNamespacesDenied",
			crossNamespaceDeniedMessage(sch.Namespace, deniedNamespaces))
	} else {
		r.setCondition(sch, ConditionDegraded, metav1.ConditionFalse, "Reconciled", "")
	}

	events, err := r.listAlertEvents(ctx, namespaces)
	if err != nil {
		return ctrl.Result{}, err
	}

	eligible, err := r.filterEligibleEvents(ctx, sch, events)
	if err != nil {
		return ctrl.Result{}, err
	}
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].At.Before(eligible[j].At) })

	switch triggerConcurrency(tr) {
	case podtracev1alpha1.ForbidConcurrent:
		if len(active) > 0 {
			r.setCondition(sch, ConditionReconciled, metav1.ConditionTrue, "Forbidden",
				fmt.Sprintf("%d active triggered session(s); not firing", len(active)))
			return ctrl.Result{RequeueAfter: triggerResyncInterval}, r.finishTriggerStatus(ctx, sch, active, ledger, now, cooldown)
		}
	case podtracev1alpha1.ReplaceConcurrent:
		// The active sessions are evidence for the occurrences they
		// consumed. Persist their receipts into the ledger BEFORE deleting
		// them, so replays stay rejected and their firings keep counting
		// toward cooldown and the rate window after replacement.
		sch.Status.Trigger = &podtracev1alpha1.TriggerStatus{RecentFirings: ledger}
		if err := r.patchStatus(ctx, sch); err != nil {
			return ctrl.Result{}, err
		}
		for i := range active {
			if err := r.Delete(ctx, &active[i]); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("replace: delete active session %s: %w", active[i].Name, err)
			}
		}
		active = nil
	}

	maxPerHour := triggerMaxPerHour(tr)
	remaining := int(maxPerHour) - countFiringsInWindow(ledger, now, triggerRateWindow)
	firedThisPass := 0
	rateLimited := false

	for _, ev := range eligible {
		// Already consumed (child session, persisted firing, or both):
		// associate with the original session; never create or count again.
		if firingByKey(ledger, ev.Key) != nil {
			continue
		}
		if remaining <= 0 {
			rateLimited = true
			break
		}
		if !eligibleToFire(lastFiringForPod(ledger, ev.Namespace, ev.PodName), ev.At, now, cooldown) {
			continue
		}
		session, created, err := r.ensureTriggeredSession(ctx, sch, ev)
		if err != nil {
			if apierrors.IsForbidden(err) && strings.Contains(err.Error(), "being terminated") {
				logger.Info("namespace terminating; stopping trigger reconcile", "err", err)
				return ctrl.Result{}, nil
			}
			r.setCondition(sch, ConditionDegraded, metav1.ConditionTrue, "CreateSession", err.Error())
			return ctrl.Result{}, err
		}
		// Only the API-confirmed creator records the firing: a concurrent
		// reconciler that lost the create race (AlreadyExists surfaced as
		// an unchanged pre-existing session) just associates with it.
		if !created {
			continue
		}
		ledger = append(ledger, podtracev1alpha1.TriggerFiring{
			OccurrenceKey: ev.Key,
			PodName:       ev.PodName,
			Namespace:     ev.Namespace,
			Time:          metav1.NewTime(now),
			SessionName:   session,
		})
		remaining--
		firedThisPass++
		logger.Info("fired triggered session", "session", session, "pod", ev.Namespace+"/"+ev.PodName,
			"kind", ev.Kind, "severity", ev.Severity)
	}

	sch.Status.Trigger = &podtracev1alpha1.TriggerStatus{RecentFirings: pruneFirings(ledger, now, cooldown)}

	switch {
	case rateLimited:
		r.setCondition(sch, ConditionReconciled, metav1.ConditionTrue, "RateLimited",
			fmt.Sprintf("maxSessionsPerHour=%d reached; %d fired this pass", maxPerHour, firedThisPass))
	case firedThisPass > 0:
		r.setCondition(sch, ConditionReconciled, metav1.ConditionTrue, "Triggered",
			fmt.Sprintf("fired %d session(s) from matching alerts", firedThisPass))
	default:
		r.setCondition(sch, ConditionReconciled, metav1.ConditionTrue, "Watching",
			"no matching alert eligible to fire")
	}

	return ctrl.Result{RequeueAfter: triggerResyncInterval}, r.finishTriggerStatus(ctx, sch, active, ledger, now, cooldown)
}

// finishTriggerStatus refreshes active-session status and persists the
// merged firing ledger.
func (r *PodTraceScheduleReconciler) finishTriggerStatus(ctx context.Context, sch *podtracev1alpha1.PodTraceSchedule, active []podtracev1alpha1.PodTraceSession, ledger []podtracev1alpha1.TriggerFiring, now time.Time, cooldown time.Duration) error {
	r.refreshStatus(sch, active, nil)
	sch.Status.Trigger = &podtracev1alpha1.TriggerStatus{RecentFirings: pruneFirings(ledger, now, cooldown)}
	return r.patchStatus(ctx, sch)
}

func triggerFirings(sch *podtracev1alpha1.PodTraceSchedule) []podtracev1alpha1.TriggerFiring {
	if sch.Status.Trigger == nil {
		return nil
	}
	return sch.Status.Trigger.RecentFirings
}

// triggerNamespaces resolves the namespaces whose alert Events this trigger
// may consume: the schedule's own namespace, plus any namespace its
// NamespaceSelector matches that consents via the tenancy annotation.
func (r *PodTraceScheduleReconciler) triggerNamespaces(ctx context.Context, sch *podtracev1alpha1.PodTraceSchedule) (allowed, denied []string, err error) {
	if sch.Spec.Trigger.NamespaceSelector == nil {
		return []string{sch.Namespace}, nil, nil
	}
	allowed, denied, err = ResolveNamespaceSelector(ctx, r.Client, sch.Spec.Trigger.NamespaceSelector, sch.Namespace)
	if err != nil {
		return nil, nil, err
	}
	if !containsString(allowed, sch.Namespace) {
		allowed = append(allowed, sch.Namespace)
	}
	return allowed, denied, nil
}

// listAlertEvents returns podtrace alert Events across the given namespaces.
// This list is the durable backstop: even if a watch delivery is dropped, a
// missed alert is caught here on the next resync while the Event lives in
// etcd.
func (r *PodTraceScheduleReconciler) listAlertEvents(ctx context.Context, namespaces []string) ([]alertEvent, error) {
	seen := map[string]struct{}{}
	var out []alertEvent
	for _, ns := range namespaces {
		var list corev1.EventList
		if err := r.List(ctx, &list, client.InNamespace(ns)); err != nil {
			return nil, fmt.Errorf("list events in %s: %w", ns, err)
		}
		for i := range list.Items {
			parsed, ok := parseAlertEvent(&list.Items[i])
			if !ok {
				continue
			}
			if _, dup := seen[parsed.Key]; dup {
				continue
			}
			seen[parsed.Key] = struct{}{}
			out = append(out, parsed)
		}
	}
	return out, nil
}

// filterEligibleEvents keeps only events matching the trigger's sources and
// (when set) its pod Selector.
func (r *PodTraceScheduleReconciler) filterEligibleEvents(ctx context.Context, sch *podtracev1alpha1.PodTraceSchedule, events []alertEvent) ([]alertEvent, error) {
	tr := sch.Spec.Trigger
	var sel labels.Selector
	if tr.Selector != nil {
		s, err := metav1.LabelSelectorAsSelector(tr.Selector)
		if err != nil {
			return nil, fmt.Errorf("spec.trigger.selector: %w", err)
		}
		sel = s
	}
	out := make([]alertEvent, 0, len(events))
	for _, ev := range events {
		if !matchesTriggerSources(ev, tr.Sources) {
			continue
		}
		if sel != nil {
			var pod corev1.Pod
			if err := r.Get(ctx, client.ObjectKey{Namespace: ev.Namespace, Name: ev.PodName}, &pod); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return nil, fmt.Errorf("get pod %s/%s for selector match: %w", ev.Namespace, ev.PodName, err)
			}
			if !sel.Matches(labels.Set(pod.Labels)) {
				continue
			}
		}
		out = append(out, ev)
	}
	return out, nil
}

// ensureTriggeredSession creates, idempotently and deterministically named,
// a PodTraceSession that traces the alerting pod. The trigger receipt is
// written only at creation. The boolean reports whether THIS call was the
// API-confirmed creator; a concurrent reconciler that raced on the same
// deterministic name gets false and must not count the firing.
func (r *PodTraceScheduleReconciler) ensureTriggeredSession(ctx context.Context, sch *podtracev1alpha1.PodTraceSchedule, ev alertEvent) (string, bool, error) {
	name := triggeredSessionName(sch.Name, ev.Key)
	session := &podtracev1alpha1.PodTraceSession{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: sch.Namespace},
	}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, session, func() error {
		if session.CreationTimestamp.IsZero() {
			session.Spec = *sch.Spec.SessionTemplate.Spec.DeepCopy()
			session.Spec.Selector = nil
			session.Spec.NamespaceSelector = nil
			session.Spec.PodRefs = []podtracev1alpha1.PodRef{{Namespace: ev.Namespace, Name: ev.PodName}}
		}
		labelsMap := map[string]string{}
		for k, v := range sch.Spec.SessionTemplate.Metadata.Labels {
			labelsMap[k] = v
		}
		labelsMap[LabelManagedBy] = ManagedByValue
		labelsMap[LabelComponent] = ComponentSession
		labelsMap["podtrace.io/schedule"] = sch.Name
		session.Labels = mergeLabels(session.Labels, labelsMap)

		anns := map[string]string{}
		for k, v := range sch.Spec.SessionTemplate.Metadata.Annotations {
			anns[k] = v
		}
		// Receipt fields identify the exact occurrence; stamp them once,
		// at creation, so later reconciles never overwrite recovery data.
		if session.CreationTimestamp.IsZero() {
			anns[AnnotationTriggerEventKey] = ev.Key
			anns[annotationTriggeredBy] = string(ev.Kind)
			anns[annotationTriggerSeverity] = ev.Severity
			anns[annotationTriggerPod] = ev.Namespace + "/" + ev.PodName
			anns[annotationTriggeredAt] = ev.At.UTC().Format(time.RFC3339)
		}
		session.Annotations = mergeLabels(session.Annotations, anns)

		return controllerutil.SetControllerReference(sch, session, r.Scheme)
	})
	if err != nil {
		return "", false, fmt.Errorf("ensure triggered session for %s/%s: %w", ev.Namespace, ev.PodName, err)
	}
	return name, op == controllerutil.OperationResultCreated, nil
}

// triggeredSessionName is deterministic per (schedule, occurrence key) so a
// re-reconcile of the same Event is idempotent even if its LastTimestamp or
// ResourceVersion changed, and bounded to 63 chars.
func triggeredSessionName(scheduleName, occurrenceKey string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(occurrenceKey))
	suffix := fmt.Sprintf("-%08x", h.Sum32())
	head := scheduleName
	if len(head)+len(suffix) > 63 {
		head = head[:63-len(suffix)]
	}
	return head + suffix
}

func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
