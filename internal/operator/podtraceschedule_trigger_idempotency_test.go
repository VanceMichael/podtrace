package operator

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	podtracev1alpha1 "github.com/gma1k/podtrace/api/v1alpha1"
)

// receiptSession builds an owned session carrying the trigger receipt for the
// given occurrence key ("uid:..." / "composite:...").
func receiptSession(name string, sch *podtracev1alpha1.PodTraceSchedule, key, podRef string, createdAt time.Time, state podtracev1alpha1.SessionState) podtracev1alpha1.PodTraceSession {
	controller := true
	session := podtracev1alpha1.PodTraceSession{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         sch.Namespace,
			CreationTimestamp: metav1.NewTime(createdAt),
			Annotations: map[string]string{
				AnnotationTriggerEventKey: key,
				annotationTriggerPod:      podRef,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: podtracev1alpha1.GroupVersion.String(),
				Kind:       "PodTraceSchedule",
				Name:       sch.Name,
				UID:        sch.UID,
				Controller: &controller,
			}},
		},
		Status: podtracev1alpha1.PodTraceSessionStatus{State: state},
	}
	if state != "" {
		ct := metav1.NewTime(createdAt.Add(10 * time.Second))
		session.Status.CompletionTime = &ct
	}
	return session
}

func countSessions(t *testing.T, r *PodTraceScheduleReconciler, namespace string) int {
	t.Helper()
	var list podtracev1alpha1.PodTraceSessionList
	if err := r.List(context.Background(), &list, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	return len(list.Items)
}

// The same Event object, re-seen after a LastTimestamp/ResourceVersion update
// (watch replay, list fallback), must keep associating with the one session it
// originally consumed and never produce a second firing.
func TestReconcileTrigger_ReplayedEventAssociatesOriginalSession(t *testing.T) {
	now := time.Now()
	sch := triggerScheduleWithTemplate("fr", "obs", nil)
	event := storedAlertEvent("obs", "hog-a", "uid-a", now.Add(-time.Minute))
	r := reconcileTriggerFixture(t, sch, interceptor.Funcs{}, event)

	if _, err := r.reconcileTrigger(context.Background(), sch, nil, nil, nil, now); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if n := countSessions(t, r, "obs"); n != 1 {
		t.Fatalf("first pass: want 1 session, got %d", n)
	}
	firstFiring := sch.Status.Trigger.RecentFirings
	if len(firstFiring) != 1 || firstFiring[0].OccurrenceKey != "uid:uid-a" {
		t.Fatalf("firing receipt = %+v, want key uid:uid-a", firstFiring)
	}

	// Simulate the same Event object being delivered again with a mutated
	// LastTimestamp and a new ResourceVersion.
	var got corev1.Event
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "obs", Name: event.Name}, &got); err != nil {
		t.Fatalf("get event: %v", err)
	}
	got.LastTimestamp = metav1.NewTime(now.Add(30 * time.Second))
	got.Count++
	if err := r.Update(context.Background(), &got); err != nil {
		t.Fatalf("update event: %v", err)
	}

	if _, err := r.reconcileTrigger(context.Background(), sch, nil, nil, nil, now); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if n := countSessions(t, r, "obs"); n != 1 {
		t.Errorf("replayed Event must not create a second session: got %d", n)
	}
	firings := sch.Status.Trigger.RecentFirings
	if len(firings) != 1 {
		t.Fatalf("replay must not append a firing: got %+v", firings)
	}
	if !firings[0].Time.Equal(&firstFiring[0].Time) {
		t.Errorf("replay must not move the firing time: got %v, want %v", firings[0].Time, firstFiring[0].Time)
	}
}

// Session created, then the schedule status write was lost (error or process
// exit): recovery must merge the session's receipt back into the ledger, count
// it exactly once, and never create or deduct again.
func TestReconcileTrigger_RecoversFiringFromSessionReceipt(t *testing.T) {
	now := time.Now()
	sch := triggerScheduleWithTemplate("fr", "obs", nil)

	createdAt := now.Add(-time.Minute)
	existing := receiptSession(triggeredSessionName("fr", "uid:uid-a"), sch,
		"uid:uid-a", "obs/hog-a", createdAt, "") // still active

	r := reconcileTriggerFixture(t, sch, interceptor.Funcs{}, &existing,
		storedAlertEvent("obs", "hog-a", "uid-a", now.Add(-time.Minute)))

	if _, err := r.reconcileTrigger(context.Background(), sch,
		[]podtracev1alpha1.PodTraceSession{existing}, nil, nil, now); err != nil {
		t.Fatalf("recovery reconcile: %v", err)
	}
	if n := countSessions(t, r, "obs"); n != 1 {
		t.Fatalf("recovery must not duplicate the session: got %d", n)
	}
	firings := sch.Status.Trigger.RecentFirings
	if len(firings) != 1 {
		t.Fatalf("recovery must record the missed firing once: got %+v", firings)
	}
	wantTime := metav1.NewTime(createdAt)
	if !firings[0].Time.Equal(&wantTime) {
		t.Errorf("recovered firing time = %v, want original creation time %v", firings[0].Time, createdAt)
	}

	// A second pass must keep a single ledger entry — no repeated deduction.
	if _, err := r.reconcileTrigger(context.Background(), sch,
		[]podtracev1alpha1.PodTraceSession{existing}, nil, nil, now); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if len(sch.Status.Trigger.RecentFirings) != 1 {
		t.Errorf("second pass must keep one firing, got %+v", sch.Status.Trigger.RecentFirings)
	}
}

// The recovered firing keeps occupying the rate budget: another matching
// occurrence must be rate-limited rather than consuming a second slot.
func TestReconcileTrigger_RecoveredFiringStillHoldsRateBudget(t *testing.T) {
	now := time.Now()
	maxPerHour := int32(1)
	sch := triggerScheduleWithTemplate("fr", "obs", func(s *podtracev1alpha1.PodTraceSchedule) {
		s.Spec.Trigger.MaxSessionsPerHour = &maxPerHour
		s.Spec.Trigger.ConcurrencyPolicy = podtracev1alpha1.AllowConcurrent
	})

	createdAt := now.Add(-time.Minute)
	existing := receiptSession(triggeredSessionName("fr", "uid:uid-a"), sch,
		"uid:uid-a", "obs/hog-a", createdAt, "")

	r := reconcileTriggerFixture(t, sch, interceptor.Funcs{}, &existing,
		storedAlertEvent("obs", "hog-a", "uid-a", now.Add(-time.Minute)),
		storedAlertEvent("obs", "hog-b", "uid-b", now.Add(-30*time.Second)))

	if _, err := r.reconcileTrigger(context.Background(), sch,
		[]podtracev1alpha1.PodTraceSession{existing}, nil, nil, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n := countSessions(t, r, "obs"); n != 1 {
		t.Errorf("rate budget recovered from receipt must block the second occurrence: got %d sessions", n)
	}
	cond := findCondition(sch.Status.Conditions, ConditionReconciled)
	if cond == nil || cond.Reason != "RateLimited" {
		t.Errorf("want RateLimited, got %+v", cond)
	}
}

// A genuinely new occurrence from the same alert source after the cooldown
// elapses must fire a new session, even though earlier occurrences remain in
// the ledger.
func TestReconcileTrigger_NewOccurrenceAfterCooldownFires(t *testing.T) {
	t0 := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	cd := metav1.Duration{Duration: 10 * time.Minute}
	sch := triggerScheduleWithTemplate("fr", "obs", func(s *podtracev1alpha1.PodTraceSchedule) {
		s.Spec.Trigger.Cooldown = &cd
		s.Spec.Trigger.ConcurrencyPolicy = podtracev1alpha1.AllowConcurrent
	})

	event1 := storedAlertEvent("obs", "hog", "uid-a", t0)
	event1.Name = "alert-hog-a"
	event2 := storedAlertEvent("obs", "hog", "uid-b", t0.Add(15*time.Minute))
	event2.Name = "alert-hog-b"
	r := reconcileTriggerFixture(t, sch, interceptor.Funcs{}, event1, event2)

	clock := t0
	r.nowFn = func() time.Time { return clock }

	if _, err := r.reconcileTrigger(context.Background(), sch, nil, nil, nil, clock); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if n := countSessions(t, r, "obs"); n != 1 {
		t.Fatalf("pass 1: want 1 session, got %d", n)
	}

	clock = t0.Add(15 * time.Minute)
	if _, err := r.reconcileTrigger(context.Background(), sch, nil, nil, nil, clock); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if n := countSessions(t, r, "obs"); n != 2 {
		t.Errorf("new occurrence after cooldown must fire a second session: got %d", n)
	}
}

// A failed session create must not leave any firing record that occupies
// cooldown or the rate budget.
func TestReconcileTrigger_CreateFailureLeavesNoGhostFiring(t *testing.T) {
	now := time.Now()
	sch := triggerScheduleWithTemplate("fr", "obs", nil)
	r := reconcileTriggerFixture(t, sch, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*podtracev1alpha1.PodTraceSession); ok {
				return errors.New("admission webhook rejected")
			}
			return cl.Create(ctx, obj, opts...)
		},
	}, storedAlertEvent("obs", "hog-a", "uid-a", now.Add(-time.Minute)))

	if _, err := r.reconcileTrigger(context.Background(), sch, nil, nil, nil, now); err == nil {
		t.Fatal("a failed create must surface as an error")
	}
	if sch.Status.Trigger != nil && len(sch.Status.Trigger.RecentFirings) != 0 {
		t.Errorf("a failed create must not record a firing: %+v", sch.Status.Trigger.RecentFirings)
	}
	if firingByKey(triggerFirings(sch), "uid:uid-a") != nil {
		t.Error("the unconsumed occurrence must not appear in the ledger")
	}
}

// With a history limit of zero, GC must not delete a completed session while
// its trigger receipt is the sole recovery evidence; once the ledger persists
// the receipt, the next pass removes it.
func TestReconcileTrigger_GCWaitsForReceiptPersistence(t *testing.T) {
	now := time.Now()
	limit := int32(0)
	sch := triggerScheduleWithTemplate("fr", "obs", func(s *podtracev1alpha1.PodTraceSchedule) {
		s.Spec.SuccessfulSessionsHistoryLimit = &limit
	})

	createdAt := now.Add(-30 * time.Second)
	completed := receiptSession(triggeredSessionName("fr", "uid:uid-a"), sch,
		"uid:uid-a", "obs/hog-a", createdAt, podtracev1alpha1.SessionStateCompleted)
	r := reconcileTriggerFixture(t, sch, interceptor.Funcs{}, &completed,
		storedAlertEvent("obs", "hog-a", "uid-a", now.Add(-time.Minute)))

	// Pass 1: receipt not persisted yet → GC must skip the sole evidence.
	if _, err := r.reconcileTrigger(context.Background(), sch, nil,
		[]podtracev1alpha1.PodTraceSession{completed}, nil, now); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	var check podtracev1alpha1.PodTraceSession
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "obs", Name: completed.Name}, &check); err != nil {
		t.Fatalf("sole evidence must survive pass 1: %v", err)
	}
	if firingByKey(triggerFirings(sch), "uid:uid-a") == nil {
		t.Fatal("pass 1 must persist the receipt into the ledger")
	}

	// Pass 2: receipt is durable → GC proceeds under keep=0.
	if _, err := r.reconcileTrigger(context.Background(), sch, nil,
		[]podtracev1alpha1.PodTraceSession{completed}, nil, now); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	err := r.Get(context.Background(), client.ObjectKey{Namespace: "obs", Name: completed.Name}, &check)
	if err == nil {
		t.Error("after the receipt persisted, keep=0 must delete the session")
	}
}

// Forbid must still persist the ledger recovered from active receipts, so a
// firing lost from status is not missed just because the trigger forbids new
// runs.
func TestReconcileTrigger_ForbidPersistsRecoveredLedger(t *testing.T) {
	now := time.Now()
	sch := triggerScheduleWithTemplate("fr", "obs", func(s *podtracev1alpha1.PodTraceSchedule) {
		s.Spec.Trigger.ConcurrencyPolicy = podtracev1alpha1.ForbidConcurrent
	})

	createdAt := now.Add(-time.Minute)
	active := receiptSession(triggeredSessionName("fr", "uid:uid-a"), sch,
		"uid:uid-a", "obs/hog-a", createdAt, "")
	r := reconcileTriggerFixture(t, sch, interceptor.Funcs{}, &active,
		storedAlertEvent("obs", "hog-a", "uid-a", now.Add(-time.Minute)))

	if _, err := r.reconcileTrigger(context.Background(), sch,
		[]podtracev1alpha1.PodTraceSession{active}, nil, nil, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n := countSessions(t, r, "obs"); n != 1 {
		t.Errorf("Forbid must keep the existing session only, got %d", n)
	}
	firings := sch.Status.Trigger.RecentFirings
	if len(firings) != 1 || firings[0].OccurrenceKey != "uid:uid-a" {
		t.Errorf("Forbid must persist the recovered receipt: %+v", firings)
	}
	cond := findCondition(sch.Status.Conditions, ConditionReconciled)
	if cond == nil || cond.Reason != "Forbidden" {
		t.Errorf("want Forbidden, got %+v", cond)
	}
}
