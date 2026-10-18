package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

func TestDeliveryLoad(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 1)
	store, id := postgres.NewDeliveryStore(db), deliveryID(t, db)

	d, err := store.Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != id || d.Channel != domain.ChannelSMS || d.Contact != "+201000000000" || d.State != domain.DeliveryPending {
		t.Errorf("delivery = %+v", d)
	}
	if d.Content.Text != "مرحبا" {
		t.Errorf("content = %+v, want the Arabic text intact", d.Content)
	}
	if !d.ExpiresAt.Equal(expiry) {
		t.Errorf("expires_at = %v, want %v", d.ExpiresAt, expiry)
	}

	if _, err := store.Load(ctx, id+1000); !errors.Is(err, ports.ErrDeliveryNotFound) {
		t.Errorf("unknown delivery: err = %v, want ErrDeliveryNotFound", err)
	}
}

func state(t *testing.T, db *postgres.Pool, query string, id int64) string {
	t.Helper()
	var s *string
	if err := db.Pool().QueryRow(context.Background(), query, id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	if s == nil {
		return ""
	}
	return *s
}

const (
	deliveryState  = `SELECT state FROM deliveries WHERE id = $1`
	deliveryReason = `SELECT failure_reason FROM deliveries WHERE id = $1`
	inboxState     = `SELECT state FROM inbox WHERE delivery_id = $1`
)

func claimed(t *testing.T, db *postgres.Pool) (*postgres.InboxStore, int64, int) {
	t.Helper()
	store, id := postgres.NewInboxStore(db), deliveryID(t, db)
	c, err := store.Claim(context.Background(), id, "sms-a", 5)
	if err != nil || c.Result != domain.InboxClaimWon {
		t.Fatalf("claim = %+v, %v", c, err)
	}
	return store, id, c.ClaimCount
}

func TestInboxComplete(t *testing.T) {
	ctx := context.Background()

	t.Run("accepts the delivery", func(t *testing.T) {
		db := seed(t, 1)
		store, id, n := claimed(t, db)
		if ok, err := store.Complete(ctx, id, n); err != nil || !ok {
			t.Fatalf("Complete = %v, %v", ok, err)
		}
		if got := state(t, db, inboxState, id); got != "DONE" {
			t.Errorf("inbox = %s, want DONE", got)
		}
		if got := state(t, db, deliveryState, id); got != "ACCEPTED" {
			t.Errorf("delivery = %s, want ACCEPTED", got)
		}
	})

	t.Run("a stale claim changes nothing", func(t *testing.T) {
		db := seed(t, 1)
		store, id, n := claimed(t, db)
		if ok, err := store.Complete(ctx, id, n+1); err != nil || ok {
			t.Fatalf("Complete with a stale claim = %v, %v, want false", ok, err)
		}
		if got := state(t, db, inboxState, id); got != "PROCESSING" {
			t.Errorf("inbox = %s, want PROCESSING", got)
		}
		if got := state(t, db, deliveryState, id); got != "PENDING" {
			t.Errorf("delivery = %s, want PENDING", got)
		}
	})

	t.Run("an early webhook is not undone", func(t *testing.T) {
		db := seed(t, 1)
		store, id, n := claimed(t, db)
		if _, err := db.Pool().Exec(ctx, `UPDATE deliveries SET state = 'DELIVERED' WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		if ok, err := store.Complete(ctx, id, n); err != nil || !ok {
			t.Fatalf("Complete = %v, %v", ok, err)
		}
		if got := state(t, db, deliveryState, id); got != "DELIVERED" {
			t.Errorf("delivery = %s, want DELIVERED to stay", got)
		}
		if got := state(t, db, inboxState, id); got != "DONE" {
			t.Errorf("inbox = %s, want DONE", got)
		}
	})
}

func TestInboxFail(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 1)
	store, id, n := claimed(t, db)

	if ok, err := store.Fail(ctx, id, n+1, domain.FailureMessage, domain.ReasonProviderRejected); err != nil || ok {
		t.Fatalf("Fail with a stale claim = %v, %v, want false", ok, err)
	}
	if ok, err := store.Fail(ctx, id, n, domain.FailureMessage, domain.ReasonProviderRejected); err != nil || !ok {
		t.Fatalf("Fail = %v, %v", ok, err)
	}
	if got := state(t, db, inboxState, id); got != "DEAD" {
		t.Errorf("inbox = %s, want DEAD", got)
	}
	if got := state(t, db, `SELECT failure_class FROM inbox WHERE delivery_id = $1`, id); got != "message" {
		t.Errorf("failure_class = %q, want message", got)
	}
	if got := state(t, db, deliveryState, id); got != "FAILED" {
		t.Errorf("delivery = %s, want FAILED", got)
	}
	if got := state(t, db, deliveryReason, id); got != "PROVIDER_REJECTED" {
		t.Errorf("failure_reason = %q, want PROVIDER_REJECTED", got)
	}
}

func TestInboxPark(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 1)
	store, id, n := claimed(t, db)
	next := time.Now().Add(time.Minute).Truncate(time.Microsecond)

	if ok, err := store.Park(ctx, id, n+1, domain.FailureTimeout, next); err != nil || ok {
		t.Fatalf("Park with a stale claim = %v, %v, want false", ok, err)
	}
	if ok, err := store.Park(ctx, id, n, domain.FailureTimeout, next); err != nil || !ok {
		t.Fatalf("Park = %v, %v", ok, err)
	}
	if got := state(t, db, inboxState, id); got != "WAITING_RETRY" {
		t.Errorf("inbox = %s, want WAITING_RETRY", got)
	}
	if got := state(t, db, `SELECT failure_class FROM inbox WHERE delivery_id = $1`, id); got != "timeout" {
		t.Errorf("failure_class = %q, want timeout", got)
	}
	var retries int
	var due time.Time
	if err := db.Pool().QueryRow(ctx, `SELECT retries, next_attempt_at FROM inbox WHERE delivery_id = $1`, id).Scan(&retries, &due); err != nil {
		t.Fatal(err)
	}
	if retries != 1 || !due.Equal(next) {
		t.Errorf("retries = %d, next_attempt_at = %v; want 1 and %v", retries, due, next)
	}
	if got := state(t, db, deliveryState, id); got != "PENDING" {
		t.Errorf("delivery = %s, want PENDING: a parked attempt is not a failure", got)
	}

	// A redelivered message cannot take the row before it is due: the scan owns the retry.
	if c, err := store.Claim(ctx, id, "sms-a", 5); err != nil || c.Result != domain.InboxClaimDuplicate {
		t.Errorf("claim before due = %+v, %v; want duplicate", c, err)
	}
	if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET next_attempt_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	// Once due, the parked row can be claimed again, and the new claim has a new fencing token.
	c, err := store.Claim(ctx, id, "sms-a", 5)
	if err != nil || c.Result != domain.InboxClaimWon || c.ClaimCount != n+1 {
		t.Errorf("reclaim = %+v, %v; want won with claim_count %d", c, err, n+1)
	}
}

// The sender's own updates only move a delivery out of PENDING, so an UNRESOLVED delivery
// is left as it is.
func TestInboxUpdatesLeaveAnUnresolvedDeliveryAlone(t *testing.T) {
	ctx := context.Background()
	for name, finish := range map[string]func(*postgres.InboxStore, int64, int) (bool, error){
		"Complete": func(s *postgres.InboxStore, id int64, n int) (bool, error) { return s.Complete(ctx, id, n) },
		"Fail": func(s *postgres.InboxStore, id int64, n int) (bool, error) {
			return s.Fail(ctx, id, n, domain.FailureMessage, domain.ReasonProviderRejected)
		},
	} {
		t.Run(name, func(t *testing.T) {
			db := seed(t, 1)
			store, id, n := claimed(t, db)
			if _, err := db.Pool().Exec(ctx, `UPDATE deliveries SET state = 'UNRESOLVED'`); err != nil {
				t.Fatal(err)
			}
			if _, err := finish(store, id, n); err != nil {
				t.Fatal(err)
			}
			if got := state(t, db, deliveryState, id); got != "UNRESOLVED" {
				t.Errorf("delivery = %s, want UNRESOLVED left alone", got)
			}
		})
	}
}

func TestWebhookApply(t *testing.T) {
	ctx := context.Background()
	event := func(id int64, outcome domain.DeliveryState, reason domain.FailureReason) ports.WebhookEvent {
		return ports.WebhookEvent{
			DeliveryID: id, Provider: "sms-a", Channel: domain.ChannelSMS, EventType: string(outcome),
			Payload: []byte(`{"delivery_id":1,"note":"مرحبا"}`), Outcome: outcome, Reason: reason,
		}
	}
	applied := func(t *testing.T, db *postgres.Pool, id int64) []bool {
		t.Helper()
		rows, err := db.Pool().Query(ctx, `SELECT applied FROM webhook_events WHERE delivery_id = $1 ORDER BY id`, id)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []bool
		for rows.Next() {
			var a bool
			if err := rows.Scan(&a); err != nil {
				t.Fatal(err)
			}
			out = append(out, a)
		}
		return out
	}

	t.Run("an early webhook moves PENDING straight to DELIVERED", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewWebhookStore(db), deliveryID(t, db)
		if ok, err := store.Apply(ctx, event(id, domain.DeliveryDelivered, "")); err != nil || !ok {
			t.Fatalf("Apply = %v, %v", ok, err)
		}
		if got := state(t, db, deliveryState, id); got != "DELIVERED" {
			t.Errorf("delivery = %s, want DELIVERED", got)
		}
		if got := state(t, db, deliveryReason, id); got != "" {
			t.Errorf("failure_reason = %q, want none", got)
		}
	})

	t.Run("duplicates and later contradictions are stored and ignored", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewWebhookStore(db), deliveryID(t, db)
		if _, err := db.Pool().Exec(ctx, `UPDATE deliveries SET state = 'ACCEPTED'`); err != nil {
			t.Fatal(err)
		}

		first, err := store.Apply(ctx, event(id, domain.DeliveryFailed, domain.ReasonBounced))
		if err != nil || !first {
			t.Fatalf("first = %v, %v", first, err)
		}
		dup, err := store.Apply(ctx, event(id, domain.DeliveryFailed, domain.ReasonBounced))
		if err != nil || dup {
			t.Fatalf("duplicate = %v, %v, want ignored", dup, err)
		}
		late, err := store.Apply(ctx, event(id, domain.DeliveryDelivered, ""))
		if err != nil || late {
			t.Fatalf("contradiction = %v, %v, want ignored: the first terminal webhook wins", late, err)
		}

		if got := state(t, db, deliveryState, id); got != "FAILED" {
			t.Errorf("delivery = %s, want FAILED", got)
		}
		if got := state(t, db, deliveryReason, id); got != "BOUNCED" {
			t.Errorf("failure_reason = %q, want BOUNCED", got)
		}
		got := applied(t, db, id)
		if len(got) != 3 || !got[0] || got[1] || got[2] {
			t.Errorf("applied flags = %v, want [true false false]: every event is kept", got)
		}
	})

	t.Run("a late webhook settles UNRESOLVED, and only that way round", func(t *testing.T) {
		for _, tt := range []struct {
			outcome domain.DeliveryState
			reason  domain.FailureReason
			want    string
		}{
			{domain.DeliveryDelivered, "", "DELIVERED"},
			{domain.DeliveryFailed, domain.ReasonBounced, "FAILED"},
		} {
			db := seed(t, 1)
			store, id := postgres.NewWebhookStore(db), deliveryID(t, db)
			if _, err := db.Pool().Exec(ctx, `UPDATE deliveries SET state = 'UNRESOLVED'`); err != nil {
				t.Fatal(err)
			}

			ok, err := store.Apply(ctx, event(id, tt.outcome, tt.reason))
			if err != nil || !ok {
				t.Fatalf("%s: Apply = %v, %v, want it to settle UNRESOLVED", tt.want, ok, err)
			}
			if got := state(t, db, deliveryState, id); got != tt.want {
				t.Errorf("delivery = %s, want %s", got, tt.want)
			}
			if got := state(t, db, deliveryReason, id); got != string(tt.reason) {
				t.Errorf("failure_reason = %q, want %q", got, tt.reason)
			}

			// What settled it is final: the opposite report is stored and ignored.
			opposite := domain.DeliveryDelivered
			if tt.outcome == domain.DeliveryDelivered {
				opposite = domain.DeliveryFailed
			}
			if ok, err := store.Apply(ctx, event(id, opposite, domain.ReasonBounced)); err != nil || ok {
				t.Errorf("after %s: opposite Apply = %v, %v, want ignored", tt.want, ok, err)
			}
			if got := state(t, db, deliveryState, id); got != tt.want {
				t.Errorf("delivery = %s after a contradiction, want %s", got, tt.want)
			}
		}
	})

	t.Run("only a failure keeps a reason", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewWebhookStore(db), deliveryID(t, db)
		if _, err := store.Apply(ctx, event(id, domain.DeliveryDelivered, domain.ReasonBounced)); err != nil {
			t.Fatal(err)
		}
		if got := state(t, db, deliveryReason, id); got != "" {
			t.Errorf("failure_reason = %q on a DELIVERED delivery, want none", got)
		}
	})

	t.Run("the payload is stored as received", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewWebhookStore(db), deliveryID(t, db)
		if _, err := store.Apply(ctx, event(id, domain.DeliveryDelivered, "")); err != nil {
			t.Fatal(err)
		}
		if got := state(t, db, `SELECT payload ->> 'note' FROM webhook_events WHERE delivery_id = $1`, id); got != "مرحبا" {
			t.Errorf("payload note = %q", got)
		}
	})

	t.Run("an unknown delivery is reported and nothing is stored", func(t *testing.T) {
		db := seed(t, 1)
		store := postgres.NewWebhookStore(db)
		if _, err := store.Apply(ctx, event(999999, domain.DeliveryDelivered, "")); !errors.Is(err, ports.ErrDeliveryNotFound) {
			t.Errorf("err = %v, want ErrDeliveryNotFound", err)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM webhook_events`); got != 0 {
			t.Errorf("webhook_events = %d, want 0", got)
		}
	})

	t.Run("a provider of another channel cannot settle the delivery", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewWebhookStore(db), deliveryID(t, db)
		e := event(id, domain.DeliveryDelivered, "")
		e.Provider, e.Channel = "email-a", domain.ChannelEmail
		if _, err := store.Apply(ctx, e); !errors.Is(err, ports.ErrDeliveryNotFound) {
			t.Errorf("err = %v, want ErrDeliveryNotFound", err)
		}
		if got := state(t, db, deliveryState, id); got != "PENDING" {
			t.Errorf("delivery = %s, want PENDING", got)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM webhook_events`); got != 0 {
			t.Errorf("webhook_events = %d, want 0", got)
		}
	})

	t.Run("an outcome a delivery cannot move into is refused", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewWebhookStore(db), deliveryID(t, db)
		if _, err := store.Apply(ctx, event(id, domain.DeliveryPending, "")); err == nil {
			t.Error("want an error for PENDING")
		}
	})
}
