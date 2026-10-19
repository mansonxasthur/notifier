package sender

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// countingInbox counts the batch writes that reach the store.
type countingInbox struct {
	ports.InboxStore
	settles atomic.Int32
}

func (c *countingInbox) Settle(ctx context.Context, o []domain.InboxOutcome) ([]bool, error) {
	c.settles.Add(1)
	return c.InboxStore.Settle(ctx, o)
}

// scriptedProvider answers each delivery with the error scripted for its ID.
type scriptedProvider struct{ errs map[int64]error }

func (p scriptedProvider) Name() string { return "sms-a" }
func (p scriptedProvider) Send(_ context.Context, r ports.SendRequest) error {
	return p.errs[r.DeliveryID]
}

// TestBatchedAttemptsKeepEachDeliveryApart sends six deliveries that share one load and one
// settle batch: one is accepted, one fails for good, one is parked for a retry, and the rest
// are accepted. Each must end as it would alone.
func TestBatchedAttemptsKeepEachDeliveryApart(t *testing.T) {
	ctx := context.Background()
	db := postgres.New(pgtest.NewMigratedDB(t))
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Stop(ctx) })

	n := &domain.Notification{
		IdempotencyKey: uuid.NewString(), Type: "match_alert",
		Content: map[domain.Channel]domain.Content{domain.ChannelSMS: {Text: "مرحبا"}},
	}
	const total = 6
	for i := range total {
		n.Deliveries = append(n.Deliveries, domain.Delivery{
			RecipientID: string(rune('a' + i)), Channel: domain.ChannelSMS, Contact: "+201000000000",
			ExpiresAt: time.Now().Add(time.Hour),
		})
	}
	if _, err := postgres.NewNotificationStore(db, 5000).Create(ctx, n); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Pool().Query(ctx, `SELECT id FROM deliveries ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()

	inbox := &countingInbox{InboxStore: postgres.NewInboxStore(db)}
	claims, err := inbox.ClaimMany(ctx, ids, "sms-a", domain.Claims(5))
	if err != nil {
		t.Fatal(err)
	}

	prov := scriptedProvider{errs: map[int64]error{
		ids[1]: &ports.SendError{Class: domain.FailureMessage, Err: errors.New("status 400")},
		ids[2]: &ports.SendError{Class: domain.FailureProvider, Err: errors.New("status 503")},
	}}
	retry := domain.RetryPolicy{MaxRetries: 1, Backoff: domain.Backoff{Initial: 2 * time.Second, Max: time.Minute, Multiplier: 2}}
	attempt := NewAttempt(postgres.NewDeliveryStore(db), inbox, providers{"sms-a": prov},
		&fakeChecker{res: ports.CheckNotFound}, NewBreakers(domain.BreakerPolicy{FailureThreshold: 100, OpenFor: time.Minute}, time.Now), nil,
		Routing{
			Order:    map[domain.Channel][]string{domain.ChannelSMS: {"sms-a", "sms-b"}},
			Policies: map[string]domain.RetryPolicy{"sms-a": retry},
		}, slog.New(slog.DiscardHandler))
	// The linger is far longer than the test, so only a full batch flushes: the six attempts
	// must all reach each stage together.
	attempt.WithBatching(total, time.Hour)

	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Go(func() {
			c := Claimed{DeliveryID: id, Provider: "sms-a", ClaimCount: claims[i].ClaimCount}
			if err := attempt.Send(ctx, c); err != nil {
				t.Errorf("Send(%d): %v", id, err)
			}
		})
	}
	wg.Wait()

	if got := inbox.settles.Load(); got != 1 {
		t.Errorf("settle calls = %d, want the six outcomes in one", got)
	}
	state := func(id int64) (inboxState, deliveryState string, retries int) {
		t.Helper()
		err := db.Pool().QueryRow(ctx, `SELECT i.state, d.state, i.retries FROM inbox i JOIN deliveries d ON d.id = i.delivery_id WHERE i.delivery_id = $1`, id).
			Scan(&inboxState, &deliveryState, &retries)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	for i, id := range ids {
		is, ds, retries := state(id)
		switch i {
		case 1:
			if is != "DEAD" || ds != "FAILED" {
				t.Errorf("rejected delivery = %s/%s, want DEAD/FAILED", is, ds)
			}
		case 2:
			if is != "WAITING_RETRY" || ds != "PENDING" || retries != 1 {
				t.Errorf("failed delivery = %s/%s retries %d, want WAITING_RETRY/PENDING with one retry spent", is, ds, retries)
			}
		default:
			if is != "DONE" || ds != "ACCEPTED" {
				t.Errorf("delivery %d = %s/%s, want DONE/ACCEPTED", i, is, ds)
			}
		}
	}
	attempt.Close()
}
