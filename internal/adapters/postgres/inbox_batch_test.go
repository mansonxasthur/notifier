package postgres_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/domain"
)

func deliveryIDs(t *testing.T, db *postgres.Pool) []int64 {
	t.Helper()
	rows, err := db.Pool().Query(context.Background(), `SELECT id FROM deliveries ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestInboxClaimMany(t *testing.T) {
	ctx := context.Background()
	limits := domain.Claims(2)

	t.Run("each id gets the result a single claim would give", func(t *testing.T) {
		db := seed(t, 6)
		store := postgres.NewInboxStore(db)
		ids := deliveryIDs(t, db)
		// ids[1] is processing, [2] waits and is due, [3] waits for later, [4] waits and is at the cap.
		for _, id := range ids[1:5] {
			if _, err := store.Claim(ctx, id, "sms-a", limits); err != nil {
				t.Fatal(err)
			}
		}
		exec := func(q string, args ...any) {
			t.Helper()
			if _, err := db.Pool().Exec(ctx, q, args...); err != nil {
				t.Fatal(err)
			}
		}
		exec(`UPDATE inbox SET state = 'WAITING_RETRY', next_attempt_at = now() - interval '1s', retries = 1, provider = 'sms-b' WHERE delivery_id = $1`, ids[2])
		exec(`UPDATE inbox SET state = 'WAITING_RETRY', next_attempt_at = now() + interval '1h' WHERE delivery_id = $1`, ids[3])
		exec(`UPDATE inbox SET state = 'WAITING_RETRY', next_attempt_at = now() - interval '1s', claim_count = 2 WHERE delivery_id = $1`, ids[4])

		const missing = int64(987654321)
		batch := []int64{ids[0], ids[1], ids[2], ids[3], ids[4], ids[5], missing, ids[0]}
		got, err := store.ClaimMany(ctx, batch, "sms-a", limits)
		if err != nil {
			t.Fatal(err)
		}
		want := []domain.InboxClaim{
			{Result: domain.InboxClaimWon, ClaimCount: 1, Provider: "sms-a"},
			{Result: domain.InboxClaimDuplicate},
			{Result: domain.InboxClaimWon, ClaimCount: 2, Retries: 1, Provider: "sms-b"},
			{Result: domain.InboxClaimDuplicate},
			{Result: domain.InboxClaimDead},
			{Result: domain.InboxClaimWon, ClaimCount: 1, Provider: "sms-a"},
			{Result: domain.InboxClaimMissing},
			{Result: domain.InboxClaimDuplicate},
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("claim[%d] = %+v, want %+v", i, got[i], want[i])
			}
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'PROCESSING'`); n != 4 {
			t.Errorf("processing rows = %d, want 4 (ids 0, 1, 2 and 5)", n)
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE delivery_id = $1 AND state = 'DEAD' AND failure_reason = 'MAX_CLAIMS_EXCEEDED'`, ids[4]); n != 1 {
			t.Error("the capped row should be DEAD with the reason")
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM deliveries WHERE id = $1 AND state = 'FAILED'`, ids[4]); n != 1 {
			t.Error("the capped row's delivery should be FAILED")
		}
	})

	t.Run("an empty batch claims nothing", func(t *testing.T) {
		db := seed(t, 1)
		got, err := postgres.NewInboxStore(db).ClaimMany(ctx, nil, "sms-a", limits)
		if err != nil || len(got) != 0 {
			t.Fatalf("ClaimMany(nil) = %v, %v, want empty", got, err)
		}
	})

	t.Run("senders claiming overlapping batches: each id is won once, with no deadlock", func(t *testing.T) {
		db := seed(t, 40)
		store := postgres.NewInboxStore(db)
		ids := deliveryIDs(t, db)
		reversed := make([]int64, len(ids))
		for i, id := range ids {
			reversed[len(ids)-1-i] = id
		}

		var mu sync.Mutex
		wins := map[int64]int{}
		var wg sync.WaitGroup
		for _, batch := range [][]int64{ids, reversed, ids, reversed} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := store.ClaimMany(ctx, batch, "sms-a", limits)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for i, c := range got {
					if c.Result == domain.InboxClaimWon {
						wins[batch[i]]++
					}
				}
			}()
		}
		wg.Wait()
		for _, id := range ids {
			if wins[id] != 1 {
				t.Errorf("delivery %d won %d times, want once", id, wins[id])
			}
		}
	})
}

func TestInboxSettle(t *testing.T) {
	ctx := context.Background()
	limits := domain.Claims(5)
	next := time.Now().Add(time.Hour).Truncate(time.Millisecond)

	db := seed(t, 7)
	store := postgres.NewInboxStore(db)
	ids := deliveryIDs(t, db)
	claims, err := store.ClaimMany(ctx, ids, "sms-a", limits)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range claims {
		if c.Result != domain.InboxClaimWon {
			t.Fatalf("claim %d = %+v, want won", i, c)
		}
	}
	// ids[5]'s claim goes stale (another sender took it over); ids[6] was finished by a webhook.
	if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET claim_count = 2 WHERE delivery_id = $1`, ids[5]); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool().Exec(ctx, `UPDATE deliveries SET state = 'DELIVERED' WHERE id = $1`, ids[6]); err != nil {
		t.Fatal(err)
	}

	got, err := store.Settle(ctx, []domain.InboxOutcome{
		{Kind: domain.OutcomeComplete, DeliveryID: ids[0], ClaimCount: 1},
		{Kind: domain.OutcomeFail, DeliveryID: ids[1], ClaimCount: 1, Class: domain.FailureProvider, Reason: domain.ReasonProviderFailed},
		{Kind: domain.OutcomePark, DeliveryID: ids[2], ClaimCount: 1, Class: domain.FailureProvider, Next: next},
		{Kind: domain.OutcomeSwitch, DeliveryID: ids[3], ClaimCount: 1, Class: domain.FailureTimeout, Provider: "sms-b", Next: next},
		{Kind: domain.OutcomeDefer, DeliveryID: ids[4], ClaimCount: 1, Next: next},
		{Kind: domain.OutcomeComplete, DeliveryID: ids[5], ClaimCount: 1},
		{Kind: domain.OutcomeComplete, DeliveryID: ids[6], ClaimCount: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, true, true, true, true, false, true}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("applied[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	check := func(what, q string, args ...any) {
		t.Helper()
		if n := count(t, db.Pool(), q, args...); n != 1 {
			t.Errorf("%s: no matching row", what)
		}
	}
	check("complete", `SELECT count(*) FROM inbox i JOIN deliveries d ON d.id = i.delivery_id WHERE i.delivery_id = $1 AND i.state = 'DONE' AND d.state = 'ACCEPTED' AND d.accepted_at IS NOT NULL`, ids[0])
	check("fail", `SELECT count(*) FROM inbox i JOIN deliveries d ON d.id = i.delivery_id WHERE i.delivery_id = $1 AND i.state = 'DEAD' AND i.failure_class = 'provider' AND i.failure_reason = 'PROVIDER_FAILED' AND d.state = 'FAILED' AND d.failure_reason = 'PROVIDER_FAILED'`, ids[1])
	check("park", `SELECT count(*) FROM inbox WHERE delivery_id = $1 AND state = 'WAITING_RETRY' AND retries = 1 AND failure_class = 'provider' AND next_attempt_at = $2`, ids[2], next)
	check("switch", `SELECT count(*) FROM inbox WHERE delivery_id = $1 AND state = 'WAITING_RETRY' AND provider = 'sms-b' AND retries = 0 AND failure_class = 'timeout' AND next_attempt_at = $2`, ids[3], next)
	check("defer", `SELECT count(*) FROM inbox WHERE delivery_id = $1 AND state = 'WAITING_RETRY' AND retries = 0 AND deferrals = 1 AND failure_class IS NULL AND next_attempt_at = $2`, ids[4], next)
	check("a stale claim writes nothing", `SELECT count(*) FROM inbox i JOIN deliveries d ON d.id = i.delivery_id WHERE i.delivery_id = $1 AND i.state = 'PROCESSING' AND d.state = 'PENDING'`, ids[5])
	check("a finished delivery keeps its state", `SELECT count(*) FROM inbox i JOIN deliveries d ON d.id = i.delivery_id WHERE i.delivery_id = $1 AND i.state = 'DONE' AND d.state = 'DELIVERED'`, ids[6])

	t.Run("unresolve ends the row and the delivery", func(t *testing.T) {
		db := seed(t, 1)
		store := postgres.NewInboxStore(db)
		id := deliveryID(t, db)
		if _, err := store.Claim(ctx, id, "sms-a", limits); err != nil {
			t.Fatal(err)
		}
		got, err := store.Settle(ctx, []domain.InboxOutcome{{Kind: domain.OutcomeUnresolve, DeliveryID: id, ClaimCount: 1, Class: domain.FailureTimeout}})
		if err != nil || len(got) != 1 || !got[0] {
			t.Fatalf("settle = %v, %v, want applied", got, err)
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM inbox i JOIN deliveries d ON d.id = i.delivery_id WHERE i.state = 'DEAD' AND i.failure_class = 'timeout' AND d.state = 'UNRESOLVED'`); n != 1 {
			t.Error("row should be DEAD and the delivery UNRESOLVED")
		}
	})

	t.Run("an empty batch writes nothing", func(t *testing.T) {
		if got, err := store.Settle(ctx, nil); err != nil || len(got) != 0 {
			t.Fatalf("Settle(nil) = %v, %v, want empty", got, err)
		}
	})
}

func TestDeliveryLoadMany(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 3)
	store := postgres.NewDeliveryStore(db)
	ids := deliveryIDs(t, db)

	got, err := store.LoadMany(ctx, append([]int64{987654321}, ids...))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("loaded %d deliveries, want 3 (the unknown id left out)", len(got))
	}
	for _, id := range ids {
		one, err := store.Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got[id], one) {
			t.Errorf("LoadMany[%d] = %+v, want what Load gives: %+v", id, got[id], one)
		}
	}
}
