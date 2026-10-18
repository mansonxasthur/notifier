package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/domain"
)

func deliveryID(t *testing.T, db *postgres.Pool) int64 {
	t.Helper()
	var id int64
	if err := db.Pool().QueryRow(context.Background(), `SELECT min(id) FROM deliveries`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestInboxClaim(t *testing.T) {
	ctx := context.Background()

	t.Run("first claim wins and a redelivery is a duplicate", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewInboxStore(db), deliveryID(t, db)

		if got, err := store.Claim(ctx, id, "sms-a", 3); err != nil || got != domain.InboxClaimWon {
			t.Fatalf("first claim = %v, %v, want won", got, err)
		}
		if got, err := store.Claim(ctx, id, "sms-a", 3); err != nil || got != domain.InboxClaimDuplicate {
			t.Fatalf("second claim = %v, %v, want duplicate", got, err)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'PROCESSING' AND claim_count = 1 AND provider = 'sms-a'`); got != 1 {
			t.Errorf("inbox rows = %d, want one processing row with claim_count 1", got)
		}
	})

	t.Run("concurrent senders: exactly one wins", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewInboxStore(db), deliveryID(t, db)

		var mu sync.Mutex
		results := map[domain.InboxClaimResult]int{}
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := store.Claim(ctx, id, "sms-a", 3)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				results[got]++
				mu.Unlock()
			}()
		}
		wg.Wait()
		if results[domain.InboxClaimWon] != 1 || results[domain.InboxClaimDuplicate] != 7 {
			t.Errorf("results = %v, want 1 won and 7 duplicates", results)
		}
	})

	t.Run("a waiting retry can be claimed again, up to the cap", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewInboxStore(db), deliveryID(t, db)
		wait := func() {
			if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET state = 'WAITING_RETRY'`); err != nil {
				t.Fatal(err)
			}
		}

		if got, err := store.Claim(ctx, id, "sms-a", 2); err != nil || got != domain.InboxClaimWon {
			t.Fatalf("first claim = %v, %v, want won", got, err)
		}
		wait()
		if got, err := store.Claim(ctx, id, "sms-a", 2); err != nil || got != domain.InboxClaimWon {
			t.Fatalf("retry claim = %v, %v, want won", got, err)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE claim_count = 2`); got != 1 {
			t.Error("claim_count should be 2 after the retry claim")
		}
		wait()
		if got, err := store.Claim(ctx, id, "sms-a", 2); err != nil || got != domain.InboxClaimDead {
			t.Fatalf("claim over the cap = %v, %v, want dead", got, err)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'DEAD' AND failure_reason = 'MAX_CLAIMS_EXCEEDED'`); got != 1 {
			t.Error("inbox row should be DEAD with the reason")
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM deliveries WHERE state = 'FAILED'`); got != 1 {
			t.Error("delivery should be FAILED")
		}
	})
}

func TestInboxStuck(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 2)
	store := postgres.NewInboxStore(db)

	var ids []int64
	rows, err := db.Pool().Query(ctx, `SELECT id FROM deliveries ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if _, err := store.Claim(ctx, id, "sms-a", 3); err != nil {
			t.Fatal(err)
		}
	}
	// Only the first row has been processing long enough to be stuck.
	if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET last_try = now() - interval '1 minute' WHERE delivery_id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}

	stuck, err := store.ListStuck(ctx, "sms-a", 30*time.Second, 10)
	if err != nil || len(stuck) != 1 || stuck[0].DeliveryID != ids[0] || stuck[0].ClaimCount != 1 {
		t.Fatalf("stuck = %+v, err = %v, want only delivery %d", stuck, err, ids[0])
	}
	if other, _ := store.ListStuck(ctx, "sms-b", 30*time.Second, 10); len(other) != 0 {
		t.Error("another provider's rows must not be listed")
	}

	t.Run("requeue is fenced by claim_count", func(t *testing.T) {
		wrong := stuck[0]
		wrong.ClaimCount = 9
		if ok, err := store.Requeue(ctx, wrong, time.Now()); err != nil || ok {
			t.Errorf("stale requeue = %v, %v, want false", ok, err)
		}
		next := time.Now().Add(time.Hour)
		if ok, err := store.Requeue(ctx, stuck[0], next); err != nil || !ok {
			t.Fatalf("requeue = %v, %v, want true", ok, err)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'WAITING_RETRY' AND next_attempt_at > now()`); got != 1 {
			t.Error("row should wait for retry in the future")
		}
	})

	t.Run("accept ends the row and accepts the delivery", func(t *testing.T) {
		if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET last_try = now() - interval '1 minute' WHERE delivery_id = $1`, ids[1]); err != nil {
			t.Fatal(err)
		}
		s := domain.StuckInbox{DeliveryID: ids[1], Provider: "sms-a", ClaimCount: 1}
		if ok, err := store.Accept(ctx, s); err != nil || !ok {
			t.Fatalf("accept = %v, %v, want true", ok, err)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE delivery_id = $1 AND state = 'DONE'`, ids[1]); got != 1 {
			t.Error("inbox row should be DONE")
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM deliveries WHERE id = $1 AND state = 'ACCEPTED'`, ids[1]); got != 1 {
			t.Error("delivery should be ACCEPTED")
		}
	})
}
