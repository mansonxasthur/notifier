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

		if got, err := store.Claim(ctx, id, "sms-a", 3); err != nil || got.Result != domain.InboxClaimWon {
			t.Fatalf("first claim = %v, %v, want won", got, err)
		}
		if got, err := store.Claim(ctx, id, "sms-a", 3); err != nil || got.Result != domain.InboxClaimDuplicate {
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
				results[got.Result]++
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

		if got, err := store.Claim(ctx, id, "sms-a", 2); err != nil || got.Result != domain.InboxClaimWon {
			t.Fatalf("first claim = %v, %v, want won", got, err)
		}
		wait()
		if got, err := store.Claim(ctx, id, "sms-a", 2); err != nil || got.Result != domain.InboxClaimWon {
			t.Fatalf("retry claim = %v, %v, want won", got, err)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE claim_count = 2`); got != 1 {
			t.Error("claim_count should be 2 after the retry claim")
		}
		wait()
		if got, err := store.Claim(ctx, id, "sms-a", 2); err != nil || got.Result != domain.InboxClaimDead {
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

// claimAndPark seeds n deliveries, claims each on sms-a and parks it due at the given offset.
func claimAndPark(t *testing.T, db *postgres.Pool, store *postgres.InboxStore, due time.Duration) []int64 {
	t.Helper()
	ctx := context.Background()
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
	for _, id := range ids {
		if _, err := store.Claim(ctx, id, "sms-a", 20); err != nil {
			t.Fatal(err)
		}
		if ok, err := store.Park(ctx, id, 1, domain.FailureProvider, time.Now().Add(due)); err != nil || !ok {
			t.Fatalf("park = %v, %v", ok, err)
		}
	}
	return ids
}

func TestInboxClaimHonoursDueTimeAndProvider(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 1)
	store, id := postgres.NewInboxStore(db), deliveryID(t, db)
	claimAndPark(t, db, store, time.Hour)

	if got, err := store.Claim(ctx, id, "sms-a", 20); err != nil || got.Result != domain.InboxClaimDuplicate {
		t.Fatalf("claim before due = %v, %v, want duplicate", got, err)
	}
	// A failover moved the row to another provider; the next claim must see it.
	if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET provider = 'sms-b', next_attempt_at = now() - interval '1 second', retries = 0`); err != nil {
		t.Fatal(err)
	}
	got, err := store.Claim(ctx, id, "sms-a", 20)
	if err != nil || got.Result != domain.InboxClaimWon || got.Provider != "sms-b" || got.ClaimCount != 2 {
		t.Fatalf("claim when due = %+v, %v, want won on sms-b with claim_count 2", got, err)
	}
}

func TestInboxClaimDue(t *testing.T) {
	ctx := context.Background()

	t.Run("claims only rows that are due, oldest first, up to the limit", func(t *testing.T) {
		db := seed(t, 3)
		store := postgres.NewInboxStore(db)
		ids := claimAndPark(t, db, store, time.Hour)
		for i, id := range ids[:2] {
			if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET next_attempt_at = now() - make_interval(secs => $2) WHERE delivery_id = $1`,
				id, float64(10-i)); err != nil {
				t.Fatal(err)
			}
		}

		got, err := store.ClaimDue(ctx, 1, 20)
		if err != nil || len(got) != 1 || got[0].DeliveryID != ids[0] {
			t.Fatalf("first scan = %+v, %v, want only delivery %d", got, err, ids[0])
		}
		if got[0].ClaimCount != 2 || got[0].Provider != "sms-a" || got[0].Retries != 1 {
			t.Errorf("claim = %+v, want claim_count 2, sms-a, retries 1", got[0])
		}
		got, err = store.ClaimDue(ctx, 10, 20)
		if err != nil || len(got) != 1 || got[0].DeliveryID != ids[1] {
			t.Fatalf("second scan = %+v, %v, want only delivery %d", got, err, ids[1])
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'PROCESSING'`); n != 2 {
			t.Errorf("processing rows = %d, want 2", n)
		}
		if got, err = store.ClaimDue(ctx, 10, 20); err != nil || len(got) != 0 {
			t.Errorf("third scan = %+v, %v, want nothing (the last row is not due)", got, err)
		}
	})

	t.Run("concurrent scans never claim the same row", func(t *testing.T) {
		db := seed(t, 30)
		store := postgres.NewInboxStore(db)
		claimAndPark(t, db, store, -time.Second)

		var mu sync.Mutex
		seen := map[int64]int{}
		var wg sync.WaitGroup
		for range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := store.ClaimDue(ctx, 10, 20)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for _, d := range got {
					seen[d.DeliveryID]++
				}
			}()
		}
		wg.Wait()
		for id, n := range seen {
			if n != 1 {
				t.Errorf("delivery %d claimed %d times", id, n)
			}
		}
		if len(seen) != 30 {
			t.Errorf("claimed %d rows, want all 30", len(seen))
		}
	})

	t.Run("a row over the claim cap is failed, not returned", func(t *testing.T) {
		db := seed(t, 1)
		store := postgres.NewInboxStore(db)
		claimAndPark(t, db, store, -time.Second)

		got, err := store.ClaimDue(ctx, 10, 1)
		if err != nil || len(got) != 0 {
			t.Fatalf("scan = %+v, %v, want nothing", got, err)
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'DEAD' AND failure_reason = 'MAX_CLAIMS_EXCEEDED'`); n != 1 {
			t.Error("inbox row should be DEAD with the reason")
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM deliveries WHERE state = 'FAILED' AND failure_reason = 'MAX_CLAIMS_EXCEEDED'`); n != 1 {
			t.Error("delivery should be FAILED")
		}
	})
}

func TestInboxSwitchAndUnresolve(t *testing.T) {
	ctx := context.Background()

	t.Run("switch moves the row to another provider with retries reset, fenced by claim_count", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewInboxStore(db), deliveryID(t, db)
		claimAndPark(t, db, store, -time.Second)
		due, err := store.ClaimDue(ctx, 1, 20)
		if err != nil || len(due) != 1 {
			t.Fatalf("scan = %+v, %v", due, err)
		}

		if ok, err := store.Switch(ctx, id, due[0].ClaimCount+1, "sms-b", domain.FailureProvider, time.Now()); err != nil || ok {
			t.Errorf("stale switch = %v, %v, want false", ok, err)
		}
		if ok, err := store.Switch(ctx, id, due[0].ClaimCount, "sms-b", domain.FailureTimeout, time.Now().Add(-time.Second)); err != nil || !ok {
			t.Fatalf("switch = %v, %v, want true", ok, err)
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'WAITING_RETRY' AND provider = 'sms-b' AND retries = 0 AND failure_class = 'timeout'`); n != 1 {
			t.Error("row should wait on sms-b with retries reset")
		}
		got, err := store.ClaimDue(ctx, 1, 20)
		if err != nil || len(got) != 1 || got[0].Provider != "sms-b" || got[0].Retries != 0 {
			t.Errorf("next scan = %+v, %v, want the row on sms-b with retries 0", got, err)
		}
	})

	t.Run("unresolve ends the row and the pending delivery, fenced by claim_count", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewInboxStore(db), deliveryID(t, db)
		if _, err := store.Claim(ctx, id, "sms-b", 20); err != nil {
			t.Fatal(err)
		}
		if ok, err := store.Unresolve(ctx, id, 9, domain.FailureTimeout); err != nil || ok {
			t.Errorf("stale unresolve = %v, %v, want false", ok, err)
		}
		if ok, err := store.Unresolve(ctx, id, 1, domain.FailureTimeout); err != nil || !ok {
			t.Fatalf("unresolve = %v, %v, want true", ok, err)
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'DEAD' AND failure_class = 'timeout'`); n != 1 {
			t.Error("inbox row should be DEAD")
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM deliveries WHERE state = 'UNRESOLVED'`); n != 1 {
			t.Error("delivery should be UNRESOLVED")
		}
	})

	t.Run("unresolve leaves a delivery a webhook already finished", func(t *testing.T) {
		db := seed(t, 1)
		store, id := postgres.NewInboxStore(db), deliveryID(t, db)
		if _, err := store.Claim(ctx, id, "sms-b", 20); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Pool().Exec(ctx, `UPDATE deliveries SET state = 'DELIVERED'`); err != nil {
			t.Fatal(err)
		}
		if ok, err := store.Unresolve(ctx, id, 1, domain.FailureTimeout); err != nil || !ok {
			t.Fatalf("unresolve = %v, %v, want true", ok, err)
		}
		if n := count(t, db.Pool(), `SELECT count(*) FROM deliveries WHERE state = 'DELIVERED'`); n != 1 {
			t.Error("a DELIVERED delivery must stay DELIVERED")
		}
	})
}

func TestInboxDefer(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 1)
	store, id := postgres.NewInboxStore(db), deliveryID(t, db)
	won, err := store.Claim(ctx, id, "sms-a", 3)
	if err != nil || won.Result != domain.InboxClaimWon {
		t.Fatalf("claim = %v, %v", won, err)
	}
	next := time.Now().Add(time.Hour)

	if ok, err := store.Defer(ctx, id, won.ClaimCount+1, next); err != nil || ok {
		t.Fatalf("stale defer = %v, %v, want false", ok, err)
	}
	if ok, err := store.Defer(ctx, id, won.ClaimCount, next); err != nil || !ok {
		t.Fatalf("defer = %v, %v, want true", ok, err)
	}
	if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'WAITING_RETRY' AND retries = 0 AND deferrals = 1 AND claim_count = 1 AND failure_class IS NULL AND next_attempt_at > now()`); got != 1 {
		t.Error("a deferral must wait without spending a retry, a failure class or a claim")
	}
	if ok, err := store.Defer(ctx, id, won.ClaimCount, next); err != nil || ok {
		t.Fatalf("defer of a row no longer processing = %v, %v, want false", ok, err)
	}

	t.Run("claims after deferrals stay under the cap", func(t *testing.T) {
		const maxClaims = 2
		for i := range 5 {
			if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET next_attempt_at = now() - interval '1 second'`); err != nil {
				t.Fatal(err)
			}
			due, err := store.ClaimDue(ctx, 10, maxClaims)
			if err != nil || len(due) != 1 {
				t.Fatalf("round %d: ClaimDue = %v, %v; want the row", i, due, err)
			}
			if ok, err := store.Defer(ctx, id, due[0].ClaimCount, next); err != nil || !ok {
				t.Fatalf("round %d: defer = %v, %v", i, ok, err)
			}
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'WAITING_RETRY'`); got != 1 {
			t.Error("the row was killed by the claim cap although it was only deferred")
		}
	})

	t.Run("real claims still reach the cap", func(t *testing.T) {
		const maxClaims = 2
		var last []domain.DueInbox
		for range 6 {
			if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET next_attempt_at = now() - interval '1 second' WHERE state = 'WAITING_RETRY'`); err != nil {
				t.Fatal(err)
			}
			last, err = store.ClaimDue(ctx, 10, maxClaims)
			if err != nil {
				t.Fatal(err)
			}
			if len(last) == 0 {
				break
			}
			// The attempt fails instead of being deferred, so the claim is spent.
			if ok, err := store.Park(ctx, id, last[0].ClaimCount, domain.FailureProvider, time.Now()); err != nil || !ok {
				t.Fatalf("park = %v, %v", ok, err)
			}
		}
		if len(last) != 0 || count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'DEAD'`) != 1 {
			t.Error("claims spent on failed attempts must still end the row at the cap")
		}
	})
}
