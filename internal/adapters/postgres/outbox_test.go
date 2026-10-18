package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
	"github.com/mansonxasthur/notifier/internal/domain"
)

// seed starts a migrated database and creates n pending SMS deliveries with outbox rows.
func seed(t *testing.T, n int) *postgres.Pool {
	t.Helper()
	ctx := context.Background()
	db := postgres.New(pgtest.NewMigratedDB(t))
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Stop(ctx) })

	recipients := make([]string, n)
	for i := range recipients {
		recipients[i] = fmt.Sprintf("u_%04d", i)
	}
	if _, err := postgres.NewNotificationStore(db, 5000).Create(ctx, newNotification(uuid.NewString(), recipients...)); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestOutboxClaim(t *testing.T) {
	ctx := context.Background()

	t.Run("claims due rows once and bumps the fencing token", func(t *testing.T) {
		db := seed(t, 3)
		store := postgres.NewOutboxStore(db)

		claims, err := store.Claim(ctx, 10, 5)
		if err != nil {
			t.Fatal(err)
		}
		if len(claims) != 3 {
			t.Fatalf("claimed %d, want 3", len(claims))
		}
		for _, c := range claims {
			if c.Retries != 1 || c.Channel != domain.ChannelSMS {
				t.Errorf("claim = %+v, want retries 1 on sms", c)
			}
		}
		if again, _ := store.Claim(ctx, 10, 5); len(again) != 0 {
			t.Errorf("second claim returned %d rows, want none", len(again))
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'PROCESSING' AND retries = 1`); got != 3 {
			t.Errorf("processing rows = %d, want 3", got)
		}
	})

	t.Run("respects the limit and skips rows not yet due", func(t *testing.T) {
		db := seed(t, 4)
		store := postgres.NewOutboxStore(db)
		if _, err := db.Pool().Exec(ctx, `UPDATE outbox SET next_attempt_at = now() + interval '1 hour'
			WHERE delivery_id = (SELECT min(delivery_id) FROM outbox)`); err != nil {
			t.Fatal(err)
		}
		claims, err := store.Claim(ctx, 2, 5)
		if err != nil || len(claims) != 2 {
			t.Fatalf("claims = %d, err = %v, want 2", len(claims), err)
		}
		rest, _ := store.Claim(ctx, 10, 5)
		if len(rest) != 1 {
			t.Errorf("remaining claim = %d, want 1 (one is not due)", len(rest))
		}
	})

	t.Run("two relays never take the same row", func(t *testing.T) {
		const rows = 200
		db := seed(t, rows)
		store := postgres.NewOutboxStore(db)

		var mu sync.Mutex
		seen := map[int64]int{}
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					claims, err := store.Claim(ctx, 7, 5)
					if err != nil {
						t.Error(err)
						return
					}
					if len(claims) == 0 {
						return
					}
					mu.Lock()
					for _, c := range claims {
						seen[c.DeliveryID]++
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if len(seen) != rows {
			t.Errorf("claimed %d distinct rows, want %d", len(seen), rows)
		}
		for id, n := range seen {
			if n != 1 {
				t.Errorf("delivery %d claimed %d times", id, n)
			}
		}
	})

	t.Run("a row past the attempt cap ends dead and fails its delivery", func(t *testing.T) {
		db := seed(t, 1)
		store := postgres.NewOutboxStore(db)
		if _, err := db.Pool().Exec(ctx, `UPDATE outbox SET retries = 3`); err != nil {
			t.Fatal(err)
		}
		claims, err := store.Claim(ctx, 10, 3)
		if err != nil || len(claims) != 0 {
			t.Fatalf("claims = %d, err = %v, want none", len(claims), err)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'DEAD' AND failure_reason = 'MAX_ATTEMPTS_EXCEEDED'`); got != 1 {
			t.Errorf("dead rows = %d, want 1", got)
		}
		if got := count(t, db.Pool(), `SELECT count(*) FROM deliveries WHERE state = 'FAILED'`); got != 1 {
			t.Errorf("failed deliveries = %d, want 1", got)
		}
	})
}

func TestOutboxFencing(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 1)
	store := postgres.NewOutboxStore(db)

	stale, err := store.Claim(ctx, 1, 5)
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v, %d", err, len(stale))
	}
	// The janitor reclaims the row and another relay claims it again.
	n, err := store.ReclaimStuck(ctx, 0, func(int) time.Duration { return 0 })
	if err != nil || n != 1 {
		t.Fatalf("reclaim = %d, err = %v, want 1", n, err)
	}
	fresh, err := store.Claim(ctx, 1, 5)
	if err != nil || len(fresh) != 1 || fresh[0].Retries != 2 {
		t.Fatalf("fresh claim = %+v, err = %v, want retries 2", fresh, err)
	}

	if n, err := store.MarkPublished(ctx, stale); err != nil || n != 0 {
		t.Errorf("stale MarkPublished = %d, %v, want 0", n, err)
	}
	if ok, err := store.Release(ctx, stale[0], time.Now(), domain.ReasonPublishFailed); err != nil || ok {
		t.Errorf("stale Release = %v, %v, want false", ok, err)
	}
	if n, err := store.MarkPublished(ctx, fresh); err != nil || n != 1 {
		t.Errorf("current MarkPublished = %d, %v, want 1", n, err)
	}
}

func TestOutboxRelease(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 1)
	store := postgres.NewOutboxStore(db)

	claims, _ := store.Claim(ctx, 1, 5)
	next := time.Now().Add(time.Hour)
	if ok, err := store.Release(ctx, claims[0], next, domain.ReasonPublishFailed); err != nil || !ok {
		t.Fatalf("Release = %v, %v", ok, err)
	}
	if again, _ := store.Claim(ctx, 1, 5); len(again) != 0 {
		t.Error("released row must not be due before next_attempt_at")
	}
	if got := count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'NEW' AND failure_reason = 'PUBLISH_FAILED' AND next_attempt_at > now()`); got != 1 {
		t.Errorf("released rows = %d, want 1", got)
	}
}

func TestOutboxReclaimStuck(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 2)
	store := postgres.NewOutboxStore(db)

	if claims, _ := store.Claim(ctx, 2, 5); len(claims) != 2 {
		t.Fatal("setup: want 2 claims")
	}
	if _, err := db.Pool().Exec(ctx, `UPDATE outbox SET last_try = now() - interval '1 minute'
		WHERE delivery_id = (SELECT min(delivery_id) FROM outbox)`); err != nil {
		t.Fatal(err)
	}

	n, err := store.ReclaimStuck(ctx, 30*time.Second, func(int) time.Duration { return time.Hour })
	if err != nil || n != 1 {
		t.Fatalf("reclaimed %d, err = %v, want 1 (only the old claim)", n, err)
	}
	if got := count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'NEW' AND next_attempt_at > now()`); got != 1 {
		t.Errorf("reclaimed rows = %d, want 1 back in NEW and delayed", got)
	}
	if got := count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'PROCESSING'`); got != 1 {
		t.Errorf("recent claim = %d rows still processing, want 1", got)
	}
}

func TestOutboxMarkPublishedBatch(t *testing.T) {
	ctx := context.Background()
	db := seed(t, 5)
	store := postgres.NewOutboxStore(db)

	claims, err := store.Claim(ctx, 5, 5)
	if err != nil || len(claims) != 5 {
		t.Fatalf("claim = %d, %v", len(claims), err)
	}
	// One claim goes stale: its row is reclaimed and claimed again by someone else.
	stale := claims[0]
	if _, err := db.Pool().Exec(ctx, `UPDATE outbox SET retries = retries + 1 WHERE delivery_id = $1`, stale.DeliveryID); err != nil {
		t.Fatal(err)
	}

	n, err := store.MarkPublished(ctx, claims)
	if err != nil || n != 4 {
		t.Fatalf("MarkPublished = %d, %v, want 4 (the stale claim is skipped)", n, err)
	}
	if got := count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'PUBLISHED'`); got != 4 {
		t.Errorf("published rows = %d, want 4", got)
	}
	if got := count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'PROCESSING' AND delivery_id = $1`, stale.DeliveryID); got != 1 {
		t.Error("the stale row must stay with its new owner")
	}
	if n, err := store.MarkPublished(ctx, nil); err != nil || n != 0 {
		t.Errorf("empty MarkPublished = %d, %v, want 0", n, err)
	}
}
