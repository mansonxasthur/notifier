package postgres_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
	"github.com/mansonxasthur/notifier/internal/domain"
)

func benchDB(b *testing.B) *postgres.Pool {
	b.Helper()
	ctx := context.Background()
	db := postgres.New(pgtest.NewMigratedDB(b))
	if err := db.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Stop(ctx) })
	return db
}

func recipientIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("u_%06d", i)
	}
	return ids
}

// BenchmarkCreate measures one accepted request: its deliveries and outbox rows in one
// transaction, against a real Postgres in a container on this machine.
func BenchmarkCreate(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("deliveries=%d", n), func(b *testing.B) {
			db := benchDB(b)
			store := postgres.NewNotificationStore(db, 5000)
			ids := recipientIDs(n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := store.Create(ctx, newNotification(uuid.NewString(), ids...)); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(n*b.N)/b.Elapsed().Seconds(), "deliveries/s")
		})
	}
}

// BenchmarkOutboxClaimAndPublish measures what the relay does per batch: claim 500 rows
// and mark them published.
func BenchmarkOutboxClaimAndPublish(b *testing.B) {
	const batch, stock = 500, 50_000
	db := benchDB(b)
	ctx := context.Background()
	notes := postgres.NewNotificationStore(db, 5000)
	outbox := postgres.NewOutboxStore(db)
	left := 0
	refill := func() {
		if _, err := notes.Create(ctx, newNotification(uuid.NewString(), recipientIDs(stock)...)); err != nil {
			b.Fatal(err)
		}
		left = stock
	}
	refill()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if left < batch {
			b.StopTimer()
			refill()
			b.StartTimer()
		}
		claims, err := outbox.Claim(ctx, batch, 10)
		if err != nil || len(claims) != batch {
			b.Fatalf("claim = %d rows, %v; want %d", len(claims), err, batch)
		}
		if n, err := outbox.MarkPublished(ctx, claims); err != nil || n != batch {
			b.Fatalf("mark published = %d, %v; want %d", n, err, batch)
		}
		left -= batch
	}
	b.ReportMetric(float64(batch*b.N)/b.Elapsed().Seconds(), "rows/s")
}

// BenchmarkSenderInbox compares the sender's database work per delivery, one at a time as before
// and in batches of 50: claim, load, and complete. Each single-row call is a commit or a round
// trip of its own, the batch calls one per 50 deliveries.
func BenchmarkSenderInbox(b *testing.B) {
	const batch = 50
	setup := func(b *testing.B) (ids []int64, inbox *postgres.InboxStore, deliveries *postgres.DeliveryStore) {
		db := benchDB(b)
		ctx := context.Background()
		if _, err := postgres.NewNotificationStore(db, 5000).Create(ctx, newNotification(uuid.NewString(), recipientIDs(batch*200)...)); err != nil {
			b.Fatal(err)
		}
		rows, err := db.Pool().Query(ctx, `SELECT id FROM deliveries ORDER BY id`)
		if err != nil {
			b.Fatal(err)
		}
		ids, err = pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			b.Fatal(err)
		}
		return ids, postgres.NewInboxStore(db), postgres.NewDeliveryStore(db)
	}
	limits := domain.Claims(5)

	b.Run("single", func(b *testing.B) {
		ids, inbox, deliveries := setup(b)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			id := ids[i%len(ids)]
			c, err := inbox.Claim(ctx, id, "sms-a", limits)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := deliveries.Load(ctx, id); err != nil {
				b.Fatal(err)
			}
			if _, err := inbox.Complete(ctx, id, c.ClaimCount); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "deliveries/s")
	})

	b.Run("batch=50", func(b *testing.B) {
		ids, inbox, deliveries := setup(b)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i += batch {
			from := (i / batch * batch) % len(ids)
			part := ids[from : from+batch]
			claims, err := inbox.ClaimMany(ctx, part, "sms-a", limits)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := deliveries.LoadMany(ctx, part); err != nil {
				b.Fatal(err)
			}
			outcomes := make([]domain.InboxOutcome, len(part))
			for j, id := range part {
				outcomes[j] = domain.InboxOutcome{Kind: domain.OutcomeComplete, DeliveryID: id, ClaimCount: claims[j].ClaimCount}
			}
			if _, err := inbox.Settle(ctx, outcomes); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "deliveries/s")
	})
}
