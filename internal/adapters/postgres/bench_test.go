package postgres_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
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
