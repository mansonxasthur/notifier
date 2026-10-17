package postgres_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const migrationsDir = "file://../../../migrations"

// newDB starts a throwaway Postgres and returns its connection string.
func newDB(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("notifier"),
		tcpostgres.WithUsername("notifier"),
		tcpostgres.WithPassword("notifier"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(ctr) })

	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	return url
}

func newMigrate(t *testing.T, url string) *migrate.Migrate {
	t.Helper()
	m, err := migrate.New(migrationsDir, strings.Replace(url, "postgres://", "pgx5://", 1))
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	return m
}

func up(t *testing.T, m *migrate.Migrate) {
	t.Helper()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("up: %v", err)
	}
}

func TestMigrationsRoundTrip(t *testing.T) {
	m := newMigrate(t, newDB(t))
	up(t, m)
	if err := m.Down(); err != nil {
		t.Fatalf("down: %v", err)
	}
	up(t, m)
}

func TestSchema(t *testing.T) {
	ctx := context.Background()
	url := newDB(t)
	up(t, newMigrate(t, url))

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	t.Run("only the documented columns are nullable", func(t *testing.T) {
		rows, err := pool.Query(ctx, `
			SELECT table_name || '.' || column_name FROM information_schema.columns
			WHERE table_schema = 'public' AND is_nullable = 'YES'
			  AND table_name IN ('notifications', 'deliveries', 'outbox', 'inbox')`)
		if err != nil {
			t.Fatal(err)
		}
		got, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)
		want := []string{
			"inbox.failure_class", "inbox.failure_reason", "inbox.next_attempt_at",
			"outbox.failure_reason", "outbox.next_attempt_at",
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("nullable columns = %v, want %v", got, want)
		}
	})

	t.Run("indexes exist", func(t *testing.T) {
		for _, name := range []string{"outbox_due_idx", "outbox_stuck_idx", "inbox_due_idx", "inbox_stuck_idx"} {
			var def string
			err := pool.QueryRow(ctx, `SELECT indexdef FROM pg_indexes WHERE indexname = $1`, name).Scan(&def)
			if err != nil {
				t.Errorf("index %s: %v", name, err)
				continue
			}
			if !strings.Contains(def, "WHERE") {
				t.Errorf("index %s should be partial: %s", name, def)
			}
		}
	})

	t.Run("duplicate delivery is skipped", func(t *testing.T) {
		var nid int64
		err := pool.QueryRow(ctx, `INSERT INTO notifications (idempotency_key, type, content)
			VALUES ('k1', 'marketing', '{}') RETURNING id`).Scan(&nid)
		if err != nil {
			t.Fatal(err)
		}
		const insert = `INSERT INTO deliveries (notification_id, recipient_id, channel, contact, expires_at)
			VALUES ($1, 'r1', 'email', '{}', now() + interval '1 hour')
			ON CONFLICT (notification_id, recipient_id, channel) DO NOTHING RETURNING id`

		var id int64
		if err := pool.QueryRow(ctx, insert, nid).Scan(&id); err != nil {
			t.Fatalf("first insert should return a row: %v", err)
		}
		if err := pool.QueryRow(ctx, insert, nid).Scan(&id); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("duplicate should return no row, got %v", err)
		}
	})

	t.Run("relay due query uses the partial index", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		// The table is tiny, so force the planner off a sequential scan.
		if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
			t.Fatal(err)
		}
		rows, err := tx.Query(ctx, `EXPLAIN SELECT delivery_id FROM outbox
			WHERE state = 'NEW' AND (next_attempt_at IS NULL OR next_attempt_at <= now())
			ORDER BY next_attempt_at NULLS FIRST LIMIT 100`)
		if err != nil {
			t.Fatal(err)
		}
		lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		if plan := strings.Join(lines, "\n"); !strings.Contains(plan, "outbox_due_idx") {
			t.Errorf("plan does not use outbox_due_idx:\n%s", plan)
		}
	})
}
