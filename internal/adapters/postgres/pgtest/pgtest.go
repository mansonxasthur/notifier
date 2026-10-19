// Package pgtest starts a throwaway Postgres with the project's migrations, for integration tests.
package pgtest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the pgx5 database driver
	_ "github.com/golang-migrate/migrate/v4/source/file"     // registers the file source driver
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewDB starts a Postgres container and returns its connection string. It skips the test
// under -short and removes the container when the test ends.
func NewDB(t testing.TB) string {
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

// Migrator returns a migrator for the database at url, reading the repository's migrations.
func Migrator(t testing.TB, url string) *migrate.Migrate {
	t.Helper()
	dir := filepath.Join(repoRoot(t), "migrations")
	m, err := migrate.New("file://"+filepath.ToSlash(dir), strings.Replace(url, "postgres://", "pgx5://", 1))
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { _, _ = m.Close() })
	return m
}

// repoRoot walks up from the test's working directory (its package directory) to go.mod.
func repoRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// Up applies every pending migration.
func Up(t testing.TB, m *migrate.Migrate) {
	t.Helper()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("up: %v", err)
	}
}

// NewMigratedDB starts a Postgres container, applies the migrations, and returns its connection string.
func NewMigratedDB(t testing.TB) string {
	t.Helper()
	url := NewDB(t)
	Up(t, Migrator(t, url))
	return url
}
