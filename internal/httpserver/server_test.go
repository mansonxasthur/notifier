package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

type pinger struct{ err error }

func (p pinger) Ping(context.Context) error { return p.err }

func status(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	return rec.Code
}

func TestHealth(t *testing.T) {
	up := Health(slog.New(slog.DiscardHandler), pinger{})
	if got := status(t, up, "/healthz"); got != http.StatusOK {
		t.Errorf("healthz = %d", got)
	}
	if got := status(t, up, "/readyz"); got != http.StatusOK {
		t.Errorf("readyz healthy = %d", got)
	}

	down := Health(slog.New(slog.DiscardHandler), pinger{}, pinger{err: errors.New("down")})
	if got := status(t, down, "/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("readyz with a dead dependency = %d, want 503", got)
	}
	if got := status(t, down, "/healthz"); got != http.StatusOK {
		t.Errorf("healthz must not depend on dependencies, got %d", got)
	}
}

func TestStartBadAddress(t *testing.T) {
	s := New("api", "256.0.0.1:99999", http.NewServeMux(), slog.New(slog.DiscardHandler))
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("want listen error")
	}
}

func TestStartServeStop(t *testing.T) {
	s := New("api", "127.0.0.1:0", Health(slog.New(slog.DiscardHandler)), slog.New(slog.DiscardHandler))
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
