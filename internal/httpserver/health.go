package httpserver

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/mansonxasthur/notifier/internal/ports"
)

// Health returns a mux with /healthz (the process is up) and /readyz (dependencies answer).
func Health(log *slog.Logger, deps ...ports.Pinger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		for _, d := range deps {
			if err := d.Ping(ctx); err != nil {
				log.Warn("not ready", "error", err)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
