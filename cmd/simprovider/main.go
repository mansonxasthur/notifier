// Command simprovider is the simulated provider service. Failure modes, webhooks and
// status lookup arrive in Phase 4; for now it only answers health checks.
package main

import (
	"log/slog"
	"os"

	"github.com/mansonxasthur/notifier/internal/app"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/httpserver"
	"github.com/mansonxasthur/notifier/internal/lifecycle"
)

func main() { os.Exit(app.Main("simprovider", build)) }

func build(cfg *config.Config, log *slog.Logger) ([]lifecycle.Component, error) {
	return []lifecycle.Component{httpserver.New("http", cfg.HTTP.Addr, httpserver.Health(log), log)}, nil
}
