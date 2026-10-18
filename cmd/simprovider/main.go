// Command simprovider is the simulated provider service. It answers sends in configurable
// scenarios, serves status lookups, and sends signed webhooks back to the API.
package main

import (
	"log/slog"
	"os"

	"github.com/mansonxasthur/notifier/internal/app"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/httpserver"
	"github.com/mansonxasthur/notifier/internal/lifecycle"
	"github.com/mansonxasthur/notifier/internal/simprovider"
)

func main() { os.Exit(app.Main("simprovider", build)) }

func build(cfg *config.Config, log *slog.Logger) ([]lifecycle.Component, error) {
	if err := cfg.RequireWebhookSecrets(); err != nil {
		return nil, err
	}
	hooks := simprovider.NewWebhooks(cfg, log)
	mux := httpserver.Health(log)
	simprovider.NewServer(cfg, hooks, log).Register(mux)
	// The server stops first, so no new webhooks are scheduled while the last ones drain.
	return []lifecycle.Component{hooks, httpserver.New("http", cfg.HTTP.Addr, mux, log)}, nil
}
