// Command api is the Dispatcher: it accepts notification requests and provider webhooks.
package main

import (
	"log/slog"
	"os"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/redis"
	"github.com/mansonxasthur/notifier/internal/app"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/httpserver"
	"github.com/mansonxasthur/notifier/internal/lifecycle"
)

func main() { os.Exit(app.Main("api", build)) }

func build(cfg *config.Config, log *slog.Logger) ([]lifecycle.Component, error) {
	if err := cfg.Require(config.Postgres, config.Redis); err != nil {
		return nil, err
	}
	db := postgres.New(cfg.DatabaseURL)
	cache := redis.New(cfg.RedisURL)
	srv := httpserver.New("http", cfg.HTTP.Addr, httpserver.Health(log, db, cache), log)
	// The server starts last so it only listens once its dependencies are connected.
	return []lifecycle.Component{db, cache, srv}, nil
}
