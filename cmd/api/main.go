// Command api is the Dispatcher: it accepts notification requests and provider webhooks.
package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/redis"
	"github.com/mansonxasthur/notifier/internal/app"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/httpserver"
	"github.com/mansonxasthur/notifier/internal/lifecycle"
)

func main() { os.Exit(app.Main("api", build)) }

func build(cfg *config.Config, log *slog.Logger) ([]lifecycle.Component, error) {
	if err := cfg.Require(config.Postgres, config.Redis, config.APIKey); err != nil {
		return nil, err
	}
	db := postgres.New(cfg.DatabaseURL)
	cache := redis.New(cfg.RedisURL)
	mux := httpserver.Health(log, db, cache)
	err := httpserver.RegisterNotifications(mux, httpserver.NotificationsOptions{
		Log:          log,
		Store:        postgres.NewNotificationStore(db, cfg.Accept.ChunkSize),
		APIKey:       cfg.APIKey,
		Policy:       policy(cfg),
		MaxBodyBytes: cfg.Accept.MaxBodyBytes,
	})
	if err != nil {
		return nil, err
	}
	srv := httpserver.New("http", cfg.HTTP.Addr, mux, log)
	// The server starts last so it only listens once its dependencies are connected.
	return []lifecycle.Component{db, cache, srv}, nil
}

// policy turns the configured limits into the rules the domain validates against.
func policy(cfg *config.Config) domain.Policy {
	expiry := make(map[string]time.Duration, len(cfg.NotificationTypes))
	for name, t := range cfg.NotificationTypes {
		expiry[name] = t.Expiry
	}
	return domain.Policy{BatchLimit: cfg.BatchLimit, Expiry: expiry}
}
