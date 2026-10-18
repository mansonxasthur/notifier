// Command api is the Dispatcher: it accepts notification requests and provider webhooks.
package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/ratelimit"
	"github.com/mansonxasthur/notifier/internal/adapters/redis"
	"github.com/mansonxasthur/notifier/internal/adapters/webhookauth"
	"github.com/mansonxasthur/notifier/internal/app"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/httpserver"
	"github.com/mansonxasthur/notifier/internal/httpserver/requests"
	"github.com/mansonxasthur/notifier/internal/lifecycle"
)

const (
	// globalBucketKey is the Redis key every Dispatcher instance draws the global limit from.
	globalBucketKey = "rl:global"
	// sharedLimiterTimeout bounds one Redis call, well inside the 5 second target, so a hung
	// Redis delays a request by little before the local limiter answers.
	sharedLimiterTimeout = 100 * time.Millisecond
	// sharedLimiterCooldown is how long the Dispatcher leaves Redis alone after a failure.
	sharedLimiterCooldown = time.Second
)

func main() { os.Exit(app.Main("api", build)) }

func build(cfg *config.Config, log *slog.Logger) ([]lifecycle.Component, error) {
	if err := cfg.Require(config.Postgres, config.Redis, config.APIKey); err != nil {
		return nil, err
	}
	if err := cfg.RequireWebhookSecrets(); err != nil {
		return nil, err
	}
	authenticators, err := webhookauth.FromConfig(cfg)
	if err != nil {
		return nil, err
	}
	db := postgres.New(cfg.DatabaseURL)
	cache := redis.New(cfg.RedisURL)
	limiter := ratelimit.NewFallback(
		redis.NewLimiter(cache.Scripter(), globalBucketKey, cfg.Limits.Global.Policy()),
		ratelimit.NewLocal(cfg.Limits.Global.Policy(), time.Now),
		sharedLimiterTimeout, sharedLimiterCooldown, log, time.Now)
	mux := httpserver.Health(log, db, cache)
	err = httpserver.RegisterNotifications(mux, httpserver.NotificationsOptions{
		Log:          log,
		Store:        postgres.NewNotificationStore(db, cfg.Accept.ChunkSize),
		APIKey:       cfg.APIKey,
		Limiter:      limiter,
		Policy:       policy(cfg),
		MaxBodyBytes: cfg.Accept.MaxBodyBytes,
	})
	if err != nil {
		return nil, err
	}
	err = httpserver.RegisterWebhooks(mux, httpserver.WebhooksOptions{
		Log:            log,
		Store:          postgres.NewWebhookStore(db),
		Authenticators: authenticators,
		Channels:       cfg.ProviderChannels(),
	})
	if err != nil {
		return nil, err
	}
	srv := httpserver.New("http", cfg.HTTP.Addr, mux, log)
	// The server starts last so it only listens once its dependencies are connected.
	return []lifecycle.Component{db, cache, srv}, nil
}

// policy turns the configured limits into the rules the domain validates against.
func policy(cfg *config.Config) requests.Policy {
	expiry := make(map[string]time.Duration, len(cfg.NotificationTypes))
	for name, t := range cfg.NotificationTypes {
		expiry[name] = t.Expiry
	}
	return requests.Policy{BatchLimit: cfg.BatchLimit, Expiry: expiry}
}
