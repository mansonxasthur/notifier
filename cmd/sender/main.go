// Command sender consumes channel queues and makes one provider call per attempt.
package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/prometheus"
	"github.com/mansonxasthur/notifier/internal/adapters/provider"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq"
	"github.com/mansonxasthur/notifier/internal/app"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/httpserver"
	"github.com/mansonxasthur/notifier/internal/lifecycle"
	"github.com/mansonxasthur/notifier/internal/sender"
)

func main() { os.Exit(app.Main("sender", build)) }

func build(cfg *config.Config, log *slog.Logger) ([]lifecycle.Component, error) {
	if err := cfg.Require(config.Postgres, config.RabbitMQ); err != nil {
		return nil, err
	}
	db := postgres.New(cfg.DatabaseURL).WithMaxConns(cfg.Database.MaxConns)
	mq := rabbitmq.New(cfg.AMQPURL)

	inbox := postgres.NewInboxStore(db)
	providers := provider.NewRegistry(cfg, nil)
	metrics := prometheus.New()
	breakers := sender.NewBreakers(cfg.Sender.Breaker.Policy(), time.Now)
	attempt := sender.NewAttempt(postgres.NewDeliveryStore(db), inbox, providers, providers,
		breakers,
		sender.NewProviderLimits(cfg.ProviderRateLimits(), time.Now),
		sender.Routing{Order: cfg.ProviderOrder(), Policies: cfg.ProviderRetryPolicies()}, log).WithRecorder(metrics)
	intake := sender.NewIntake(inbox, cfg.PrimaryProviders(), cfg.Sender.ClaimLimits(), attempt.Send, log)
	scan := sender.NewScan(inbox, attempt.Send, cfg.Sender.PollInterval, cfg.Sender.ScanBatch, cfg.Sender.ClaimLimits(), log)
	s := sender.New(rabbitmq.NewConsumer(mq, cfg.Sender.Prefetch, log).WithRecorder(metrics), intake, scan)

	// Every configured provider is exported from the start, closed until its breaker says otherwise.
	metrics.BreakerStates(func() map[string]int {
		states := breakers.States()
		for _, names := range cfg.ProviderOrder() {
			for _, name := range names {
				if _, ok := states[name]; !ok {
					states[name] = prometheus.BreakerClosed
				}
			}
		}
		return states
	})
	mux := httpserver.Health(log, db)
	mux.Handle("GET /metrics", metrics.Handler())
	srv := httpserver.New("http", cfg.HTTP.Addr, mux, log)

	return []lifecycle.Component{db, mq, s, srv}, nil
}
