// Command relay publishes outbox rows to the broker and runs the janitor sweeps.
package main

import (
	"log/slog"
	"os"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/provider"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq"
	"github.com/mansonxasthur/notifier/internal/app"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/lifecycle"
	"github.com/mansonxasthur/notifier/internal/ports"
	"github.com/mansonxasthur/notifier/internal/relay"
)

func main() { os.Exit(app.Main("relay", build)) }

func build(cfg *config.Config, log *slog.Logger) ([]lifecycle.Component, error) {
	if err := cfg.Require(config.Postgres, config.RabbitMQ); err != nil {
		return nil, err
	}
	db := postgres.New(cfg.DatabaseURL)
	mq := rabbitmq.New(cfg.AMQPURL)
	backoff := cfg.Relay.Backoff.Policy()

	var providers []relay.Provider
	for _, ch := range cfg.Channels {
		for i, p := range ch.Providers {
			rp := relay.Provider{Name: p.Name, Timeout: p.Timeout, MaxRetries: p.MaxRetries, Backoff: p.Backoff.Policy()}
			if i+1 < len(ch.Providers) {
				rp.Next = ch.Providers[i+1].Name
			}
			providers = append(providers, rp)
		}
	}

	outbox := postgres.NewOutboxStore(db)
	newPublisher := func() ports.Publisher { return rabbitmq.NewPublisher(mq, cfg.Relay.ConfirmTimeout) }
	r := relay.New(outbox, newPublisher, relay.Config{
		PollInterval: cfg.Relay.PollInterval,
		BatchSize:    cfg.Relay.BatchSize,
		Workers:      cfg.Relay.Workers,
		MaxAttempts:  cfg.Relay.MaxAttempts,
		Backoff:      backoff,
	}, log)
	j := relay.NewJanitor(outbox, postgres.NewInboxStore(db), provider.NewRegistry(cfg, nil), relay.JanitorConfig{
		Interval:         cfg.Relay.Janitor.Interval,
		OutboxStuckAfter: cfg.Relay.Janitor.OutboxStuckAfter,
		InboxStuckMargin: cfg.Relay.Janitor.InboxStuckMargin,
		Backoff:          backoff,
		Providers:        providers,
	}, log)

	// Workers come last so they stop first, while the connections are still up.
	return []lifecycle.Component{db, mq, r, j}, nil
}
