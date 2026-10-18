// Command sender consumes channel queues and makes one provider call per attempt.
package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/provider"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq"
	"github.com/mansonxasthur/notifier/internal/app"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/lifecycle"
	"github.com/mansonxasthur/notifier/internal/sender"
)

func main() { os.Exit(app.Main("sender", build)) }

func build(cfg *config.Config, log *slog.Logger) ([]lifecycle.Component, error) {
	if err := cfg.Require(config.Postgres, config.RabbitMQ); err != nil {
		return nil, err
	}
	db := postgres.New(cfg.DatabaseURL)
	mq := rabbitmq.New(cfg.AMQPURL)

	inbox := postgres.NewInboxStore(db)
	providers := provider.NewRegistry(cfg, nil)
	attempt := sender.NewAttempt(postgres.NewDeliveryStore(db), inbox, providers, providers,
		sender.NewBreakers(cfg.Sender.Breaker.Policy(), time.Now),
		sender.Routing{Order: cfg.ProviderOrder(), Policies: cfg.ProviderRetryPolicies()}, log)
	intake := sender.NewIntake(inbox, cfg.PrimaryProviders(), cfg.Sender.MaxClaims, attempt.Send, log)
	scan := sender.NewScan(inbox, attempt.Send, cfg.Sender.PollInterval, cfg.Sender.ScanBatch, cfg.Sender.MaxClaims, log)
	s := sender.New(rabbitmq.NewConsumer(mq, cfg.Sender.Prefetch, log), intake, scan)

	return []lifecycle.Component{db, mq, s}, nil
}
