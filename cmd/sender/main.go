// Command sender consumes channel queues and makes one provider call per attempt.
package main

import (
	"log/slog"
	"os"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
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

	intake := sender.NewIntake(postgres.NewInboxStore(db), cfg.PrimaryProviders(), cfg.Sender.MaxClaims, sender.LogOnly(log), log)
	s := sender.New(rabbitmq.NewConsumer(mq, cfg.Sender.Prefetch, log), intake)

	return []lifecycle.Component{db, mq, s}, nil
}
