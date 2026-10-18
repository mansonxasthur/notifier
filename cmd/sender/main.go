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
)

func main() { os.Exit(app.Main("sender", build)) }

func build(cfg *config.Config, _ *slog.Logger) ([]lifecycle.Component, error) {
	if err := cfg.Require(config.Postgres, config.RabbitMQ); err != nil {
		return nil, err
	}
	return []lifecycle.Component{postgres.New(cfg.DatabaseURL), rabbitmq.New(cfg.AMQPURL)}, nil
}
