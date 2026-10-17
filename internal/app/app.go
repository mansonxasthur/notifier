// Package app is the shared process skeleton for every entry point: config, logger,
// signal handling, and ordered startup and shutdown.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/lifecycle"
	"github.com/mansonxasthur/notifier/internal/logging"
)

const defaultConfigPath = "config/config.yaml"

// Build assembles the components of one entry point, in start order.
type Build func(cfg *config.Config, log *slog.Logger) ([]lifecycle.Component, error)

// Main runs an entry point and returns its process exit code: 0 after a clean signal
// shutdown, 1 for any startup or shutdown failure.
func Main(service string, build Build) int {
	log, err := logging.New(os.Stdout, service, os.Getenv("LOG_LEVEL"), os.Getenv("LOG_FORMAT"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "logging: %v\n", err)
		return 1
	}

	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = defaultConfigPath
	}
	cfg, err := config.Load(path, os.Getenv)
	if err != nil {
		log.Error("load config", "error", err)
		return 1
	}
	log.Info("config loaded", "path", path, "simulation", cfg.Simulation)

	comps, err := build(cfg, log)
	if err != nil {
		log.Error("build", "error", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := lifecycle.Run(ctx, log, cfg.ShutdownTimeout, comps...); err != nil {
		log.Error("exit", "error", err)
		return 1
	}
	log.Info("exited cleanly")
	return 0
}
