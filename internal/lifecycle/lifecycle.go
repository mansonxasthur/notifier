// Package lifecycle starts components in order and stops them in reverse on shutdown.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

// Component is one piece of a process: a connection, a server, a worker loop.
// Start must return once the component is running, not block for its lifetime.
type Component interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Failer is implemented by components that can die after Start returned, such as a server
// or a worker loop. A value on the channel ends the process with that error.
type Failer interface {
	Failed() <-chan error
}

// Run starts the components in order and waits for ctx to be cancelled, then stops them
// in reverse order within shutdownTimeout. If a component fails to start, the ones already
// started are stopped and the start error is returned.
func Run(ctx context.Context, log *slog.Logger, shutdownTimeout time.Duration, comps ...Component) error {
	started := make([]Component, 0, len(comps))
	failed := make(chan error, len(comps))

	for _, c := range comps {
		log.Info("starting", "component", c.Name())
		if err := c.Start(ctx); err != nil {
			startErr := fmt.Errorf("start %s: %w", c.Name(), err)
			return errors.Join(startErr, stopAll(log, shutdownTimeout, started))
		}
		started = append(started, c)
		if f, ok := c.(Failer); ok {
			go func() {
				if err := <-f.Failed(); err != nil {
					failed <- fmt.Errorf("%s: %w", c.Name(), err)
				}
			}()
		}
	}

	log.Info("running")
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-failed:
		log.Error("component failed", "error", runErr)
	}
	log.Info("stopping")

	return errors.Join(runErr, stopAll(log, shutdownTimeout, started))
}

// stopAll uses a fresh context: the run context is already cancelled by now.
func stopAll(log *slog.Logger, timeout time.Duration, started []Component) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var errs []error
	for _, c := range slices.Backward(started) {
		if err := c.Stop(ctx); err != nil {
			log.Error("stop failed", "component", c.Name(), "error", err)
			errs = append(errs, fmt.Errorf("stop %s: %w", c.Name(), err))
			continue
		}
		log.Info("stopped", "component", c.Name())
	}
	return errors.Join(errs...)
}
