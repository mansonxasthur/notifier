// Package httpserver wraps net/http as a lifecycle component.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

type Server struct {
	name string
	log  *slog.Logger
	srv  *http.Server
	errc chan error
}

func New(name, addr string, handler http.Handler, log *slog.Logger) *Server {
	return &Server{
		name: name,
		log:  log,
		errc: make(chan error, 1),
		srv: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
		},
	}
}

func (s *Server) Name() string { return s.name }

// Start binds the port and serves in the background, so a bad address fails startup.
func (s *Server) Start(ctx context.Context) error {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.srv.Addr, err)
	}
	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.errc <- err
		}
	}()
	s.log.Info("listening", "component", s.name, "addr", ln.Addr().String())
	return nil
}

// Failed delivers an error if the server stops serving unexpectedly.
func (s *Server) Failed() <-chan error { return s.errc }

// Stop stops accepting connections and waits for in-flight requests until ctx ends.
func (s *Server) Stop(ctx context.Context) error { return s.srv.Shutdown(ctx) }
