// Package logging builds the process logger: slog on stdout by default.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// New returns a logger that writes to w and tags every record with service.
// format is "json" (the default when empty) or "text".
func New(w io.Writer, service, level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if level != "" {
		if err := lvl.UnmarshalText([]byte(level)); err != nil {
			return nil, fmt.Errorf("log level %q: %w", level, err)
		}
	}
	opts := &slog.HandlerOptions{Level: lvl}

	var h slog.Handler
	switch strings.ToLower(format) {
	case "", "json":
		h = slog.NewJSONHandler(w, opts)
	case "text":
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("log format %q: want json or text", format)
	}
	return slog.New(h).With("service", service), nil
}
