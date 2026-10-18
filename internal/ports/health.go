// Package ports holds the interfaces the core needs from the outside world.
package ports

import "context"

// Pinger reports whether a dependency is reachable.
type Pinger interface {
	Ping(ctx context.Context) error
}
