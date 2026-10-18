// Package simprovider is the simulated provider service: it answers sends in configurable
// scenarios, records the scenario it chose for every delivery, serves status lookups, and
// sends signed webhooks back to the API.
package simprovider

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
)

// Chooser picks the scenario for a new delivery.
type Chooser struct {
	mode    string
	weights map[domain.Scenario]int
	total   int

	mu  sync.Mutex
	rng *rand.Rand
}

// NewChooser returns a Chooser for the configured mode. In random mode a zero seed seeds
// from the clock.
func NewChooser(c config.SimProvider) *Chooser {
	seed := uint64(c.Seed) //nolint:gosec // a seed, not a size
	if seed == 0 {
		seed = uint64(time.Now().UnixNano()) //nolint:gosec // a seed, not a size
	}
	ch := &Chooser{mode: c.Mode, weights: c.Weights, rng: rand.New(rand.NewPCG(seed, seed))} //nolint:gosec // the simulator needs no secure randomness
	for _, w := range c.Weights {
		ch.total += max(w, 0)
	}
	return ch
}

// Pick returns the scenario for the next new delivery.
func (c *Chooser) Pick() domain.Scenario {
	if c.mode != config.RandomMode {
		return domain.Scenario(c.mode)
	}
	c.mu.Lock()
	n := c.rng.IntN(c.total)
	c.mu.Unlock()

	// Scenarios() has a fixed order, so a seed gives the same sequence on every run.
	for _, s := range domain.Scenarios() {
		w := max(c.weights[s], 0)
		if n < w {
			return s
		}
		n -= w
	}
	return domain.ScenarioSucceed // unreachable: n < total
}
