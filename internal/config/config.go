// Package config loads and validates the YAML configuration shared by all entry points.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// MaxProviderTimeout is the request-to-ACCEPTED target. A single provider call that may
// take this long can never meet it, so configuration refuses such a timeout. Retries and
// failover add to the worst case; the load test tunes those, not this guard.
const MaxProviderTimeout = 5 * time.Second

type Config struct {
	ShutdownTimeout   time.Duration               `yaml:"shutdown_timeout"`
	HTTP              HTTP                        `yaml:"http"`
	BatchLimit        int                         `yaml:"batch_limit"`
	NotificationTypes map[string]NotificationType `yaml:"notification_types"`
	Simulation        bool                        `yaml:"simulation"`
	SimulationURL     string                      `yaml:"simulation_url"`
	Channels          map[domain.Channel]Channel  `yaml:"channels"`
	Relay             Relay                       `yaml:"relay"`
	Sender            Sender                      `yaml:"sender"`

	// Connection strings come from the environment only.
	DatabaseURL string `yaml:"-"`
	AMQPURL     string `yaml:"-"`
	RedisURL    string `yaml:"-"`
}

type HTTP struct {
	Addr string `yaml:"addr"`
}

// NotificationType holds the per-type defaults, such as how long a delivery may wait.
type NotificationType struct {
	Expiry time.Duration `yaml:"expiry"`
}

type Channel struct {
	// Providers are in try order. Exactly one is primary; the rest are fallbacks.
	Providers []Provider `yaml:"providers"`
}

type Provider struct {
	Name         string        `yaml:"name"`
	Primary      bool          `yaml:"primary"`
	URL          string        `yaml:"url"`
	Timeout      time.Duration `yaml:"timeout"`
	StatusLookup bool          `yaml:"status_lookup"`
	MaxRetries   int           `yaml:"max_retries"`
	Backoff      Backoff       `yaml:"backoff"`
}

type Backoff struct {
	Initial    time.Duration `yaml:"initial"`
	Max        time.Duration `yaml:"max"`
	Multiplier float64       `yaml:"multiplier"`
	Jitter     float64       `yaml:"jitter"`
}

type Relay struct {
	PollInterval time.Duration `yaml:"poll_interval"`
	BatchSize    int           `yaml:"batch_size"`
	MaxAttempts  int           `yaml:"max_attempts"`
	Backoff      Backoff       `yaml:"backoff"`
	Janitor      Janitor       `yaml:"janitor"`
}

type Janitor struct {
	Interval         time.Duration `yaml:"interval"`
	OutboxStuckAfter time.Duration `yaml:"outbox_stuck_after"`
	// InboxStuckMargin is added to a provider's timeout to get the inbox threshold.
	InboxStuckMargin time.Duration `yaml:"inbox_stuck_margin"`
}

type Sender struct {
	PollInterval time.Duration `yaml:"poll_interval"`
	MaxClaims    int           `yaml:"max_claims"`
}

// Load reads the YAML file at path, applies environment overrides, and validates the result.
func Load(path string, getenv func(string) string) (*Config, error) {
	f, err := os.Open(path) //nolint:gosec // the path is operator-supplied configuration
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = f.Close() }()
	return parse(f, getenv)
}

func parse(r io.Reader, getenv func(string) string) (*Config, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := dec.Decode(&yaml.Node{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("parse config: more than one YAML document")
	}

	c.DatabaseURL = getenv("DATABASE_URL")
	c.AMQPURL = getenv("AMQP_URL")
	c.RedisURL = getenv("REDIS_URL")
	if addr := getenv("HTTP_ADDR"); addr != "" {
		c.HTTP.Addr = addr
	}

	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &c, nil
}

// Require fails when a connection string the calling entry point needs is unset.
func (c *Config) Require(database, amqp, redis bool) error {
	var errs []error
	if database && c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if amqp && c.AMQPURL == "" {
		errs = append(errs, errors.New("AMQP_URL is required"))
	}
	if redis && c.RedisURL == "" {
		errs = append(errs, errors.New("REDIS_URL is required"))
	}
	return errors.Join(errs...)
}

// Validate reports every problem it finds, not just the first.
func (c *Config) Validate() error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.ShutdownTimeout <= 0 {
		bad("shutdown_timeout must be positive")
	}
	if c.HTTP.Addr == "" {
		bad("http.addr is required")
	}
	if c.BatchLimit <= 0 {
		bad("batch_limit must be positive")
	}

	if len(c.NotificationTypes) == 0 {
		bad("notification_types must not be empty")
	}
	for name, t := range c.NotificationTypes {
		if name == "" {
			bad("notification_types has an empty type name")
		}
		if t.Expiry <= 0 {
			bad("notification_types.%s.expiry must be positive", name)
		}
	}

	if c.Simulation && c.SimulationURL == "" {
		bad("simulation_url is required when simulation is true")
	}

	for ch := range c.Channels {
		if !ch.Valid() {
			bad("channels.%s is not a known channel", ch)
		}
	}
	for _, ch := range domain.Channels() {
		cc, ok := c.Channels[ch]
		if !ok {
			bad("channels.%s is required", ch)
			continue
		}
		errs = append(errs, cc.validate(string(ch), c.Simulation)...)
	}

	errs = append(errs, c.Relay.validate()...)
	if c.Sender.PollInterval <= 0 {
		bad("sender.poll_interval must be positive")
	}
	if c.Sender.MaxClaims <= 0 {
		bad("sender.max_claims must be positive")
	}

	return errors.Join(errs...)
}

func (cc Channel) validate(name string, simulation bool) []error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if len(cc.Providers) == 0 {
		bad("channels.%s.providers must not be empty", name)
		return errs
	}

	primaries := 0
	seen := map[string]bool{}
	for i, p := range cc.Providers {
		at := fmt.Sprintf("channels.%s.providers[%d]", name, i)
		if p.Name == "" {
			bad("%s.name is required", at)
		} else {
			if seen[p.Name] {
				bad("%s.name %q is used twice", at, p.Name)
			}
			seen[p.Name] = true
		}

		if p.Primary {
			primaries++
			if i != 0 {
				bad("%s is primary but not first; providers are tried in list order", at)
			}
		}
		if !simulation && p.URL == "" {
			bad("%s.url is required when simulation is false", at)
		}
		if p.Timeout <= 0 || p.Timeout >= MaxProviderTimeout {
			bad("%s.timeout must be positive and below %s", at, MaxProviderTimeout)
		}
		if p.MaxRetries < 0 {
			bad("%s.max_retries must not be negative", at)
		}
		errs = append(errs, p.Backoff.validate(at+".backoff")...)
	}
	if primaries != 1 {
		bad("channels.%s needs exactly one primary provider, found %d", name, primaries)
	}
	return errs
}

func (b Backoff) validate(at string) []error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if b.Initial <= 0 {
		bad("%s.initial must be positive", at)
	}
	if b.Max < b.Initial {
		bad("%s.max must not be below initial", at)
	}
	if !(b.Multiplier >= 1) {
		bad("%s.multiplier must be at least 1", at)
	}
	if !(b.Jitter >= 0 && b.Jitter <= 1) {
		bad("%s.jitter must be between 0 and 1", at)
	}
	return errs
}

func (r Relay) validate() []error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if r.PollInterval <= 0 {
		bad("relay.poll_interval must be positive")
	}
	if r.BatchSize <= 0 {
		bad("relay.batch_size must be positive")
	}
	if r.MaxAttempts <= 0 {
		bad("relay.max_attempts must be positive")
	}
	errs = append(errs, r.Backoff.validate("relay.backoff")...)
	if r.Janitor.Interval <= 0 {
		bad("relay.janitor.interval must be positive")
	}
	if r.Janitor.OutboxStuckAfter <= 0 {
		bad("relay.janitor.outbox_stuck_after must be positive")
	}
	if r.Janitor.InboxStuckMargin <= 0 {
		bad("relay.janitor.inbox_stuck_margin must be positive")
	}
	return errs
}
