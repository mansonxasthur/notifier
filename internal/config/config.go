// Package config loads and validates the YAML configuration shared by all entry points.
package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
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
	Accept            Accept                      `yaml:"accept"`
	NotificationTypes map[string]NotificationType `yaml:"notification_types"`
	Simulation        bool                        `yaml:"simulation"`
	SimulationURL     string                      `yaml:"simulation_url"`
	SimProvider       SimProvider                 `yaml:"simprovider"`
	Channels          map[domain.Channel]Channel  `yaml:"channels"`
	Relay             Relay                       `yaml:"relay"`
	Sender            Sender                      `yaml:"sender"`

	// Connection strings come from the environment only.
	DatabaseURL string `yaml:"-"`
	AMQPURL     string `yaml:"-"`
	RedisURL    string `yaml:"-"`

	// APIKey is the one key producers send in X-API-KEY. It comes from the environment only.
	APIKey string `yaml:"-"`

	// WebhookSecrets holds each provider's webhook secret by provider name, read from the
	// environment variable that provider's webhook.secret_env names.
	WebhookSecrets map[string]Secret `yaml:"-"`
}

// Secret is a credential that prints as "[redacted]" in formatted output and logs.
// Reveal returns the value.
type Secret string

func (Secret) String() string       { return "[redacted]" }
func (Secret) GoString() string     { return "[redacted]" }
func (Secret) LogValue() slog.Value { return slog.StringValue("[redacted]") }
func (s Secret) Reveal() string     { return string(s) }

// Accept tunes the accept path.
type Accept struct {
	// ChunkSize is the most delivery rows one insert statement carries.
	ChunkSize int `yaml:"chunk_size"`
	// MaxBodyBytes caps a request body.
	MaxBodyBytes int64 `yaml:"max_body_bytes"`
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
	Webhook      Webhook       `yaml:"webhook"`
}

// WebhookAuth names how a provider authenticates the webhooks it sends us.
type WebhookAuth string

const (
	// WebhookHMAC: Header carries the hex HMAC-SHA256 of the request body, keyed by the secret.
	WebhookHMAC WebhookAuth = "hmac_signature"
	// WebhookSharedSecret: Header carries the secret itself.
	WebhookSharedSecret WebhookAuth = "shared_secret_header"
)

// Valid reports whether a is a known scheme.
func (a WebhookAuth) Valid() bool { return a == WebhookHMAC || a == WebhookSharedSecret }

// Webhook is how one provider's webhooks are authenticated.
type Webhook struct {
	Auth   WebhookAuth `yaml:"auth"`
	Header string      `yaml:"header"`
	// SecretEnv is the name of the environment variable that holds the secret.
	SecretEnv string `yaml:"secret_env"`
}

// SimProvider configures the simulated provider service.
type SimProvider struct {
	// Mode is "random", which draws a scenario per request from Weights, or the name of one
	// scenario applied to every request.
	Mode string `yaml:"mode"`
	// Seed makes random mode reproducible; zero seeds from the clock.
	Seed    int64                   `yaml:"seed"`
	Weights map[domain.Scenario]int `yaml:"weights"`
	// SlowDelay is how long a slow call takes; keep it under the providers' timeouts.
	SlowDelay time.Duration `yaml:"slow_delay"`
	// TimeoutDelay is how long a timing-out call hangs; keep it over the providers' timeouts.
	TimeoutDelay time.Duration `yaml:"timeout_delay"`
	// WebhookDelay is the wait before a delivery or late-failure webhook is sent.
	WebhookDelay time.Duration `yaml:"webhook_delay"`
	// WebhookURL is the API's base URL; webhooks go to <WebhookURL>/v1/webhooks/<provider>.
	WebhookURL string `yaml:"webhook_url"`
}

// RandomMode is the SimProvider mode that draws a scenario per request.
const RandomMode = "random"

type Backoff struct {
	Initial    time.Duration `yaml:"initial"`
	Max        time.Duration `yaml:"max"`
	Multiplier float64       `yaml:"multiplier"`
	Jitter     float64       `yaml:"jitter"`
}

type Relay struct {
	PollInterval time.Duration `yaml:"poll_interval"`
	BatchSize    int           `yaml:"batch_size"`
	// Workers is how many claim-and-publish loops one relay runs.
	Workers     int `yaml:"workers"`
	MaxAttempts int `yaml:"max_attempts"`
	// ConfirmTimeout bounds the wait for the broker to confirm a published batch.
	ConfirmTimeout time.Duration `yaml:"confirm_timeout"`
	Backoff        Backoff       `yaml:"backoff"`
	Janitor        Janitor       `yaml:"janitor"`
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
	// Prefetch is how many unacknowledged messages a sender holds per queue.
	Prefetch int `yaml:"prefetch"`
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
	c.APIKey = getenv("API_KEY")
	if addr := getenv("HTTP_ADDR"); addr != "" {
		c.HTTP.Addr = addr
	}

	c.WebhookSecrets = map[string]Secret{}
	for _, cc := range c.Channels {
		for _, p := range cc.Providers {
			if p.Webhook.SecretEnv != "" {
				c.WebhookSecrets[p.Name] = Secret(getenv(p.Webhook.SecretEnv))
			}
		}
	}

	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &c, nil
}

// Dependency names a connection string an entry point needs.
type Dependency string

const (
	Postgres Dependency = "DATABASE_URL"
	RabbitMQ Dependency = "AMQP_URL"
	Redis    Dependency = "REDIS_URL"
	APIKey   Dependency = "API_KEY"
)

// Require fails when a connection string the calling entry point needs is unset.
func (c *Config) Require(deps ...Dependency) error {
	urls := map[Dependency]string{Postgres: c.DatabaseURL, RabbitMQ: c.AMQPURL, Redis: c.RedisURL, APIKey: c.APIKey}
	var errs []error
	for _, d := range deps {
		if urls[d] == "" {
			errs = append(errs, fmt.Errorf("%s is required", d))
		}
	}
	return errors.Join(errs...)
}

// RequireWebhookSecrets fails when a provider's webhook secret environment variable is unset.
// Entry points that verify or sign webhooks call it; an empty secret must never authenticate.
func (c *Config) RequireWebhookSecrets() error {
	var missing []string
	for _, cc := range c.Channels {
		for _, p := range cc.Providers {
			if c.WebhookSecrets[p.Name] == "" {
				missing = append(missing, fmt.Sprintf("%s (webhook secret for %s)", p.Webhook.SecretEnv, p.Name))
			}
		}
	}
	sort.Strings(missing)
	errs := make([]error, len(missing))
	for i, m := range missing {
		errs[i] = fmt.Errorf("%s is required", m)
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
	if c.Accept.ChunkSize <= 0 {
		bad("accept.chunk_size must be positive")
	}
	if c.Accept.MaxBodyBytes <= 0 {
		bad("accept.max_body_bytes must be positive")
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

	if c.Simulation {
		errs = append(errs, c.SimProvider.validate(c.providerTimeouts())...)
	}
	names := map[string]bool{}
	for _, ch := range domain.Channels() {
		for _, p := range c.Channels[ch].Providers {
			if p.Name != "" && names[p.Name] {
				bad("provider name %q is used in more than one channel; webhook routes and secrets are keyed by it", p.Name)
			}
			names[p.Name] = true
		}
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
	if c.Sender.Prefetch <= 0 {
		bad("sender.prefetch must be positive")
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
		errs = append(errs, p.Webhook.validate(at+".webhook")...)
	}
	if primaries != 1 {
		bad("channels.%s needs exactly one primary provider, found %d", name, primaries)
	}
	return errs
}

func (w Webhook) validate(at string) []error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if !w.Auth.Valid() {
		bad("%s.auth %q is not a known scheme (%s or %s)", at, w.Auth, WebhookHMAC, WebhookSharedSecret)
	}
	if w.Header == "" {
		bad("%s.header is required", at)
	}
	if w.SecretEnv == "" {
		bad("%s.secret_env is required", at)
	}
	return errs
}

// providerTimeouts returns the shortest and longest configured provider timeout.
func (c *Config) providerTimeouts() (shortest, longest time.Duration) {
	for _, cc := range c.Channels {
		for _, p := range cc.Providers {
			if p.Timeout <= 0 {
				continue
			}
			if shortest == 0 || p.Timeout < shortest {
				shortest = p.Timeout
			}
			longest = max(longest, p.Timeout)
		}
	}
	return shortest, longest
}

func (s SimProvider) validate(shortestTimeout, longestTimeout time.Duration) []error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if s.Mode != RandomMode && !domain.Scenario(s.Mode).Valid() {
		bad("simprovider.mode %q must be %q or a scenario name", s.Mode, RandomMode)
	}
	total := 0
	for sc, w := range s.Weights {
		if !sc.Valid() {
			bad("simprovider.weights.%s is not a known scenario", sc)
		}
		if w < 0 {
			bad("simprovider.weights.%s must not be negative", sc)
		}
		total += w
	}
	if s.Mode == RandomMode && total <= 0 {
		bad("simprovider.weights needs a positive weight when mode is %q", RandomMode)
	}
	if s.SlowDelay <= 0 {
		bad("simprovider.slow_delay must be positive")
	}
	if shortestTimeout > 0 && s.SlowDelay >= shortestTimeout {
		bad("simprovider.slow_delay must be below the shortest provider timeout (%s)", shortestTimeout)
	}
	if s.TimeoutDelay <= 0 {
		bad("simprovider.timeout_delay must be positive")
	}
	if s.TimeoutDelay > 0 && s.TimeoutDelay <= longestTimeout {
		bad("simprovider.timeout_delay must exceed the longest provider timeout (%s)", longestTimeout)
	}
	if s.WebhookDelay <= 0 {
		bad("simprovider.webhook_delay must be positive")
	}
	if s.WebhookURL == "" {
		bad("simprovider.webhook_url is required")
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
	if r.Workers <= 0 {
		bad("relay.workers must be positive")
	}
	if r.MaxAttempts <= 0 {
		bad("relay.max_attempts must be positive")
	}
	if r.ConfirmTimeout <= 0 {
		bad("relay.confirm_timeout must be positive")
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

// Policy converts the configured backoff to the domain type.
func (b Backoff) Policy() domain.Backoff {
	return domain.Backoff{Initial: b.Initial, Max: b.Max, Multiplier: b.Multiplier, Jitter: b.Jitter}
}

// ProviderBackoffs maps each provider name to its retry backoff policy.
func (c *Config) ProviderBackoffs() map[string]domain.Backoff {
	out := map[string]domain.Backoff{}
	for _, cc := range c.Channels {
		for _, p := range cc.Providers {
			out[p.Name] = p.Backoff.Policy()
		}
	}
	return out
}

// PrimaryProviders maps each channel to the name of its primary provider.
func (c *Config) PrimaryProviders() map[domain.Channel]string {
	out := make(map[domain.Channel]string, len(c.Channels))
	for ch, cc := range c.Channels {
		for _, p := range cc.Providers {
			if p.Primary {
				out[ch] = p.Name
			}
		}
	}
	return out
}
