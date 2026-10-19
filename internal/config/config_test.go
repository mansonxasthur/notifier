package config

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

const shippedConfig = "../../config/config.yaml"

func noEnv(string) string { return "" }

func loadShipped(t *testing.T) *Config {
	t.Helper()
	c, err := Load(shippedConfig, noEnv)
	if err != nil {
		t.Fatalf("shipped config must load: %v", err)
	}
	return c
}

func TestShippedConfigDefaults(t *testing.T) {
	c := loadShipped(t)
	for _, ch := range domain.Channels() {
		var primary int
		for _, p := range c.Channels[ch].Providers {
			if p.Primary {
				primary++
			}
			if p.MaxRetries != 1 {
				t.Errorf("%s/%s max_retries = %d, want 1", ch, p.Name, p.MaxRetries)
			}
		}
		if primary != 1 {
			t.Errorf("%s has %d primaries, want 1", ch, primary)
		}
	}
	if !c.Simulation {
		t.Error("shipped config should run in simulation mode")
	}
}

func TestShippedLimits(t *testing.T) {
	c := loadShipped(t)
	if got := c.Limits.Global.Policy(); got.Rate != 500000 || !got.Fits(c.BatchLimit*len(domain.Channels())) {
		t.Errorf("global limit = %+v; want 500000/s and room for the largest request", got)
	}
	limits := c.ProviderRateLimits()
	for name := range c.ProvidersByName() {
		if limits[name].Validate() != nil {
			t.Errorf("provider %s has no usable rate limit: %+v", name, limits[name])
		}
	}
	if len(limits) != 6 {
		t.Errorf("rate limits for %d providers, want 6", len(limits))
	}
}

func TestEnvOverrides(t *testing.T) {
	env := map[string]string{
		"DATABASE_URL": "postgres://db",
		"AMQP_URL":     "amqp://mq",
		"REDIS_URL":    "redis://cache",
		"HTTP_ADDR":    ":9999",
		"API_KEY":      "k",
	}
	c, err := Load(shippedConfig, func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.DatabaseURL != "postgres://db" || c.AMQPURL != "amqp://mq" || c.RedisURL != "redis://cache" {
		t.Errorf("connection strings not read from env: %+v", c)
	}
	if c.APIKey != "k" {
		t.Errorf("API_KEY not read from env: %q", c.APIKey)
	}
	if c.HTTP.Addr != ":9999" {
		t.Errorf("HTTP_ADDR should override http.addr, got %q", c.HTTP.Addr)
	}
}

func TestRequire(t *testing.T) {
	c := loadShipped(t)
	if err := c.Require(Postgres, RabbitMQ, Redis); err == nil {
		t.Fatal("want error for unset connection strings")
	}
	c.DatabaseURL = "postgres://db"
	if err := c.Require(Postgres); err != nil {
		t.Fatalf("database only: %v", err)
	}
	if err := c.Require(Postgres, APIKey); err == nil {
		t.Fatal("want error for unset API_KEY")
	}
}

func TestSecondDocumentRejected(t *testing.T) {
	c := loadShipped(t)
	_ = c
	raw, err := os.ReadFile(shippedConfig)
	if err != nil {
		t.Fatal(err)
	}
	_, err = parse(strings.NewReader(string(raw)+"\n---\nbatch_limit: 1\n"), noEnv)
	if err == nil || !strings.Contains(err.Error(), "more than one") {
		t.Fatalf("want multi-document error, got %v", err)
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	_, err := parse(strings.NewReader("shutdown_timout: 5s\n"), noEnv)
	if err == nil || !strings.Contains(err.Error(), "shutdown_timout") {
		t.Fatalf("want unknown-field error naming the key, got %v", err)
	}
}

func TestMissingFile(t *testing.T) {
	if _, err := Load("does-not-exist.yaml", noEnv); err == nil {
		t.Fatal("want error for a missing file")
	}
}

func TestValidateRejects(t *testing.T) {
	email := func(c *Config) *Channel { cc := c.Channels[domain.ChannelEmail]; return &cc }
	setEmail := func(c *Config, cc *Channel) { c.Channels[domain.ChannelEmail] = *cc }

	tests := []struct {
		name   string
		mutate func(c *Config)
		want   string
	}{
		{"no shutdown timeout", func(c *Config) { c.ShutdownTimeout = 0 }, "shutdown_timeout"},
		{"no listen address", func(c *Config) { c.HTTP.Addr = "" }, "http.addr"},
		{"batch limit zero", func(c *Config) { c.BatchLimit = 0 }, "batch_limit"},
		{"chunk size zero", func(c *Config) { c.Accept.ChunkSize = 0 }, "accept.chunk_size"},
		{"body cap zero", func(c *Config) { c.Accept.MaxBodyBytes = 0 }, "accept.max_body_bytes"},
		{"no notification types", func(c *Config) { c.NotificationTypes = nil }, "notification_types"},
		{"empty type name", func(c *Config) { c.NotificationTypes[""] = NotificationType{Expiry: time.Hour} }, "empty type name"},
		{"type expiry zero", func(c *Config) { c.NotificationTypes["marketing"] = NotificationType{} }, "marketing.expiry"},
		{"simulation without url", func(c *Config) { c.Simulation, c.SimulationURL = true, "" }, "simulation_url"},
		{"real mode without provider url", func(c *Config) {
			c.Simulation = false
			cc := email(c)
			cc.Providers[0].URL = ""
			setEmail(c, cc)
		}, "url is required"},
		{"unknown channel", func(c *Config) { c.Channels["push"] = c.Channels[domain.ChannelEmail] }, "not a known channel"},
		{"missing channel", func(c *Config) { delete(c.Channels, domain.ChannelSMS) }, "channels.sms is required"},
		{"no providers", func(c *Config) { setEmail(c, &Channel{}) }, "providers must not be empty"},
		{"no primary", func(c *Config) {
			cc := email(c)
			cc.Providers[0].Primary = false
			setEmail(c, cc)
		}, "exactly one primary provider, found 0"},
		{"two primaries", func(c *Config) {
			cc := email(c)
			cc.Providers[1].Primary = true
			setEmail(c, cc)
		}, "exactly one primary provider, found 2"},
		{"primary not first", func(c *Config) {
			cc := email(c)
			cc.Providers[0].Primary, cc.Providers[1].Primary = false, true
			setEmail(c, cc)
		}, "not first"},
		{"duplicate provider name", func(c *Config) {
			cc := email(c)
			cc.Providers[1].Name = cc.Providers[0].Name
			setEmail(c, cc)
		}, "used twice"},
		{"provider name empty", func(c *Config) {
			cc := email(c)
			cc.Providers[0].Name = ""
			setEmail(c, cc)
		}, ".name is required"},
		{"provider timeout at target", func(c *Config) {
			cc := email(c)
			cc.Providers[0].Timeout = MaxProviderTimeout
			setEmail(c, cc)
		}, "timeout"},
		{"provider timeout zero", func(c *Config) {
			cc := email(c)
			cc.Providers[0].Timeout = 0
			setEmail(c, cc)
		}, "timeout"},
		{"negative max retries", func(c *Config) {
			cc := email(c)
			cc.Providers[0].MaxRetries = -1
			setEmail(c, cc)
		}, "max_retries"},
		{"jitter above one", func(c *Config) {
			cc := email(c)
			cc.Providers[0].Backoff.Jitter = 1.5
			setEmail(c, cc)
		}, "jitter"},
		{"backoff max below initial", func(c *Config) { c.Relay.Backoff.Max = c.Relay.Backoff.Initial / 2 }, "relay.backoff.max"},
		{"backoff multiplier below one", func(c *Config) { c.Relay.Backoff.Multiplier = 0.5 }, "multiplier"},
		{"relay poll zero", func(c *Config) { c.Relay.PollInterval = 0 }, "relay.poll_interval"},
		{"relay batch zero", func(c *Config) { c.Relay.BatchSize = 0 }, "relay.batch_size"},
		{"relay workers zero", func(c *Config) { c.Relay.Workers = 0 }, "relay.workers"},
		{"relay attempts zero", func(c *Config) { c.Relay.MaxAttempts = 0 }, "relay.max_attempts"},
		{"janitor interval zero", func(c *Config) { c.Relay.Janitor.Interval = 0 }, "janitor.interval"},
		{"janitor outbox threshold zero", func(c *Config) { c.Relay.Janitor.OutboxStuckAfter = 0 }, "outbox_stuck_after"},
		{"janitor margin zero", func(c *Config) { c.Relay.Janitor.InboxStuckMargin = 0 }, "inbox_stuck_margin"},
		{"relay confirm timeout zero", func(c *Config) { c.Relay.ConfirmTimeout = 0 }, "relay.confirm_timeout"},
		{"sender prefetch zero", func(c *Config) { c.Sender.Prefetch = 0 }, "sender.prefetch"},
		{"sender scan batch zero", func(c *Config) { c.Sender.ScanBatch = 0 }, "sender.scan_batch"},
		{"breaker threshold zero", func(c *Config) { c.Sender.Breaker.FailureThreshold = 0 }, "breaker.failure_threshold"},
		{"breaker open_for zero", func(c *Config) { c.Sender.Breaker.OpenFor = 0 }, "breaker.open_for"},
		{"sender poll zero", func(c *Config) { c.Sender.PollInterval = 0 }, "sender.poll_interval"},
		{"webhook unknown auth", func(c *Config) {
			cc := email(c)
			cc.Providers[0].Webhook.Auth = "bogus"
			setEmail(c, cc)
		}, "webhook.auth"},
		{"webhook no header", func(c *Config) {
			cc := email(c)
			cc.Providers[0].Webhook.Header = ""
			setEmail(c, cc)
		}, "webhook.header"},
		{"webhook no secret env", func(c *Config) {
			cc := email(c)
			cc.Providers[0].Webhook.SecretEnv = ""
			setEmail(c, cc)
		}, "webhook.secret_env"},
		{"simprovider unknown mode", func(c *Config) { c.SimProvider.Mode = "chaos" }, "simprovider.mode"},
		{"simprovider unknown weight", func(c *Config) { c.SimProvider.Weights["chaos"] = 1 }, "not a known scenario"},
		{"simprovider negative weight", func(c *Config) { c.SimProvider.Weights[domain.ScenarioSlow] = -1 }, "must not be negative"},
		{"simprovider random without weights", func(c *Config) { c.SimProvider.Weights = nil }, "positive weight"},
		{"simprovider slow delay zero", func(c *Config) { c.SimProvider.SlowDelay = 0 }, "slow_delay"},
		{"simprovider timeout delay zero", func(c *Config) { c.SimProvider.TimeoutDelay = 0 }, "timeout_delay"},
		{"simprovider webhook delay zero", func(c *Config) { c.SimProvider.WebhookDelay = 0 }, "webhook_delay"},
		{"simprovider no webhook url", func(c *Config) { c.SimProvider.WebhookURL = "" }, "webhook_url"},
		{"duplicate provider name across channels", func(c *Config) {
			cc := c.Channels[domain.ChannelSMS]
			cc.Providers[0].Name = "email-a"
			c.Channels[domain.ChannelSMS] = cc
		}, "more than one channel"},
		{"slow delay not below timeout", func(c *Config) { c.SimProvider.SlowDelay = 2 * time.Second }, "slow_delay must be below"},
		{"timeout delay not above timeout", func(c *Config) { c.SimProvider.TimeoutDelay = time.Second }, "timeout_delay must exceed"},
		{"global rate zero", func(c *Config) { c.Limits.Global.Rate = 0 }, "limits.global"},
		{"global burst zero", func(c *Config) { c.Limits.Global.Burst = 0 }, "limits.global"},
		{"global burst below largest request", func(c *Config) {
			c.Limits.Global.Burst = c.BatchLimit*len(domain.Channels()) - 1
		}, "limits.global.burst must be at least"},
		{"provider rate zero", func(c *Config) {
			cc := email(c)
			cc.Providers[0].RateLimit.Rate = 0
			setEmail(c, cc)
		}, "rate_limit"},
		{"provider burst zero", func(c *Config) {
			cc := email(c)
			cc.Providers[1].RateLimit.Burst = 0
			setEmail(c, cc)
		}, "rate_limit"},
		{"sender deferrals zero", func(c *Config) { c.Sender.MaxDeferrals = 0 }, "sender.max_deferrals"},
		{"sender claims zero", func(c *Config) { c.Sender.MaxClaims = 0 }, "sender.max_claims"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := loadShipped(t)
			tt.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatal("want a validation error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err, tt.want)
			}
		})
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	c := loadShipped(t)
	c.BatchLimit = 0
	c.Sender.MaxClaims = 0
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "batch_limit") || !strings.Contains(err.Error(), "max_claims") {
		t.Fatalf("want both problems reported, got %v", err)
	}
}

func TestPinnedScenarioNeedsNoWeights(t *testing.T) {
	c := loadShipped(t)
	c.SimProvider.Mode, c.SimProvider.Weights = string(domain.ScenarioLateFailure), nil
	if err := c.Validate(); err != nil {
		t.Fatalf("a pinned scenario should not need weights: %v", err)
	}
}

func TestWebhookSecretsFromEnv(t *testing.T) {
	c := loadShipped(t)
	if err := c.RequireWebhookSecrets(); err == nil || !strings.Contains(err.Error(), "WEBHOOK_SECRET_EMAIL_A") {
		t.Fatalf("want every unset secret reported, got %v", err)
	}

	env := func(k string) string {
		if strings.HasPrefix(k, "WEBHOOK_SECRET_") {
			return "s-" + k
		}
		return ""
	}
	c, err := Load(shippedConfig, env)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.WebhookSecrets["email-a"].Reveal(); got != "s-WEBHOOK_SECRET_EMAIL_A" {
		t.Errorf("email-a secret = %q", got)
	}
	if err := c.RequireWebhookSecrets(); err != nil {
		t.Errorf("all secrets set, got %v", err)
	}
}

func TestSimProviderIgnoredWhenNotSimulating(t *testing.T) {
	c := loadShipped(t)
	c.Simulation, c.SimProvider = false, SimProvider{}
	for _, cc := range c.Channels {
		for i := range cc.Providers {
			cc.Providers[i].URL = "https://provider.example.com"
		}
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("simprovider settings must not be required when simulation is off: %v", err)
	}
}

func TestSecretIsRedacted(t *testing.T) {
	s := Secret("hunter2")
	for _, got := range []string{fmt.Sprint(s), fmt.Sprintf("%+v", s), fmt.Sprintf("%#v", s), fmt.Sprint(map[string]Secret{"a": s})} {
		if strings.Contains(got, "hunter2") {
			t.Errorf("secret leaked: %s", got)
		}
	}
}

// The compose file is a second place that names each provider's webhook secret variable.
// A provider added to config.yaml without a matching entry there would crash the api and
// simprovider on `make up`.
func TestComposeSetsEveryWebhookSecret(t *testing.T) {
	compose, err := os.ReadFile("../../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	for _, cc := range loadShipped(t).Channels {
		for _, p := range cc.Providers {
			if !strings.Contains(string(compose), "  "+p.Webhook.SecretEnv+":") {
				t.Errorf("docker-compose.yml does not set %s (webhook secret of %s)", p.Webhook.SecretEnv, p.Name)
			}
		}
	}
}
