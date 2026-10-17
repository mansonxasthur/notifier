package config

import (
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

func TestEnvOverrides(t *testing.T) {
	env := map[string]string{
		"DATABASE_URL": "postgres://db",
		"AMQP_URL":     "amqp://mq",
		"REDIS_URL":    "redis://cache",
		"HTTP_ADDR":    ":9999",
	}
	c, err := Load(shippedConfig, func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.DatabaseURL != "postgres://db" || c.AMQPURL != "amqp://mq" || c.RedisURL != "redis://cache" {
		t.Errorf("connection strings not read from env: %+v", c)
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
		{"relay attempts zero", func(c *Config) { c.Relay.MaxAttempts = 0 }, "relay.max_attempts"},
		{"janitor interval zero", func(c *Config) { c.Relay.Janitor.Interval = 0 }, "janitor.interval"},
		{"janitor outbox threshold zero", func(c *Config) { c.Relay.Janitor.OutboxStuckAfter = 0 }, "outbox_stuck_after"},
		{"janitor margin zero", func(c *Config) { c.Relay.Janitor.InboxStuckMargin = 0 }, "inbox_stuck_margin"},
		{"sender poll zero", func(c *Config) { c.Sender.PollInterval = 0 }, "sender.poll_interval"},
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
