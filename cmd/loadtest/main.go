// Command loadtest sends a fixed rate of notification requests to a running stack and
// records how the system held up. See docs/results/README.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/loadtest"
)

func main() { os.Exit(run()) }

func run() int {
	var (
		cfg       loadtest.Config
		channels  string
		primaries string
		targets   targetFlag
	)
	flag.StringVar(&cfg.URL, "url", "http://localhost:8080", "base URL of the API")
	flag.StringVar(&cfg.APIKey, "key", "dev-api-key", "API key")
	flag.IntVar(&cfg.Rate, "rate", 100, "recipients per second")
	flag.IntVar(&cfg.Batch, "batch", 100, "recipients per request")
	flag.StringVar(&channels, "channels", "sms,email,whatsapp", "channels every recipient has")
	flag.DurationVar(&cfg.Warmup, "warmup", 15*time.Second, "warm-up, left out of the results")
	flag.DurationVar(&cfg.Duration, "duration", 60*time.Second, "measured load")
	flag.IntVar(&cfg.MaxInFlight, "max-in-flight", 2000, "requests awaiting an answer before the driver skips one")
	flag.StringVar(&primaries, "primaries", "sms-a,email-a,whatsapp-a", "providers a first attempt goes to")
	flag.StringVar(&cfg.DatabaseURL, "db", os.Getenv("DATABASE_URL"), "Postgres URL (default $DATABASE_URL)")
	flag.Var(&targets, "target", "metrics target as name=url, repeatable (names: api, relay, sender)")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", 120*time.Second, "how long to wait for PENDING deliveries to settle")
	flag.StringVar(&cfg.OutDir, "out", "", "directory for the results (required)")
	flag.StringVar(&cfg.Label, "label", "load run", "title of the run")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg.Args = os.Args[1:]
	cfg.Targets = targets.m
	cfg.Primaries = strings.Split(primaries, ",")
	for _, c := range strings.Split(channels, ",") {
		ch := domain.Channel(strings.TrimSpace(c))
		if !ch.Valid() {
			fmt.Fprintf(os.Stderr, "unknown channel %q\n", c)
			return 2
		}
		cfg.Channels = append(cfg.Channels, ch)
	}
	switch {
	case cfg.OutDir == "":
		fmt.Fprintln(os.Stderr, "-out is required")
		return 2
	case cfg.DatabaseURL == "":
		fmt.Fprintln(os.Stderr, "-db or DATABASE_URL is required")
		return 2
	case cfg.Rate <= 0 || cfg.Batch <= 0 || cfg.Batch > cfg.Rate:
		fmt.Fprintln(os.Stderr, "-rate and -batch must be positive, and -batch no larger than -rate")
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	out, err := loadtest.Run(ctx, cfg, log)
	if err != nil {
		log.Error("run failed", "error", err)
		return 1
	}
	fmt.Print(loadtest.Summary(cfg, out))
	return 0
}

// targetFlag collects repeated -target name=url flags.
type targetFlag struct{ m map[string]string }

func (t *targetFlag) String() string { return fmt.Sprint(t.m) }

func (t *targetFlag) Set(v string) error {
	name, url, ok := strings.Cut(v, "=")
	if !ok || name == "" || url == "" {
		return fmt.Errorf("want name=url, got %q", v)
	}
	if t.m == nil {
		t.m = map[string]string{}
	}
	t.m[name] = url
	return nil
}
