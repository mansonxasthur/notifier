package loadtest

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// Config is one run.
type Config struct {
	URL, APIKey string
	// Rate is recipients per second; Batch recipients per request.
	Rate, Batch int
	Channels    []domain.Channel
	Warmup      time.Duration
	Duration    time.Duration
	MaxInFlight int
	// Primaries are the providers a first attempt goes to.
	Primaries    []string
	DatabaseURL  string
	Targets      map[string]string
	DrainTimeout time.Duration
	OutDir       string
	// Label names the run in its summary; Args records how the driver was started.
	Label string
	Args  []string
}

// Outcome is everything one run measured.
type Outcome struct {
	RunID           string
	Started         time.Time
	Warmup, Measure Stats
	Results         Results
	Scraper         *Scraper
	ScrapeEvery     time.Duration
	MeasuredFor     time.Duration
	SentRecipients  int
	DrainTook       time.Duration
}

// Run sends the warm-up and the measured load, waits for the system to drain, reads the
// result from the database and writes the run's files to cfg.OutDir.
func Run(ctx context.Context, cfg Config, log *slog.Logger) (Outcome, error) {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return Outcome{}, fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return Outcome{}, fmt.Errorf("database not reachable at the -db URL within 10s: %w", err)
	}
	if err := os.MkdirAll(cfg.OutDir, 0o750); err != nil {
		return Outcome{}, err
	}

	out := Outcome{RunID: fmt.Sprintf("r%d", time.Now().Unix()), Started: time.Now(), ScrapeEvery: time.Second}
	s := &Sender{URL: cfg.URL, APIKey: cfg.APIKey, RunID: out.RunID, Rate: cfg.Rate, Batch: cfg.Batch,
		Channels: cfg.Channels, MaxInFlight: cfg.MaxInFlight}

	scrapeCtx, stopScrape := context.WithCancel(ctx)
	out.Scraper = NewScraper(cfg.Targets, out.ScrapeEvery)
	scraped := make(chan struct{})
	go func() { out.Scraper.Run(scrapeCtx); close(scraped) }()

	n := 0
	if cfg.Warmup > 0 {
		log.Info("warm-up", "for", cfg.Warmup, "run", out.RunID)
		out.Warmup, n = s.Phase(ctx, Warmup, cfg.Warmup, n)
	}
	log.Info("measuring", "for", cfg.Duration, "recipients_per_s", cfg.Rate)
	t := time.Now()
	out.Measure, _ = s.Phase(ctx, Measure, cfg.Duration, n)
	out.MeasuredFor = time.Since(t)
	out.SentRecipients = out.Measure.Requests * cfg.Batch

	reader := &Reader{Pool: pool, Primaries: cfg.Primaries}
	prefix := RecipientPrefix(out.RunID, Measure)
	log.Info("waiting for the system to drain", "timeout", cfg.DrainTimeout)
	out.DrainTook, out.Results.Pending, err = reader.WaitDrained(ctx, prefix, cfg.DrainTimeout)
	if err != nil {
		stopScrape()
		<-scraped
		return out, err
	}
	time.Sleep(out.ScrapeEvery + 200*time.Millisecond) // one more scrape after the drain
	stopScrape()
	<-scraped

	pending, took := out.Results.Pending, out.DrainTook
	out.Results, err = reader.Read(ctx, prefix)
	if err != nil {
		return out, err
	}
	out.Results.Pending, out.Results.Drain = pending, took
	return out, WriteFiles(cfg, out)
}

// hardware describes the machine the run happened on.
func hardware() map[string]string {
	h := map[string]string{}
	for _, f := range []struct{ key, path, field string }{
		{"cpu", "/proc/cpuinfo", "model name"},
		{"memory", "/proc/meminfo", "MemTotal"},
	} {
		if v := firstField(f.path, f.field); v != "" {
			h[f.key] = v
		}
	}
	if b, err := os.ReadFile(filepath.Clean("/proc/cpuinfo")); err == nil {
		h["cpu_threads"] = strconv.Itoa(strings.Count("\n"+string(b), "\nprocessor"))
	}
	if b, err := os.ReadFile(filepath.Clean("/proc/version")); err == nil {
		h["kernel"] = string(b[:min(len(b), 120)])
	}
	return h
}
