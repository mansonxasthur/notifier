package loadtest

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// firstField returns the value after "field:" on the first line of path that has it.
func firstField(path, field string) string {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(k) == field {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// WriteFiles writes params.json, summary.md, samples.csv, sql.txt and the last scrape of each target.
func WriteFiles(cfg Config, o Outcome) error {
	dir := cfg.OutDir
	gitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sha, _ := exec.CommandContext(gitCtx, "git", "rev-parse", "HEAD").Output()
	dirty, _ := exec.CommandContext(gitCtx, "git", "status", "--porcelain").Output()
	params := map[string]any{
		"label": cfg.Label, "run_id": o.RunID, "started": o.Started.Format(time.RFC3339),
		"args": cfg.Args, "git_sha": strings.TrimSpace(string(sha)), "git_dirty": len(strings.TrimSpace(string(dirty))) > 0,
		"go": runtime.Version(), "driver_cpu_affinity": runtime.NumCPU(), "hardware": hardware(),
		"recipients_per_second": cfg.Rate, "batch": cfg.Batch, "channels": cfg.Channels,
		"deliveries_per_second": cfg.Rate * len(cfg.Channels),
		"warmup":                cfg.Warmup.String(), "duration": cfg.Duration.String(), "max_in_flight": cfg.MaxInFlight,
		"primaries": cfg.Primaries,
	}
	b, _ := json.MarshalIndent(params, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "params.json"), append(b, '\n'), 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.md"), []byte(Summary(cfg, o)), 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "sql.txt"), []byte(strings.Join(o.Results.SQL, "\n")), 0o600); err != nil {
		return err
	}
	for name := range cfg.Targets {
		if raw := o.Scraper.Raw(name); raw != nil {
			if err := os.WriteFile(filepath.Join(dir, "final-"+name+".prom"), raw, 0o600); err != nil {
				return err
			}
		}
	}
	return writeSamples(filepath.Join(dir, "samples.csv"), o.Scraper.Samples())
}

func writeSamples(path string, samples []Sample) error {
	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"unix_ms", "target", "series", "value"})
	for _, s := range samples {
		_ = w.Write([]string{strconv.FormatInt(s.At.UnixMilli(), 10), s.Target, s.Series, strconv.FormatFloat(s.Value, 'g', -1, 64)})
	}
	w.Flush()
	return w.Error()
}

// peak returns the largest value of any series of target whose name starts with prefix.
func peak(samples []Sample, target, prefix string) (float64, string) {
	best, series := math.Inf(-1), ""
	for _, s := range samples {
		if s.Target == target && strings.HasPrefix(s.Series, prefix) && s.Value > best {
			best, series = s.Value, s.Series
		}
	}
	return best, series
}

func secs(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	if math.IsInf(v, 1) {
		return "above the last bucket (30 s)"
	}
	return fmt.Sprintf("%.3f s", v)
}

// Summary is the run's result as a page a reader can take in without the raw files.
func Summary(cfg Config, o Outcome) string {
	var b strings.Builder
	r := o.Results
	fmt.Fprintf(&b, "# %s\n\n", cfg.Label)
	fmt.Fprintf(&b, "Run `%s`, started %s. Setup is in `params.json`; queries and raw answers in `sql.txt`; every metric sample in `samples.csv`.\n\n", o.RunID, o.Started.Format(time.RFC3339))

	fmt.Fprintf(&b, "## Load\n\n")
	fmt.Fprintf(&b, "- %d recipients/s x %d channels = **%d deliveries/s** target, in requests of %d recipients (%d requests/s), for %s after a %s warm-up.\n",
		cfg.Rate, len(cfg.Channels), cfg.Rate*len(cfg.Channels), cfg.Batch, cfg.Rate/max(cfg.Batch, 1), cfg.Duration, cfg.Warmup)
	m := o.Measure
	fmt.Fprintf(&b, "- Requests scheduled %d; answered %v; transport errors %d; **skipped by the driver %d** (in-flight limit %d); latest request left %s late.\n",
		m.Requests, m.ByStatus, m.Errors, m.Skipped, cfg.MaxInFlight, m.MaxLag.Round(time.Millisecond))
	fmt.Fprintf(&b, "- Deliveries the API accepted: %d (of %d requested by every scheduled request). HTTP latency of answered requests: p50 %s, p99 %s, max %s.\n",
		m.Accepted, o.SentRecipients*len(cfg.Channels), m.Percentile(.5).Round(time.Millisecond), m.Percentile(.99).Round(time.Millisecond), m.Percentile(1).Round(time.Millisecond))
	if m.ByStatus[429] > 0 {
		fmt.Fprintf(&b, "- Refused with 429: %d requests; largest Retry-After %d s.\n", m.ByStatus[429], m.RetryAfter)
	}

	fmt.Fprintf(&b, "\n## Request to ACCEPTED (from `accepted_at - created_at`, exact)\n\n")
	fmt.Fprintf(&b, "| Deliveries | Count | p50 | p95 | p99 | max | p99 vs 5 s |\n| --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
	row := func(name string, l Latency) {
		if l.Count == 0 {
			fmt.Fprintf(&b, "| %s | 0 | – | – | – | – | – |\n", name)
			return
		}
		verdict := "within"
		if l.P99 > 5 {
			verdict = "**over**"
		}
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s | %s |\n", name, l.Count, secs(l.P50), secs(l.P95), secs(l.P99), secs(l.Max), verdict)
	}
	row("first attempt (primary provider, no retry)", r.First)
	row("retried or failed over", r.Retried)

	fmt.Fprintf(&b, "\n## Outcome\n\n- Stored %d deliveries (%d expected from accepted requests); %d have `accepted_at`.\n", r.Total, m.Accepted, r.Accepted)
	states := make([]string, 0, len(r.ByState))
	for s := range r.ByState {
		states = append(states, s)
	}
	sort.Strings(states)
	for _, s := range states {
		fmt.Fprintf(&b, "  - %s: %d\n", s, r.ByState[s])
	}
	drained := "drained"
	if r.Pending > 0 {
		drained = fmt.Sprintf("**%d still PENDING when the wait ended**", r.Pending)
	}
	fmt.Fprintf(&b, "- After the last request the system took %s to settle (%s).\n", r.Drain.Round(100*time.Millisecond), drained)

	samples := o.Scraper.Samples()
	fmt.Fprintf(&b, "\n## Metrics (scraped every %s, %d scrapes failed)\n\n", o.ScrapeEvery, o.Scraper.Failures())
	for _, ch := range cfg.Channels {
		buckets, n := o.Scraper.Histogram("sender", "notifier_queue_wait_seconds", "channel="+string(ch))
		if n > 0 {
			fmt.Fprintf(&b, "- Queue wait, %s (%d messages including the warm-up; estimated from buckets, so shape only): p50 %s, p99 %s.\n", ch, n,
				secs(Quantile(.5, buckets)), secs(Quantile(.99, buckets)))
		}
	}
	if v, series := peak(samples, "relay", "notifier_queue_depth"); !math.IsInf(v, -1) {
		fmt.Fprintf(&b, "- Peak queue depth: %.0f (%s).\n", v, series)
	}
	if v, series := peak(samples, "sender", "notifier_breaker_state"); !math.IsInf(v, -1) {
		fmt.Fprintf(&b, "- Highest breaker state seen: %.0f (%s); 0 closed, 1 half-open, 2 open.\n", v, series)
	}
	return b.String()
}
