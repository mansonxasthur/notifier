package loadtest

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// FaultTimes is when a fault run injected and released its fault, from fault.json.
type FaultTimes struct {
	Inject        string `json:"inject"`
	InjectMS      int64  `json:"inject_unix_ms"`
	InjectDoneMS  int64  `json:"inject_done_unix_ms"`
	Release       string `json:"release"`
	ReleaseMS     int64  `json:"release_unix_ms"`
	ReleaseDoneMS int64  `json:"release_done_unix_ms"`
}

// ledgerEntry is one entry of the simulator's GET /scenarios.
type ledgerEntry struct {
	DeliveryID int64  `json:"delivery_id"`
	Provider   string `json:"provider"`
	Scenario   string `json:"scenario"`
	Accepted   bool   `json:"accepted"`
	Sends      int    `json:"sends"`
}

// Duplicates counts what the simulator received more than once. A provider that holds a
// message and is sent it again is a duplicate send; a delivery held by two providers is a
// duplicate delivery. Sends beyond the first to a provider that did not take the message are
// retries, not duplicates.
type Duplicates struct {
	Deliveries, HeldTwice, DuplicateSends int
}

// CountDuplicates sums the sends of every (provider, delivery) across the ledger snapshots.
// A simulator restart empties its ledger, so a run that restarts it passes several snapshots,
// and a key present in more than one is the same delivery on the same provider.
func CountDuplicates(snapshots [][]ledgerEntry) Duplicates {
	type key struct {
		p  string
		id int64
	}
	sends := map[key]int{}
	held := map[key]bool{}
	for _, snap := range snapshots {
		for _, e := range snap {
			k := key{e.Provider, e.DeliveryID}
			sends[k] += e.Sends
			held[k] = held[k] || e.Accepted
		}
	}
	holders := map[int64]int{}
	var d Duplicates
	for k, n := range sends {
		holders[k.id] += 0
		if held[k] {
			holders[k.id]++
			d.DuplicateSends += n - 1
		}
	}
	d.Deliveries = len(holders)
	for _, h := range holders {
		if h > 1 {
			d.HeldTwice++
		}
	}
	return d
}

func readLedger(path string) ([]ledgerEntry, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var v struct {
		Entries []ledgerEntry `json:"entries"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return v.Entries, nil
}

// point is the sum of a family of series at one scrape.
type point struct {
	ms    int64
	depth float64
	open  int      // providers with an open breaker
	max   float64  // highest breaker state
	which []string // providers with a breaker that is not closed
}

var (
	depthSeries   = regexp.MustCompile(`^notifier_queue_depth\{`)
	breakerSeries = regexp.MustCompile(`^notifier_breaker_state\{provider=([^}]+)\}$`)
)

// readTrace folds samples.csv into one point per scrape second.
func readTrace(path string) ([]point, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("%s has no samples", path)
	}
	by := map[int64]*point{}
	state := map[int64]map[string]float64{} // the highest breaker state of each provider in each second
	for _, r := range rows[1:] {
		if len(r) != 4 {
			continue
		}
		ms, err := strconv.ParseInt(r[0], 10, 64)
		if err != nil {
			continue
		}
		v, err := strconv.ParseFloat(r[3], 64)
		if err != nil {
			continue
		}
		sec := ms / 1000 * 1000
		p := by[sec]
		if p == nil {
			p = &point{ms: sec}
			by[sec] = p
		}
		switch {
		case depthSeries.MatchString(r[2]):
			// relay and sender both export the depth; the larger is the sampled one.
			p.depth = max(p.depth, v)
		case breakerSeries.MatchString(r[2]) && v > 0:
			m := breakerSeries.FindStringSubmatch(r[2])
			if state[sec] == nil {
				state[sec] = map[string]float64{}
			}
			// The sampler can read a series twice in a second; the worse reading counts.
			state[sec][m[1]] = max(state[sec][m[1]], v)
			p.max = max(p.max, v)
		}
	}
	out := make([]point, 0, len(by))
	for sec, p := range by {
		for name, v := range state[sec] {
			p.which = append(p.which, name)
			if v == 2 {
				p.open++
			}
		}
		sort.Strings(p.which)
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ms < out[j].ms })
	return out, nil
}

// Recovery returns the first scrape at or after from where the deepest queue is back to at
// most limit and no breaker open, or -1. A half-open breaker counts as recovered: it lets the
// next call through, and an idle fallback's stays half-open until it gets one.
func Recovery(trace []point, from int64, limit float64) int64 {
	for _, p := range trace {
		if p.ms >= from && p.depth <= limit && p.open == 0 {
			return p.ms
		}
	}
	return -1
}

// WriteFaultReport writes fault-report.md for the fault run in dir.
func WriteFaultReport(dir string) error {
	var ft FaultTimes
	b, err := os.ReadFile(filepath.Clean(filepath.Join(dir, "fault.json")))
	if err != nil {
		return fmt.Errorf("fault.json: %w (did the injector finish?)", err)
	}
	if err := json.Unmarshal(b, &ft); err != nil {
		return err
	}
	trace, err := readTrace(filepath.Join(dir, "samples.csv"))
	if err != nil {
		return err
	}
	rel := func(ms int64) string { return fmt.Sprintf("%+.0f s", float64(ms-ft.InjectMS)/1000) }

	var w strings.Builder
	fmt.Fprintf(&w, "# Fault report\n\n- Injected `%s` at t=0 (took %.1f s to apply).\n", ft.Inject, float64(ft.InjectDoneMS-ft.InjectMS)/1000)
	liftedAt := ft.InjectDoneMS
	if ft.Release != "" || ft.ReleaseMS > 0 {
		r := ft.Release
		if r == "" {
			r = "restored the simulator's config"
		}
		fmt.Fprintf(&w, "- Released (`%s`) at %s (took %.1f s).\n", r, rel(ft.ReleaseMS), float64(ft.ReleaseDoneMS-ft.ReleaseMS)/1000)
		liftedAt = ft.ReleaseDoneMS
	}

	// Baseline: the deepest queue in the 20 s before the injection.
	baseline := 0.0
	for _, p := range trace {
		if p.ms >= ft.InjectMS-20000 && p.ms < ft.InjectMS {
			baseline = max(baseline, p.depth)
		}
	}
	peak, peakAt, peakOpen, openSecs := 0.0, int64(0), 0, 0
	for _, p := range trace {
		if p.ms < ft.InjectMS {
			continue
		}
		if p.depth > peak {
			peak, peakAt = p.depth, p.ms
		}
		peakOpen = max(peakOpen, p.open)
		if p.open > 0 {
			openSecs++
		}
	}
	peakWhen := "no queue built"
	if peakAt > 0 {
		peakWhen = "at " + rel(peakAt)
	}
	fmt.Fprintf(&w, "- Queue depth (deepest channel): %.0f before the fault (20 s), peak %.0f (%s).\n", baseline, peak, peakWhen)
	fmt.Fprintf(&w, "- Breakers: most open at once %d; seconds with at least one open %d.\n", peakOpen, openSecs)
	// Recovered: queues back within the pre-fault level (at least 5) and every breaker closed.
	if at := Recovery(trace, liftedAt, max(baseline, 5)); at < 0 {
		fmt.Fprintf(&w, "- **Not recovered** by the end of the samples (queue within %.0f and no breaker open).\n", max(baseline, 5))
	} else {
		fmt.Fprintf(&w, "- **Recovered** at %s: %.0f s after the fault was lifted.\n", rel(at), float64(at-liftedAt)/1000)
	}

	fmt.Fprint(&w, "\n## Trace (changes only; providers listed have a breaker open or half-open)\n\n```\n")
	last := ""
	for _, p := range trace {
		if p.ms < ft.InjectMS-5000 {
			continue
		}
		line := fmt.Sprintf("depth %4.0f  %s", p.depth, strings.Join(p.which, " "))
		if line != last { // print only changes
			fmt.Fprintf(&w, "%7s  %s\n", rel(p.ms), line)
			last = line
		}
	}
	fmt.Fprint(&w, "```\n")

	// The simulator's ledger: every snapshot, since a simulator restart empties it.
	var snaps [][]ledgerEntry
	for _, name := range []string{"scenarios-before-inject.json", "scenarios-before-release.json", "scenarios.json"} {
		if e, err := readLedger(filepath.Join(dir, name)); err == nil {
			snaps = append(snaps, e)
		}
	}
	if len(snaps) > 0 {
		d := CountDuplicates(snaps)
		fmt.Fprintf(&w, "\n## Duplicates (simulator ledger, %d snapshot(s))\n\n- Deliveries the simulator saw: %d.\n- Held by more than one provider: %d.\n- Sends beyond the first to a provider that already held the message: %d.\n", len(snaps), d.Deliveries, d.HeldTwice, d.DuplicateSends)
	}
	return os.WriteFile(filepath.Join(dir, "fault-report.md"), []byte(w.String()), 0o600)
}
