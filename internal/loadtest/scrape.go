package loadtest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// Sample is one value of one series of one target at one moment.
type Sample struct {
	At     time.Time
	Target string
	Series string
	Value  float64
}

// Scraper reads the /metrics endpoints of the services at a fixed interval.
type Scraper struct {
	targets map[string]string
	every   time.Duration
	client  *http.Client

	mu      sync.Mutex
	samples []Sample
	last    map[string]map[string]*dto.MetricFamily
	raw     map[string][]byte
	errs    int
}

// NewScraper scrapes every target (name to URL) each interval.
func NewScraper(targets map[string]string, every time.Duration) *Scraper {
	return &Scraper{
		targets: targets, every: every, client: &http.Client{Timeout: 2 * time.Second},
		last: map[string]map[string]*dto.MetricFamily{}, raw: map[string][]byte{},
	}
}

// Run scrapes until ctx is done, then once more, so the last reading is at the end of the run.
func (s *Scraper) Run(ctx context.Context) {
	t := time.NewTicker(s.every)
	defer t.Stop()
	for {
		s.once(context.WithoutCancel(ctx))
		select {
		case <-ctx.Done():
			s.once(context.WithoutCancel(ctx))
			return
		case <-t.C:
		}
	}
}

func (s *Scraper) once(ctx context.Context) {
	for name, url := range s.targets {
		body, err := s.get(ctx, url)
		if err != nil {
			s.mu.Lock()
			s.errs++
			s.mu.Unlock()
			continue
		}
		var p expfmt.TextParser
		fams, err := p.TextToMetricFamilies(strings.NewReader(string(body)))
		if err != nil {
			s.mu.Lock()
			s.errs++
			s.mu.Unlock()
			continue
		}
		now := time.Now()
		var out []Sample
		for fname, f := range fams {
			if !strings.HasPrefix(fname, "notifier_") {
				continue
			}
			for _, m := range f.Metric {
				series := fname + labels(m)
				switch f.GetType() {
				case dto.MetricType_HISTOGRAM:
					out = append(out,
						Sample{now, name, series + ":count", float64(m.Histogram.GetSampleCount())},
						Sample{now, name, series + ":sum", m.Histogram.GetSampleSum()})
				case dto.MetricType_COUNTER:
					out = append(out, Sample{now, name, series, m.Counter.GetValue()})
				case dto.MetricType_GAUGE:
					out = append(out, Sample{now, name, series, m.Gauge.GetValue()})
				}
			}
		}
		s.mu.Lock()
		s.samples = append(s.samples, out...)
		s.last[name], s.raw[name] = fams, body
		s.mu.Unlock()
	}
}

func (s *Scraper) get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func labels(m *dto.Metric) string {
	if len(m.Label) == 0 {
		return ""
	}
	parts := make([]string, len(m.Label))
	for i, l := range m.Label {
		parts[i] = l.GetName() + "=" + l.GetValue()
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}"
}

// Samples returns everything scraped so far.
func (s *Scraper) Samples() []Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Sample(nil), s.samples...)
}

// Raw returns the last scrape of a target, as the service sent it.
func (s *Scraper) Raw(target string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.raw[target]
}

// Failures is how many scrapes failed, which a reader needs to trust the gaps.
func (s *Scraper) Failures() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errs
}

// Histogram returns the cumulative buckets of a histogram series in the last scrape of a
// target, merged over every label set whose rendered labels contain match.
func (s *Scraper) Histogram(target, name, match string) (buckets []Bucket, count uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.last[target][name]
	if f == nil {
		return nil, 0
	}
	merged := map[float64]uint64{}
	for _, m := range f.Metric {
		if !strings.Contains(labels(m), match) {
			continue
		}
		count += m.Histogram.GetSampleCount()
		for _, b := range m.Histogram.Bucket {
			merged[b.GetUpperBound()] += b.GetCumulativeCount()
		}
	}
	for le, c := range merged {
		buckets = append(buckets, Bucket{le, c})
	}
	return buckets, count
}
