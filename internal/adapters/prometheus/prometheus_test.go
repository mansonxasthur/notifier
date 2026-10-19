package prometheus_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/prometheus"
	"github.com/mansonxasthur/notifier/internal/domain"
)

func scrape(t *testing.T, m *prometheus.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))
	b, _ := io.ReadAll(rec.Body)
	return string(b)
}

func TestMeasurementsAreExported(t *testing.T) {
	m := prometheus.New()
	m.QueueWait(domain.ChannelSMS, 40*time.Millisecond)
	m.AcceptLatency(domain.ChannelSMS, true, 300*time.Millisecond)
	m.AcceptLatency(domain.ChannelSMS, false, 7*time.Second)
	m.QueueDepth(domain.ChannelEmail, 42)
	m.BreakerStates(func() map[string]int { return map[string]int{"sms-a": prometheus.BreakerOpen} })

	out := scrape(t, m)
	for _, want := range []string{
		`notifier_queue_wait_seconds_count{channel="sms"} 1`,
		`notifier_accept_latency_seconds_count{attempt="first",channel="sms"} 1`,
		`notifier_accept_latency_seconds_count{attempt="retried",channel="sms"} 1`,
		// The 5 second target is a bucket edge: 0.3s is inside it and 7s is not.
		`notifier_accept_latency_seconds_bucket{attempt="first",channel="sms",le="5"} 1`,
		`notifier_accept_latency_seconds_bucket{attempt="retried",channel="sms",le="5"} 0`,
		`notifier_queue_depth{channel="email"} 42`,
		`notifier_breaker_state{provider="sms-a"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("scrape lacks %q", want)
		}
	}
}

func TestInstrumentCountsByStatusAndIgnoresScrapes(t *testing.T) {
	m := prometheus.New()
	h := m.Instrument(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/limited" {
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	for _, path := range []string{"/ok", "/limited", "/limited", "/metrics"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	}
	out := scrape(t, m)
	for _, want := range []string{`notifier_requests_total{status="200"} 1`, `notifier_requests_total{status="429"} 2`} {
		if !strings.Contains(out, want) {
			t.Errorf("scrape lacks %q\n%s", want, out)
		}
	}
}
