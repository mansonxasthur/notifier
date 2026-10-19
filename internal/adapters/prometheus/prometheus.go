// Package prometheus is the metrics adapter: it implements ports.Recorder and serves the
// measurements in the Prometheus text format.
package prometheus

import (
	"net/http"
	"strconv"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.Recorder = (*Metrics)(nil)

// buckets put 5 seconds, the request-to-ACCEPTED target, inside the range with room either side.
var buckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// Metrics holds one process's measurements in its own registry, so tests and processes do not share state.
type Metrics struct {
	reg      *prom.Registry
	wait     *prom.HistogramVec
	accept   *prom.HistogramVec
	depth    *prom.GaugeVec
	requests *prom.CounterVec
}

// New returns Metrics with every instrument registered.
func New() *Metrics {
	m := &Metrics{
		reg: prom.NewRegistry(),
		wait: prom.NewHistogramVec(prom.HistogramOpts{
			Name: "notifier_queue_wait_seconds", Help: "Time a message waited in its channel's queue.", Buckets: buckets,
		}, []string{"channel"}),
		accept: prom.NewHistogramVec(prom.HistogramOpts{
			Name: "notifier_accept_latency_seconds", Help: "Time from a delivery's creation to ACCEPTED.", Buckets: buckets,
		}, []string{"channel", "attempt"}),
		depth: prom.NewGaugeVec(prom.GaugeOpts{
			Name: "notifier_queue_depth", Help: "Messages waiting in a channel's queue.",
		}, []string{"channel"}),
		requests: prom.NewCounterVec(prom.CounterOpts{
			Name: "notifier_requests_total", Help: "HTTP requests handled, by status code.",
		}, []string{"status"}),
	}
	m.reg.MustRegister(m.wait, m.accept, m.depth, m.requests)
	return m
}

// Handler serves the measurements.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// QueueWait implements ports.Recorder.
func (m *Metrics) QueueWait(ch domain.Channel, d time.Duration) {
	m.wait.WithLabelValues(string(ch)).Observe(d.Seconds())
}

// AcceptLatency implements ports.Recorder.
func (m *Metrics) AcceptLatency(ch domain.Channel, first bool, d time.Duration) {
	attempt := "retried"
	if first {
		attempt = "first"
	}
	m.accept.WithLabelValues(string(ch), attempt).Observe(d.Seconds())
}

// QueueDepth implements ports.Recorder.
func (m *Metrics) QueueDepth(ch domain.Channel, n int) {
	m.depth.WithLabelValues(string(ch)).Set(float64(n))
}

// BreakerStates exports one gauge per provider, read when scraped: 0 closed, 1 half-open,
// 2 open. A breaker that has reopened to a probe by the passing of time is seen as such.
func (m *Metrics) BreakerStates(states func() map[string]int) {
	m.reg.MustRegister(&breakerCollector{states: states})
}

// Breaker states as exported.
const (
	BreakerClosed   = 0
	BreakerHalfOpen = 1
	BreakerOpen     = 2
)

type breakerCollector struct{ states func() map[string]int }

var breakerDesc = prom.NewDesc("notifier_breaker_state", "Circuit breaker state per provider: 0 closed, 1 half-open, 2 open.", []string{"provider"}, nil)

func (c *breakerCollector) Describe(ch chan<- *prom.Desc) { ch <- breakerDesc }

func (c *breakerCollector) Collect(ch chan<- prom.Metric) {
	for provider, s := range c.states() {
		ch <- prom.MustNewConstMetric(breakerDesc, prom.GaugeValue, float64(s), provider)
	}
}

// Instrument counts the requests h handles by status code.
func (m *Metrics) Instrument(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			h.ServeHTTP(w, r) // a scrape is not traffic
			return
		}
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(sw, r)
		m.requests.WithLabelValues(strconv.Itoa(sw.status)).Inc()
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
