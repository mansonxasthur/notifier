package loadtest

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/httpserver/requests"
)

func TestQuantile(t *testing.T) {
	inf := math.Inf(1)
	// 100 observations: 50 at most 0.1 s, 90 at most 1 s, all 100 at most 5 s.
	b := []Bucket{{0.1, 50}, {1, 90}, {5, 100}, {inf, 100}}
	for _, tc := range []struct{ q, want float64 }{
		{0.25, 0.05},           // half-way through the first bucket
		{0.5, 0.1},             // exactly on its edge
		{0.7, 0.1 + 0.9*0.5},   // half-way through the second
		{0.99, 1 + 4*(9.0/10)}, // nine tenths through the third
	} {
		if got := Quantile(tc.q, b); math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("Quantile(%v) = %v, want %v", tc.q, got, tc.want)
		}
	}
	if got := Quantile(0.5, nil); !math.IsNaN(got) {
		t.Errorf("no buckets = %v, want NaN", got)
	}
	if got := Quantile(0.5, []Bucket{{1, 0}, {inf, 0}}); !math.IsNaN(got) {
		t.Errorf("no observations = %v, want NaN", got)
	}
	// Observations beyond the last finite bucket cannot be placed, and must not look bounded.
	if got := Quantile(0.99, []Bucket{{1, 1}, {inf, 10}}); !math.IsInf(got, 1) {
		t.Errorf("overflow = %v, want +Inf", got)
	}
}

// A request the driver builds is one the API accepts, with the Arabic content intact and no
// recipient or contact repeated across requests.
func TestBodyIsAValidRequest(t *testing.T) {
	channels := []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelWhatsApp}
	a, err := Body("r1", Measure, 0, 50, channels)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Body("r1", Measure, 50, 50, channels)

	var ra requests.CreateNotificationRequest
	if err := json.Unmarshal(a, &ra); err != nil {
		t.Fatal(err)
	}
	policy := requests.Policy{BatchLimit: 3000, Expiry: map[string]time.Duration{"match_alert": time.Hour}}
	v, err := ra.Validate(policy, time.Now())
	if err != nil {
		t.Fatalf("the API would refuse the request: %v", err)
	}
	if got := len(v.Notification.Deliveries); got != 150 {
		t.Errorf("deliveries = %d, want 150 (50 recipients x 3 channels)", got)
	}
	if !strings.Contains(string(a), "مباراة") {
		t.Error("the Arabic text is not in the body as UTF-8")
	}

	var rb requests.CreateNotificationRequest
	_ = json.Unmarshal(b, &rb)
	seen := map[string]bool{}
	for _, r := range append(ra.Recipients, rb.Recipients...) {
		for _, c := range append([]string{r.ID}, r.Channels[domain.ChannelSMS], r.Channels[domain.ChannelEmail]) {
			if seen[c] {
				t.Fatalf("%q appears twice across two requests of one run", c)
			}
			seen[c] = true
		}
	}
	if ra.IdempotencyKey == rb.IdempotencyKey {
		t.Error("two requests share an idempotency key, so the second would be a replay")
	}
}

// The driver keeps its schedule while the server is slow: requests leave at the set rate
// and are still outstanding when the next one leaves, which is what open loop means.
func TestPhaseIsOpenLoop(t *testing.T) {
	var got, inFlight, peak atomic.Int64
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Add(1)
		n := inFlight.Add(1)
		mu.Lock()
		peak.Store(max(peak.Load(), n))
		mu.Unlock()
		defer inFlight.Add(-1)
		_, _ = io.Copy(io.Discard, r.Body)
		time.Sleep(300 * time.Millisecond) // far slower than the 50 ms between requests
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":10}`))
	}))
	defer srv.Close()

	s := &Sender{URL: srv.URL, APIKey: "k", RunID: "r", Rate: 100, Batch: 5,
		Channels: []domain.Channel{domain.ChannelSMS}, MaxInFlight: 100}
	st, next := s.Phase(context.Background(), Measure, time.Second, 0)

	// 100 recipients/s in requests of 5 is 20 requests/s: 20 in a second.
	if st.Requests < 19 || st.Requests > 21 || int(got.Load()) != st.Requests {
		t.Errorf("sent %d requests (server saw %d), want about 20", st.Requests, got.Load())
	}
	if peak.Load() < 4 {
		t.Errorf("at most %d requests were outstanding at once: the driver waited for answers", peak.Load())
	}
	if st.ByStatus[http.StatusAccepted] != st.Requests || st.Accepted != 10*st.Requests {
		t.Errorf("answers = %v accepted = %d", st.ByStatus, st.Accepted)
	}
	if next != st.Requests*5 {
		t.Errorf("next recipient = %d, want %d: every request, skipped or not, uses its own numbers", next, st.Requests*5)
	}
	if p := st.Percentile(0.5); p < 300*time.Millisecond {
		t.Errorf("median latency = %v, want the server's 300 ms", p)
	}
}

func TestPhaseSkipsAndSaysSoWhenTheDriverIsTheLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	s := &Sender{URL: srv.URL, APIKey: "k", RunID: "r", Rate: 100, Batch: 5,
		Channels: []domain.Channel{domain.ChannelSMS}, MaxInFlight: 2}
	st, _ := s.Phase(context.Background(), Measure, 500*time.Millisecond, 0)
	if st.Skipped == 0 {
		t.Errorf("no request skipped with 2 allowed in flight and 10 sent: %+v", st)
	}
	if st.ByStatus[http.StatusTooManyRequests] == 0 || st.RetryAfter != 3 {
		t.Errorf("429s = %v, Retry-After = %d, want some and 3", st.ByStatus, st.RetryAfter)
	}
}
