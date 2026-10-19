package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// Sender sends requests at a fixed rate whether or not the earlier ones have returned
// (open loop). A closed loop would slow down when the system does, and so hide the latency
// the test exists to show.
type Sender struct {
	URL, APIKey string
	RunID       string
	// Rate is recipients per second; Batch the recipients in one request.
	Rate, Batch int
	Channels    []domain.Channel
	// MaxInFlight bounds requests awaiting an answer. A request that finds the bound
	// reached is skipped and counted: the driver, not the system, was the limit.
	MaxInFlight int

	client *http.Client
}

// Stats is what the client saw during one phase.
type Stats struct {
	Requests, Skipped, Errors int
	// ByStatus counts answers by HTTP status.
	ByStatus map[int]int
	// Accepted is the sum of "accepted" over the 202 answers.
	Accepted int
	// Latencies of answered requests, sorted.
	Latencies []time.Duration
	// RetryAfter is the largest Retry-After seen on a 429, in seconds.
	RetryAfter int
	// MaxLag is how late the latest request left, against the schedule.
	MaxLag time.Duration
}

// Percentile returns the p-th (0..1) latency, or 0 if there are none.
func (s Stats) Percentile(p float64) time.Duration {
	if len(s.Latencies) == 0 {
		return 0
	}
	i := int(p*float64(len(s.Latencies))+0.5) - 1
	return s.Latencies[min(max(i, 0), len(s.Latencies)-1)]
}

// Phase sends for d at the configured rate. first is the number of the first recipient, and
// the number after the last one is returned.
func (s *Sender) Phase(ctx context.Context, p Phase, d time.Duration, first int) (Stats, int) {
	if s.client == nil {
		s.client = &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{MaxIdleConns: s.MaxInFlight, MaxIdleConnsPerHost: s.MaxInFlight, MaxConnsPerHost: 0},
		}
	}
	var (
		mu       sync.Mutex
		st       = Stats{ByStatus: map[int]int{}}
		wg       sync.WaitGroup
		inFlight atomic.Int64
	)
	record := func(status int, lat time.Duration, accepted, retryAfter int, err error) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			st.Errors++
			return
		}
		st.ByStatus[status]++
		st.Latencies = append(st.Latencies, lat)
		st.Accepted += accepted
		st.RetryAfter = max(st.RetryAfter, retryAfter)
	}

	interval := time.Duration(float64(time.Second) * float64(s.Batch) / float64(s.Rate))
	start := time.Now()
	n := first
	for i := 0; ; i++ {
		due := start.Add(time.Duration(i) * interval)
		if due.Sub(start) >= d || ctx.Err() != nil {
			break
		}
		if wait := time.Until(due); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				break
			}
			if ctx.Err() != nil {
				break
			}
		}
		lag := time.Since(due)
		mu.Lock()
		st.MaxLag = max(st.MaxLag, lag)
		st.Requests++
		mu.Unlock()
		if inFlight.Load() >= int64(s.MaxInFlight) {
			mu.Lock()
			st.Skipped++
			mu.Unlock()
			n += s.Batch
			continue
		}
		body, err := Body(s.RunID, p, n, s.Batch, s.Channels)
		n += s.Batch
		if err != nil {
			record(0, 0, 0, 0, err)
			continue
		}
		inFlight.Add(1)
		wg.Go(func() {
			defer inFlight.Add(-1)
			status, lat, accepted, ra, err := s.post(ctx, body)
			record(status, lat, accepted, ra, err)
		})
	}
	wg.Wait()
	sort.Slice(st.Latencies, func(i, j int) bool { return st.Latencies[i] < st.Latencies[j] })
	return st, n
}

func (s *Sender) post(ctx context.Context, body []byte) (status int, lat time.Duration, accepted, retryAfter int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/v1/notifications", bytes.NewReader(body))
	if err != nil {
		return 0, 0, 0, 0, err
	}
	req.Header.Set("X-API-KEY", s.APIKey)
	req.Header.Set("Content-Type", "application/json")
	t := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	lat = time.Since(t)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	var out struct {
		Accepted int `json:"accepted"`
	}
	if resp.StatusCode == http.StatusAccepted {
		_ = json.Unmarshal(raw, &out)
	}
	if v, perr := strconv.Atoi(resp.Header.Get("Retry-After")); perr == nil {
		retryAfter = v
	}
	return resp.StatusCode, lat, out.Accepted, retryAfter, nil
}
