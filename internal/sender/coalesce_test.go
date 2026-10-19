package sender

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// recordFlush doubles each item and records the batches it was given.
type recordFlush struct {
	mu      sync.Mutex
	batches [][]int
	err     error
}

func (r *recordFlush) flush(_ context.Context, items []int) ([]int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, slices.Clone(items))
	if r.err != nil {
		return nil, r.err
	}
	out := make([]int, len(items))
	for i, v := range items {
		out[i] = v * 2
	}
	return out, nil
}

func (r *recordFlush) sizes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var s []int
	for _, b := range r.batches {
		s = append(s, len(b))
	}
	return s
}

// doAll runs Do for each item concurrently and returns the results by item.
func doAll(t *testing.T, c *Coalescer[int, int], items ...int) map[int]int {
	t.Helper()
	var mu sync.Mutex
	got := map[int]int{}
	var wg sync.WaitGroup
	for _, it := range items {
		wg.Go(func() {
			out, err := c.Do(context.Background(), it)
			if err != nil {
				t.Errorf("Do(%d): %v", it, err)
				return
			}
			mu.Lock()
			got[it] = out
			mu.Unlock()
		})
	}
	wg.Wait()
	return got
}

func TestCoalescerFlushesWhenFull(t *testing.T) {
	r := &recordFlush{}
	// The linger is far longer than the test, so only the size can flush.
	c := NewCoalescer(4, time.Hour, r.flush)
	got := doAll(t, c, 1, 2, 3, 4)
	for _, it := range []int{1, 2, 3, 4} {
		if got[it] != it*2 {
			t.Errorf("result for %d = %d, want %d", it, got[it], it*2)
		}
	}
	if s := r.sizes(); len(s) != 1 || s[0] != 4 {
		t.Errorf("batch sizes = %v, want [4]", s)
	}
}

func TestCoalescerFlushesAfterLinger(t *testing.T) {
	r := &recordFlush{}
	c := NewCoalescer(100, 10*time.Millisecond, r.flush)
	start := time.Now()
	got := doAll(t, c, 1, 2, 3)
	if len(got) != 3 || got[3] != 6 {
		t.Errorf("results = %v, want each item doubled", got)
	}
	if time.Since(start) < 10*time.Millisecond {
		t.Error("a partial batch must wait for the linger")
	}
	total := 0
	for _, n := range r.sizes() {
		total += n
	}
	if total != 3 {
		t.Errorf("flushed %d items, want 3", total)
	}
}

func TestCoalescerFansOutAFlushError(t *testing.T) {
	boom := errors.New("boom")
	r := &recordFlush{err: boom}
	c := NewCoalescer(2, time.Hour, r.flush)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Go(func() {
			if _, err := c.Do(context.Background(), i); !errors.Is(err, boom) {
				t.Errorf("Do(%d) = %v, want the flush error", i, err)
			}
		})
	}
	wg.Wait()
}

func TestCoalescerRejectsAShortResult(t *testing.T) {
	c := NewCoalescer(1, time.Hour, func(context.Context, []int) ([]int, error) { return nil, nil })
	if _, err := c.Do(context.Background(), 1); err == nil {
		t.Error("a flush that returns too few results must be an error, not a panic")
	}
}

func TestCoalescerCanceledCallerLeavesTheBatch(t *testing.T) {
	r := &recordFlush{}
	c := NewCoalescer(2, time.Hour, r.flush)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := c.Do(ctx, 1); errc <- err }()
	// Wait until the first item is pending, then cancel its caller.
	for {
		c.mu.Lock()
		n := len(c.pending)
		c.mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Do = %v, want context.Canceled", err)
	}
	// The canceled item is still in the batch, and the second caller is served.
	out, err := c.Do(context.Background(), 2)
	if err != nil || out != 4 {
		t.Fatalf("Do(2) = %d, %v, want 4", out, err)
	}
	if s := r.sizes(); len(s) != 1 || s[0] != 2 {
		t.Errorf("batch sizes = %v, want [2]", s)
	}
}

func TestCoalescerCloseFlushesPending(t *testing.T) {
	r := &recordFlush{}
	c := NewCoalescer(10, time.Hour, r.flush)
	done := make(chan int, 1)
	go func() { out, _ := c.Do(context.Background(), 5); done <- out }()
	for {
		c.mu.Lock()
		n := len(c.pending)
		c.mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Close()
	if out := <-done; out != 10 {
		t.Errorf("pending item result = %d, want 10", out)
	}
	// After Close a call still works, alone.
	if out, err := c.Do(context.Background(), 7); err != nil || out != 14 {
		t.Errorf("Do after Close = %d, %v, want 14", out, err)
	}
}
