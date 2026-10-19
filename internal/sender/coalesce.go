package sender

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Coalescer groups the calls of many goroutines into one batch call. A caller submits one item
// with Do and waits for the batch holding it; the batch is flushed when it reaches size items or
// linger after its first item, whichever comes first. Each caller gets the result at its own
// place in the batch, so one item failing does not touch the others.
type Coalescer[In, Out any] struct {
	flush  func(ctx context.Context, items []In) ([]Out, error)
	size   int
	linger time.Duration

	mu      sync.Mutex
	pending []*call[In, Out]
	timer   *time.Timer
	// gen counts the batches taken, so a timer armed for an earlier batch cannot flush a later one.
	gen    uint64
	closed bool
	wg     sync.WaitGroup
}

type call[In, Out any] struct {
	in   In
	done chan result[Out]
}

type result[Out any] struct {
	out Out
	err error
}

// NewCoalescer returns a Coalescer that hands batches of up to size items to flush. flush
// returns one result per item, in order; an error it returns goes to every caller in the batch.
func NewCoalescer[In, Out any](size int, linger time.Duration, flush func(ctx context.Context, items []In) ([]Out, error)) *Coalescer[In, Out] {
	return &Coalescer[In, Out]{flush: flush, size: max(size, 1), linger: linger}
}

// Do submits an item and returns its result once its batch has been flushed. If ctx is done
// first it returns ctx.Err(); the item is still in its batch and is still written.
func (c *Coalescer[In, Out]) Do(ctx context.Context, in In) (Out, error) {
	cl := &call[In, Out]{in: in, done: make(chan result[Out], 1)}

	c.mu.Lock()
	if c.closed {
		// Nothing is waiting to batch with it, so it goes alone.
		c.mu.Unlock()
		c.run([]*call[In, Out]{cl}, false)
	} else {
		c.pending = append(c.pending, cl)
		switch {
		case len(c.pending) >= c.size:
			batch := c.take()
			c.mu.Unlock()
			c.run(batch, true)
		case len(c.pending) == 1:
			gen := c.gen
			c.timer = time.AfterFunc(c.linger, func() { c.onTimer(gen) })
			c.mu.Unlock()
		default:
			c.mu.Unlock()
		}
	}

	select {
	case r := <-cl.done:
		return r.out, r.err
	case <-ctx.Done():
		var zero Out
		return zero, ctx.Err()
	}
}

// Close flushes what is pending and waits for the flushes in progress. Calls after it run alone.
func (c *Coalescer[In, Out]) Close() {
	c.mu.Lock()
	c.closed = true
	batch := c.take()
	c.mu.Unlock()
	c.run(batch, true)
	c.wg.Wait()
}

// take removes the pending calls, stops the timer and counts the batch as a flush in progress,
// which run then ends. The caller holds mu.
func (c *Coalescer[In, Out]) take() []*call[In, Out] {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.gen++
	batch := c.pending
	c.pending = nil
	if len(batch) > 0 {
		c.wg.Add(1)
	}
	return batch
}

// onTimer flushes the batch the timer was armed for, unless it was flushed already.
func (c *Coalescer[In, Out]) onTimer(gen uint64) {
	c.mu.Lock()
	if gen != c.gen {
		c.mu.Unlock()
		return
	}
	batch := c.take()
	c.mu.Unlock()
	c.run(batch, true)
}

// run flushes a batch and gives each caller its result. The flush does not see any caller's
// cancellation: the batch holds other callers' work too.
// tracked says the batch came from take, which counted it for Close to wait for.
func (c *Coalescer[In, Out]) run(batch []*call[In, Out], tracked bool) {
	if len(batch) == 0 {
		return
	}
	if tracked {
		defer c.wg.Done()
	}

	items := make([]In, len(batch))
	for i, cl := range batch {
		items[i] = cl.in
	}
	ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	outs, err := c.flush(ctx, items)
	if err == nil && len(outs) != len(batch) {
		err = fmt.Errorf("batch of %d got %d results", len(batch), len(outs))
	}
	for i, cl := range batch {
		var r result[Out]
		if err != nil {
			r.err = err
		} else {
			r.out = outs[i]
		}
		cl.done <- r
	}
}
