package rabbitmq_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq/rmqtest"
	"github.com/mansonxasthur/notifier/internal/domain"
)

var quiet = slog.New(slog.DiscardHandler)

func connect(t *testing.T, url string) *rabbitmq.Conn {
	t.Helper()
	ctx := context.Background()
	conn := rabbitmq.New(url)
	if err := conn.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Stop(ctx) })
	return conn
}

// collector records the IDs a consumer handler saw.
type collector struct {
	mu   sync.Mutex
	seen map[int64]int
}

func newCollector() *collector { return &collector{seen: map[int64]int{}} }

func (c *collector) handle(_ context.Context, id int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[id]++
	return nil
}

func (c *collector) has(ids ...int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		if c.seen[id] == 0 {
			return false
		}
	}
	return true
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestPublishConsume(t *testing.T) {
	broker := rmqtest.Start(t)
	conn := connect(t, broker.URL)
	pub := rabbitmq.NewPublisher(conn, 5*time.Second)
	t.Cleanup(pub.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := newCollector()
	go rabbitmq.NewConsumer(conn, 10, quiet).Consume(ctx, domain.ChannelSMS, got.handle) //nolint:errcheck // returns nil on cancel

	confirmed, err := pub.Publish(ctx, domain.ChannelSMS, []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	for i, ok := range confirmed {
		if !ok {
			t.Errorf("message %d not confirmed", i)
		}
	}
	waitFor(t, "three messages", func() bool { return got.has(1, 2, 3) })
}

func TestQueuesAreIndependent(t *testing.T) {
	broker := rmqtest.Start(t)
	conn := connect(t, broker.URL)
	pub := rabbitmq.NewPublisher(conn, 5*time.Second)
	t.Cleanup(pub.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	email := newCollector()
	go rabbitmq.NewConsumer(conn, 10, quiet).Consume(ctx, domain.ChannelEmail, email.handle) //nolint:errcheck // returns nil on cancel

	if _, err := pub.Publish(ctx, domain.ChannelSMS, []int64{7}); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.Publish(ctx, domain.ChannelEmail, []int64{8}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the email message", func() bool { return email.has(8) })
	if email.has(7) {
		t.Error("email consumer received an sms message")
	}
}

func TestFailedHandlerRequeues(t *testing.T) {
	broker := rmqtest.Start(t)
	conn := connect(t, broker.URL)
	pub := rabbitmq.NewPublisher(conn, 5*time.Second)
	t.Cleanup(pub.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	calls := 0
	handler := func(context.Context, int64) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return errors.New("transient")
		}
		return nil
	}
	go rabbitmq.NewConsumer(conn, 1, quiet).Consume(ctx, domain.ChannelWhatsApp, handler) //nolint:errcheck // returns nil on cancel

	if _, err := pub.Publish(ctx, domain.ChannelWhatsApp, []int64{5}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the redelivery", func() bool { mu.Lock(); defer mu.Unlock(); return calls >= 2 })
}

// A restarted broker keeps persistent messages in durable queues, and the publisher and
// consumer reconnect without a process restart.
func TestBrokerRestart(t *testing.T) {
	broker := rmqtest.Start(t)
	conn := connect(t, broker.URL)
	pub := rabbitmq.NewPublisher(conn, 5*time.Second)
	t.Cleanup(pub.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Published before the restart, with no consumer yet.
	if confirmed, err := pub.Publish(ctx, domain.ChannelEmail, []int64{1, 2}); err != nil || !confirmed[0] || !confirmed[1] {
		t.Fatalf("publish before restart: %v %v", confirmed, err)
	}

	broker.Restart(t)

	got := newCollector()
	go rabbitmq.NewConsumer(conn, 10, quiet).Consume(ctx, domain.ChannelEmail, got.handle) //nolint:errcheck // returns nil on cancel
	waitFor(t, "messages that survived the restart", func() bool { return got.has(1, 2) })

	// The same publisher works again once the broker is back.
	waitFor(t, "publishing after the restart", func() bool {
		confirmed, err := pub.Publish(ctx, domain.ChannelEmail, []int64{3})
		return err == nil && confirmed[0]
	})
	waitFor(t, "the post-restart message", func() bool { return got.has(3) })
}

type waits struct {
	mu sync.Mutex
	d  []time.Duration
}

func (w *waits) QueueWait(_ domain.Channel, d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.d = append(w.d, d)
}
func (*waits) AcceptLatency(domain.Channel, bool, time.Duration) {}
func (*waits) QueueDepth(domain.Channel, int)                    {}

func (w *waits) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.d)
}

// A message that sat in the queue reports about as long as it sat there, and the depth
// counts what is waiting.
func TestQueueWaitAndDepth(t *testing.T) {
	broker := rmqtest.Start(t)
	conn := connect(t, broker.URL)
	pub := rabbitmq.NewPublisher(conn, 5*time.Second)
	t.Cleanup(pub.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := pub.Publish(ctx, domain.ChannelSMS, []int64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a depth of 3", func() bool {
		n, err := conn.QueueDepth(ctx, domain.ChannelSMS)
		return err == nil && n == 3
	})
	if n, err := conn.QueueDepth(ctx, domain.ChannelEmail); err != nil || n != 0 {
		t.Errorf("email depth = %d, %v, want an empty queue", n, err)
	}

	const sat = 300 * time.Millisecond
	time.Sleep(sat)
	w := &waits{}
	got := newCollector()
	go rabbitmq.NewConsumer(conn, 10, quiet).WithRecorder(w).Consume(ctx, domain.ChannelSMS, got.handle) //nolint:errcheck // returns nil on cancel
	waitFor(t, "three waits", func() bool { return w.count() == 3 })

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, d := range w.d {
		if d < sat || d > 30*time.Second {
			t.Errorf("wait = %v, want at least the %v the message sat", d, sat)
		}
	}
}
