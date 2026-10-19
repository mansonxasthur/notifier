package rabbitmq_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq/rmqtest"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
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

func (c *collector) handle(_ context.Context, ids []int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		c.seen[id]++
	}
	return nil
}

// count is how many times id was handled.
func (c *collector) count(id int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[id]
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
	pub := rabbitmq.NewPublisher(conn, 5*time.Second, 1)
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
	pub := rabbitmq.NewPublisher(conn, 5*time.Second, 1)
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
	pub := rabbitmq.NewPublisher(conn, 5*time.Second, 1)
	t.Cleanup(pub.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	calls := 0
	handler := func(context.Context, []int64) error {
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
	pub := rabbitmq.NewPublisher(conn, 5*time.Second, 1)
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
	pub := rabbitmq.NewPublisher(conn, 5*time.Second, 1)
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

// rawPublish puts a body on a channel's queue exactly as given, as an older relay would have.
func rawPublish(t *testing.T, url string, ch domain.Channel, body string) {
	t.Helper()
	c, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	amqpCh, err := c.Channel()
	if err != nil {
		t.Fatal(err)
	}
	// The queue may not exist yet, and the default exchange drops a message sent to none.
	if _, err := amqpCh.QueueDeclare(rabbitmq.QueueName(ch), true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := amqpCh.PublishWithContext(context.Background(), "", rabbitmq.QueueName(ch), false, false,
		amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "text/plain", Body: []byte(body)}); err != nil {
		t.Fatal(err)
	}
}

// A publisher splits its ids into messages of at most the message size, and the consumer
// hands each message to the handler whole.
func TestBatchRoundTrip(t *testing.T) {
	broker := rmqtest.Start(t)
	conn := connect(t, broker.URL)
	pub := rabbitmq.NewPublisher(conn, 5*time.Second, 100)
	t.Cleanup(pub.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ids := make([]int64, 250)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	confirmed, err := pub.Publish(ctx, domain.ChannelEmail, ids)
	if err != nil || slices.Contains(confirmed, false) {
		t.Fatalf("publish: %v %v", confirmed, err)
	}
	waitFor(t, "a depth of 3 messages", func() bool {
		n, err := conn.QueueDepth(ctx, domain.ChannelEmail)
		return err == nil && n == 3
	})

	var mu sync.Mutex
	var sizes []int
	got := newCollector()
	handler := func(ctx context.Context, ids []int64) error {
		mu.Lock()
		sizes = append(sizes, len(ids))
		mu.Unlock()
		return got.handle(ctx, ids)
	}
	go rabbitmq.NewConsumer(conn, 10, quiet).Consume(ctx, domain.ChannelEmail, handler) //nolint:errcheck // returns nil on cancel
	waitFor(t, "every id", func() bool { return got.has(ids...) })

	mu.Lock()
	defer mu.Unlock()
	slices.Sort(sizes)
	if !slices.Equal(sizes, []int{50, 100, 100}) {
		t.Errorf("message sizes = %v, want [50 100 100]", sizes)
	}
	for _, id := range ids {
		if n := got.count(id); n != 1 {
			t.Errorf("id %d handled %d times, want once", id, n)
		}
	}
}

// Old single-id bodies, batched bodies and a malformed one can share a queue: the first two
// are handled and the malformed one is dropped without holding up the rest.
func TestMixedFormatsOnOneQueue(t *testing.T) {
	broker := rmqtest.Start(t)
	conn := connect(t, broker.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawPublish(t, broker.URL, domain.ChannelSMS, "7")
	rawPublish(t, broker.URL, domain.ChannelSMS, "1,2,3")
	rawPublish(t, broker.URL, domain.ChannelSMS, "4,x,6")
	rawPublish(t, broker.URL, domain.ChannelSMS, "9")

	got := newCollector()
	go rabbitmq.NewConsumer(conn, 10, quiet).Consume(ctx, domain.ChannelSMS, got.handle) //nolint:errcheck // returns nil on cancel
	waitFor(t, "the valid messages", func() bool { return got.has(1, 2, 3, 7, 9) })
	waitFor(t, "an empty queue", func() bool {
		n, err := conn.QueueDepth(ctx, domain.ChannelSMS)
		return err == nil && n == 0
	})
	if got.has(4) || got.has(6) {
		t.Error("ids of the malformed message were handled")
	}
}

// A handler that fails some ids of a message gets only those back, in a new message.
func TestPartialFailureRepublishesOnlyFailedIDs(t *testing.T) {
	broker := rmqtest.Start(t)
	conn := connect(t, broker.URL)
	pub := rabbitmq.NewPublisher(conn, 5*time.Second, 100)
	t.Cleanup(pub.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := newCollector()
	var failedOnce sync.Once
	handler := func(ctx context.Context, ids []int64) error {
		_ = got.handle(ctx, ids)
		var err error
		failedOnce.Do(func() { err = &ports.BatchError{Failed: []int64{3, 8}, Err: errors.New("db down")} })
		return err
	}
	go rabbitmq.NewConsumer(conn, 10, quiet).WithRepublisher(pub).Consume(ctx, domain.ChannelEmail, handler) //nolint:errcheck // returns nil on cancel
	if _, err := pub.Publish(ctx, domain.ChannelEmail, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the failed ids to come back", func() bool { return got.count(3) == 2 && got.count(8) == 2 })
	time.Sleep(300 * time.Millisecond) // a wrongly requeued message would show by now
	for id := int64(1); id <= 10; id++ {
		want := 1
		if id == 3 || id == 8 {
			want = 2
		}
		if n := got.count(id); n != want {
			t.Errorf("id %d handled %d times, want %d", id, n, want)
		}
	}
}

// Stopping the consumer while a handler works on a message lets it finish the whole message,
// which is acknowledged: nothing is delivered again.
func TestStopMidBatchFinishesAndAcks(t *testing.T) {
	broker := rmqtest.Start(t)
	conn := connect(t, broker.URL)
	pub := rabbitmq.NewPublisher(conn, 5*time.Second, 100)
	t.Cleanup(pub.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	got := newCollector()
	handler := func(ctx context.Context, ids []int64) error {
		close(started)
		<-release
		defer close(done)
		return got.handle(ctx, ids)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		rabbitmq.NewConsumer(conn, 1, quiet).Consume(ctx, domain.ChannelWhatsApp, handler) //nolint:errcheck,gosec // returns nil on cancel
	}()
	if _, err := pub.Publish(ctx, domain.ChannelWhatsApp, []int64{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	close(release)
	<-done
	<-stopped

	// A second consumer must find the queue empty: the message was acknowledged.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	again := newCollector()
	go rabbitmq.NewConsumer(conn, 1, quiet).Consume(ctx2, domain.ChannelWhatsApp, again.handle) //nolint:errcheck // returns nil on cancel
	time.Sleep(500 * time.Millisecond)
	for id := int64(1); id <= 4; id++ {
		if again.count(id) != 0 {
			t.Errorf("id %d of the finished message was delivered again", id)
		}
	}
	if !got.has(1, 2, 3, 4) {
		t.Error("the handler did not finish the message")
	}
}

func TestIDsRoundTrip(t *testing.T) {
	for _, ids := range [][]int64{{1}, {1, 2, 3}, {9223372036854775807, 0, 42}} {
		got, err := rabbitmq.DecodeIDs(rabbitmq.EncodeIDs(ids))
		if err != nil || !slices.Equal(got, ids) {
			t.Errorf("round trip of %v = %v, %v", ids, got, err)
		}
	}
	if string(rabbitmq.EncodeIDs([]int64{123})) != "123" {
		t.Error("a single id is not the old body")
	}
	for _, bad := range []string{"", "1,,2", "a", "1,2,", ",1"} {
		if _, err := rabbitmq.DecodeIDs([]byte(bad)); err == nil {
			t.Errorf("decoded %q without error", bad)
		}
	}
}
