package relay_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq/memory"
	"github.com/mansonxasthur/notifier/internal/adapters/rabbitmq/rmqtest"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
	"github.com/mansonxasthur/notifier/internal/relay"
	"github.com/mansonxasthur/notifier/internal/sender"
	"log/slog"
)

var (
	quiet   = slog.New(slog.DiscardHandler)
	backoff = domain.Backoff{Initial: 20 * time.Millisecond, Max: 100 * time.Millisecond, Multiplier: 2, Jitter: 0}
	expiry  = time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)
)

func relayConfig() relay.Config {
	return relay.Config{PollInterval: 10 * time.Millisecond, BatchSize: 20, Workers: 3, MaxAttempts: 5, Backoff: backoff}
}

func startDB(t *testing.T) *postgres.Pool {
	t.Helper()
	ctx := context.Background()
	db := postgres.New(pgtest.NewMigratedDB(t))
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Stop(ctx) })
	return db
}

// seed creates n SMS deliveries, each with a NEW outbox row.
func seed(t *testing.T, db *postgres.Pool, n int) {
	t.Helper()
	notification := &domain.Notification{
		IdempotencyKey: uuid.NewString(),
		Type:           "match_alert",
		Content:        map[domain.Channel]domain.Content{domain.ChannelSMS: {Text: "hi"}},
	}
	for i := range n {
		notification.Deliveries = append(notification.Deliveries, domain.Delivery{
			RecipientID: fmt.Sprintf("u_%04d", i), Channel: domain.ChannelSMS, Contact: "+201000000000", ExpiresAt: expiry,
		})
	}
	if _, err := postgres.NewNotificationStore(db, 5000).Create(context.Background(), notification); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// start runs a component and stops it when the test ends.
func start(t *testing.T, c interface {
	Start(context.Context) error
	Stop(context.Context) error
},
) {
	t.Helper()
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(ctx) })
}

// shared gives every relay worker the same concurrency-safe publisher.
func shared(p ports.Publisher) func() ports.Publisher { return func() ports.Publisher { return p } }

// failingPublisher confirms nothing while failing is set.
type failingPublisher struct {
	ports.Publisher
	failing atomic.Bool
}

func (p *failingPublisher) Publish(ctx context.Context, ch domain.Channel, ids []int64) ([]bool, error) {
	if p.failing.Load() {
		return make([]bool, len(ids)), errors.New("broker unavailable")
	}
	return p.Publisher.Publish(ctx, ch, ids)
}

func TestRelayPublishes(t *testing.T) {
	db := startDB(t)
	seed(t, db, 50)
	queue := memory.New()
	start(t, relay.New(postgres.NewOutboxStore(db), shared(queue), relayConfig(), quiet))

	waitFor(t, "every outbox row published", func() bool {
		return count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'PUBLISHED'`) == 50
	})
	if got := queue.Len(domain.ChannelSMS); got != 50 {
		t.Errorf("queue holds %d messages, want 50", got)
	}
}

func TestRelayReleasesUnconfirmedAndRetries(t *testing.T) {
	db := startDB(t)
	seed(t, db, 5)
	pub := &failingPublisher{Publisher: memory.New()}
	pub.failing.Store(true)
	start(t, relay.New(postgres.NewOutboxStore(db), shared(pub), relayConfig(), quiet))

	waitFor(t, "rows released with a reason", func() bool {
		return count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'NEW' AND failure_reason = 'PUBLISH_FAILED' AND retries >= 1`) == 5
	})
	pub.failing.Store(false)
	waitFor(t, "rows published once the broker is back", func() bool {
		return count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'PUBLISHED'`) == 5
	})
}

func TestRelayGivesUpAfterMaxAttempts(t *testing.T) {
	db := startDB(t)
	seed(t, db, 2)
	pub := &failingPublisher{Publisher: memory.New()}
	pub.failing.Store(true)
	cfg := relayConfig()
	cfg.MaxAttempts = 2
	start(t, relay.New(postgres.NewOutboxStore(db), shared(pub), cfg, quiet))

	waitFor(t, "rows dead", func() bool {
		return count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'DEAD' AND failure_reason = 'MAX_ATTEMPTS_EXCEEDED'`) == 2
	})
	if got := count(t, db.Pool(), `SELECT count(*) FROM deliveries WHERE state = 'FAILED'`); got != 2 {
		t.Errorf("failed deliveries = %d, want 2", got)
	}
}

// A relay that dies after claiming leaves rows in PROCESSING. The janitor returns them and
// a relay publishes them.
func TestKilledRelayIsRecovered(t *testing.T) {
	ctx := context.Background()
	db := startDB(t)
	seed(t, db, 30)
	outbox := postgres.NewOutboxStore(db)

	claims, err := outbox.Claim(ctx, 30, 5) // the "killed" relay: claims and never publishes
	if err != nil || len(claims) != 30 {
		t.Fatalf("setup claim = %d, %v", len(claims), err)
	}
	time.Sleep(50 * time.Millisecond)

	queue := memory.New()
	janitor := relay.NewJanitor(outbox, postgres.NewInboxStore(db), fakeChecker{}, relay.JanitorConfig{
		Interval: 20 * time.Millisecond, OutboxStuckAfter: 30 * time.Millisecond,
		InboxStuckMargin: time.Second, Backoff: backoff,
	}, quiet)
	start(t, janitor)
	start(t, relay.New(outbox, shared(queue), relayConfig(), quiet))

	waitFor(t, "all rows published", func() bool {
		return count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state = 'PUBLISHED'`) == 30
	})
	if got := queue.Len(domain.ChannelSMS); got != 30 {
		t.Errorf("queue holds %d messages, want 30", got)
	}
}

func TestJanitorRecoversStuckInbox(t *testing.T) {
	ctx := context.Background()
	db := startDB(t)
	seed(t, db, 2)
	inbox := postgres.NewInboxStore(db)

	var ids []int64
	rows, err := db.Pool().Query(ctx, `SELECT id FROM deliveries ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if _, err := inbox.Claim(ctx, id, "sms-a", 5); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Pool().Exec(ctx, `UPDATE inbox SET last_try = now() - interval '1 minute'`); err != nil {
		t.Fatal(err)
	}

	checker := fakeChecker{ids[0]: ports.CheckAccepted} // the provider has the first one only
	j := relay.NewJanitor(postgres.NewOutboxStore(db), inbox, checker, relay.JanitorConfig{
		Interval: time.Hour, OutboxStuckAfter: time.Hour, InboxStuckMargin: time.Second, Backoff: backoff,
		Providers: []relay.Provider{{Name: "sms-a", Timeout: time.Second}},
	}, quiet)
	j.SweepInbox(ctx)

	if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE delivery_id = $1 AND state = 'DONE'`, ids[0]); got != 1 {
		t.Error("a message the provider confirmed must end DONE")
	}
	if got := count(t, db.Pool(), `SELECT count(*) FROM deliveries WHERE id = $1 AND state = 'ACCEPTED'`, ids[0]); got != 1 {
		t.Error("a message the provider confirmed must be ACCEPTED")
	}
	if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE delivery_id = $1 AND state = 'WAITING_RETRY' AND next_attempt_at IS NOT NULL`, ids[1]); got != 1 {
		t.Error("a message the provider does not know must wait for a retry")
	}
}

type fakeChecker map[int64]ports.CheckResult

func (f fakeChecker) Check(_ context.Context, _ string, id int64) (ports.CheckResult, error) {
	return f[id], nil
}

// countingSend records how many times each delivery was handed to the send step.
type countingSend struct {
	mu    sync.Mutex
	calls map[int64]int
}

func (c *countingSend) send(_ context.Context, claimed sender.Claimed) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[claimed.DeliveryID]++
	return nil
}

func (c *countingSend) total() (distinct, max int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.calls {
		distinct++
		max = maxInt(max, n)
	}
	return distinct, max
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func startSender(t *testing.T, db *postgres.Pool, mq *rabbitmq.Conn) *countingSend {
	t.Helper()
	cs := &countingSend{calls: map[int64]int{}}
	intake := sender.NewIntake(postgres.NewInboxStore(db), map[domain.Channel]string{
		domain.ChannelEmail: "email-a", domain.ChannelSMS: "sms-a", domain.ChannelWhatsApp: "whatsapp-a",
	}, 5, cs.send, quiet)
	start(t, sender.New(rabbitmq.NewConsumer(mq, 10, quiet), intake))
	return cs
}

func startConn(t *testing.T, url string) *rabbitmq.Conn {
	t.Helper()
	mq := rabbitmq.New(url)
	start(t, mq)
	return mq
}

// A message delivered twice is claimed, and so sent, once.
func TestRedeliveredMessageIsHandledOnce(t *testing.T) {
	db := startDB(t)
	seed(t, db, 1)
	broker := rmqtest.Start(t)
	mq := startConn(t, broker.URL)
	cs := startSender(t, db, mq)

	var id int64
	if err := db.Pool().QueryRow(context.Background(), `SELECT id FROM deliveries`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	pub := rabbitmq.NewPublisher(mq, 5*time.Second)
	t.Cleanup(pub.Stop)
	for range 3 {
		if _, err := pub.Publish(context.Background(), domain.ChannelSMS, []int64{id}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "the delivery to be claimed", func() bool { d, _ := cs.total(); return d == 1 })
	time.Sleep(500 * time.Millisecond) // let the duplicates arrive and be dropped
	if _, max := cs.total(); max != 1 {
		t.Errorf("delivery sent %d times, want once", max)
	}
	if got := count(t, db.Pool(), `SELECT count(*) FROM inbox WHERE state = 'PROCESSING' AND claim_count = 1`); got != 1 {
		t.Errorf("inbox rows = %d, want one claimed row", got)
	}
}

// Rows seeded before and after a broker restart all reach a sender; nothing stays stuck.
func TestBrokerRestartLosesNothing(t *testing.T) {
	const wave = 40
	db := startDB(t)
	broker := rmqtest.Start(t)
	mq := startConn(t, broker.URL)
	cs := startSender(t, db, mq)
	// The outage must fit inside the attempt cap, or the rows would end DEAD by design.
	cfg := relayConfig()
	cfg.MaxAttempts = 1000
	start(t, relay.New(postgres.NewOutboxStore(db), func() ports.Publisher { return rabbitmq.NewPublisher(mq, 2*time.Second) }, cfg, quiet))

	seed(t, db, wave)
	broker.Restart(t)
	seed(t, db, wave) // a second notification: same recipient IDs, new idempotency key

	waitFor(t, "every delivery claimed by a sender", func() bool { d, _ := cs.total(); return d == 2*wave })
	waitFor(t, "every outbox row published", func() bool {
		return count(t, db.Pool(), `SELECT count(*) FROM outbox WHERE state <> 'PUBLISHED'`) == 0
	})
	if got := count(t, db.Pool(), `SELECT count(*) FROM inbox`); got != 2*wave {
		t.Errorf("inbox rows = %d, want %d", got, 2*wave)
	}
}
