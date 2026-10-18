package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
	"github.com/mansonxasthur/notifier/internal/domain"
)

var expiry = time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)

func newNotification(key string, recipients ...string) *domain.Notification {
	n := &domain.Notification{
		IdempotencyKey: key,
		Type:           "match_alert",
		Content:        map[domain.Channel]domain.Content{domain.ChannelSMS: {Text: "مرحبا"}},
	}
	for _, r := range recipients {
		n.Deliveries = append(n.Deliveries, domain.Delivery{
			RecipientID: r, Channel: domain.ChannelSMS, Contact: "+201000000000", ExpiresAt: expiry,
		})
	}
	return n
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCreate(t *testing.T) {
	ctx := context.Background()
	url := pgtest.NewMigratedDB(t)
	db := postgres.New(url)
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Stop(ctx) })
	pool := db.Pool()

	const chunk = 5000
	store := postgres.NewNotificationStore(db, chunk)
	key := func(name string) string { return uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String() }

	t.Run("stores the notification, deliveries and one outbox row each", func(t *testing.T) {
		n := newNotification(key("store"), "u_2", "u_1")
		res, err := store.Create(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		if res.Created != 2 || res.Duplicates != 0 || n.ID == 0 {
			t.Fatalf("res = %+v, id = %d", res, n.ID)
		}
		if got := count(t, pool, `SELECT count(*) FROM deliveries WHERE notification_id = $1 AND state = 'PENDING' AND expires_at = $2`, n.ID, expiry); got != 2 {
			t.Errorf("deliveries = %d, want 2 pending with the stamped expiry", got)
		}
		q := `SELECT count(*) FROM outbox o JOIN deliveries d ON d.id = o.delivery_id
			WHERE d.notification_id = $1 AND o.state = 'NEW' AND o.retries = 0 AND o.next_attempt_at IS NULL`
		if got := count(t, pool, q, n.ID); got != 2 {
			t.Errorf("outbox rows = %d, want 2 new", got)
		}
		var contact, content string
		err = pool.QueryRow(ctx, `SELECT d.contact->>'address', n.content->'sms'->>'text'
			FROM deliveries d JOIN notifications n ON n.id = d.notification_id
			WHERE n.id = $1 LIMIT 1`, n.ID).Scan(&contact, &content)
		if err != nil || contact != "+201000000000" || content != "مرحبا" {
			t.Errorf("contact = %q, content = %q, err = %v", contact, content, err)
		}
	})

	t.Run("a replay creates nothing and returns the same id", func(t *testing.T) {
		first := newNotification(key("replay"), "u_1", "u_2")
		if _, err := store.Create(ctx, first); err != nil {
			t.Fatal(err)
		}
		before := count(t, pool, `SELECT count(*) FROM deliveries`)
		beforeOutbox := count(t, pool, `SELECT count(*) FROM outbox`)

		again := newNotification(key("replay"), "u_1", "u_2")
		res, err := store.Create(ctx, again)
		if err != nil {
			t.Fatal(err)
		}
		if res.Created != 0 || res.Duplicates != 2 || again.ID != first.ID {
			t.Fatalf("res = %+v, ids %d and %d", res, first.ID, again.ID)
		}
		if got := count(t, pool, `SELECT count(*) FROM deliveries`); got != before {
			t.Errorf("deliveries grew from %d to %d", before, got)
		}
		if got := count(t, pool, `SELECT count(*) FROM outbox`); got != beforeOutbox {
			t.Errorf("outbox grew from %d to %d", beforeOutbox, got)
		}
	})

	t.Run("the same key with a different payload keeps the first content", func(t *testing.T) {
		first := newNotification(key("payload"), "u_1")
		if _, err := store.Create(ctx, first); err != nil {
			t.Fatal(err)
		}
		other := newNotification(key("payload"), "u_1")
		other.Type = "marketing"
		other.Content = map[domain.Channel]domain.Content{domain.ChannelSMS: {Text: "changed"}}
		res, err := store.Create(ctx, other)
		if err != nil {
			t.Fatal(err)
		}
		if res.Created != 0 || res.Duplicates != 1 {
			t.Fatalf("res = %+v", res)
		}
		var typ, text string
		if err := pool.QueryRow(ctx, `SELECT type, content->'sms'->>'text' FROM notifications WHERE id = $1`, first.ID).Scan(&typ, &text); err != nil {
			t.Fatal(err)
		}
		if typ != "match_alert" || text != "مرحبا" {
			t.Errorf("first request should win, got type %q text %q", typ, text)
		}
	})

	t.Run("a resend adds only the new deliveries, each with an outbox row", func(t *testing.T) {
		first := newNotification(key("resend"), "u_1")
		if _, err := store.Create(ctx, first); err != nil {
			t.Fatal(err)
		}
		resend := newNotification(key("resend"), "u_1", "u_2")
		res, err := store.Create(ctx, resend)
		if err != nil {
			t.Fatal(err)
		}
		if res.Created != 1 || res.Duplicates != 1 {
			t.Fatalf("res = %+v", res)
		}
		q := `SELECT count(*) FROM outbox o JOIN deliveries d ON d.id = o.delivery_id WHERE d.notification_id = $1`
		if got := count(t, pool, q, first.ID); got != 2 {
			t.Errorf("outbox rows = %d, want 2", got)
		}
	})

	t.Run("a request of 9,000 deliveries runs as two statements in one transaction", func(t *testing.T) {
		n := &domain.Notification{
			IdempotencyKey: key("big"), Type: "match_alert",
			Content: map[domain.Channel]domain.Content{
				domain.ChannelEmail: {Body: "b", Subject: "s"}, domain.ChannelSMS: {Text: "t"}, domain.ChannelWhatsApp: {Body: "w"},
			},
		}
		for i := range 3000 {
			for _, ch := range domain.Channels() {
				n.Deliveries = append(n.Deliveries, domain.Delivery{
					RecipientID: fmt.Sprintf("u_%05d", i), Channel: ch, Contact: "x", ExpiresAt: expiry,
				})
			}
		}
		res, err := store.Create(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		if res.Created != 9000 || res.Duplicates != 0 {
			t.Fatalf("res = %+v", res)
		}
		q := `SELECT count(*) FROM outbox o JOIN deliveries d ON d.id = o.delivery_id WHERE d.notification_id = $1`
		if got := count(t, pool, q, n.ID); got != 9000 {
			t.Errorf("outbox rows = %d, want 9000", got)
		}
	})

	t.Run("a failure in a later chunk rolls everything back", func(t *testing.T) {
		small := postgres.NewNotificationStore(db, 2)
		n := newNotification(key("rollback"), "a", "b", "c", "d\x00bad")
		n.ID = 0
		if _, err := small.Create(ctx, n); err == nil {
			t.Fatal("want an error from the bad recipient id")
		}
		if n.ID != 0 {
			t.Errorf("id = %d, a failed Create must not set it", n.ID)
		}
		if got := count(t, pool, `SELECT count(*) FROM notifications WHERE idempotency_key = $1`, key("rollback")); got != 0 {
			t.Errorf("notification rows = %d, want 0", got)
		}
		q := `SELECT count(*) FROM deliveries WHERE recipient_id IN ('a','b','c') AND notification_id NOT IN (SELECT id FROM notifications)`
		if got := count(t, pool, q); got != 0 {
			t.Errorf("orphan deliveries = %d, want 0", got)
		}
	})

	t.Run("a non-positive chunk size is an error, not a panic", func(t *testing.T) {
		if _, err := postgres.NewNotificationStore(db, 0).Create(ctx, newNotification(key("chunk"), "u")); err == nil {
			t.Fatal("want an error")
		}
	})

	t.Run("a request with no deliveries is refused", func(t *testing.T) {
		if _, err := store.Create(ctx, newNotification(key("none"))); err == nil {
			t.Fatal("want an error")
		}
	})

	t.Run("overlapping concurrent requests neither deadlock nor double insert", func(t *testing.T) {
		const workers = 8
		ids := make([]string, 40)
		for i := range ids {
			ids[i] = fmt.Sprintf("u_%02d", i)
		}
		var wg sync.WaitGroup
		results := make([]int, workers)
		notes := make([]*domain.Notification, workers)
		errs := make([]error, workers)
		small := postgres.NewNotificationStore(db, 7)
		for w := range workers {
			order := make([]string, len(ids))
			for i, id := range ids { // each worker submits the same recipients in a different order
				order[(i*(w+1)*3+w)%len(ids)] = id
			}
			seen := map[string]bool{}
			var unique []string
			for _, id := range order {
				if id != "" && !seen[id] {
					seen[id] = true
					unique = append(unique, id)
				}
			}
			notes[w] = newNotification(key("concurrent"), unique...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := small.Create(ctx, notes[w])
				results[w], errs[w] = res.Created, err
			}()
		}
		wg.Wait()

		created, total := 0, 0
		for w := range workers {
			if errs[w] != nil {
				t.Fatalf("worker %d: %v", w, errs[w])
			}
			if notes[w].ID != notes[0].ID {
				t.Errorf("worker %d got notification %d, want %d", w, notes[w].ID, notes[0].ID)
			}
			created += results[w]
			total += len(notes[w].Deliveries)
		}
		got := count(t, pool, `SELECT count(*) FROM deliveries WHERE notification_id = $1`, notes[0].ID)
		if created != got {
			t.Errorf("workers reported %d created, table has %d", created, got)
		}
		q := `SELECT count(*) FROM outbox o JOIN deliveries d ON d.id = o.delivery_id WHERE d.notification_id = $1`
		if out := count(t, pool, q, notes[0].ID); out != got {
			t.Errorf("outbox rows = %d, deliveries = %d", out, got)
		}
		if total <= got {
			t.Errorf("test needs overlap: %d submitted, %d stored", total, got)
		}
	})
}
