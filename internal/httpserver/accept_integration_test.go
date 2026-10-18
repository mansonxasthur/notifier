package httpserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/postgres"
	"github.com/mansonxasthur/notifier/internal/adapters/postgres/pgtest"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/httpserver"
)

type response struct {
	Accepted   int                `json:"accepted"`
	Duplicates int                `json:"duplicates"`
	Rejected   []domain.Rejection `json:"rejected"`
}

// A 1,000-recipient batch with one repeated recipient and one invalid contact returns the
// expected counts, and the database holds exactly the accepted rows.
func TestAcceptBatchEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := postgres.New(pgtest.NewMigratedDB(t))
	if err := db.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Stop(ctx) })

	mux := http.NewServeMux()
	err := httpserver.RegisterNotifications(mux, httpserver.NotificationsOptions{
		Log:    slog.New(slog.DiscardHandler),
		Store:  postgres.NewNotificationStore(db, 5000),
		APIKey: "k",
		Policy: domain.Policy{
			BatchLimit: 3000,
			Expiry:     map[string]time.Duration{"match_alert": 2 * time.Hour},
		},
		MaxBodyBytes: 4 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	var recipients []string
	for i := range 998 { // 998 valid
		recipients = append(recipients, fmt.Sprintf(
			`{"recipient_id":"u_%04d","channels":{"sms":"+2010%08d"}}`, i, i))
	}
	recipients = append(recipients,
		`{"recipient_id":"u_0000","channels":{"sms":"+201000000000"}}`, // repeat of the first
		`{"recipient_id":"u_bad","channels":{"sms":"not-a-number"}}`)   // invalid
	payload := fmt.Sprintf(`{"idempotency_key":"0b6f6b3e-6d4e-4d6e-9d57-2f1d3f0f6f10","type":"match_alert",
		"content":{"sms":{"text":"مباراة الهلال والنصر"}},"recipients":[%s]}`, strings.Join(recipients, ","))

	send := func() (int, response) {
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/notifications", strings.NewReader(payload))
		req.Header.Set("X-API-KEY", "k")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		var r response
		if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
			t.Fatalf("body %q: %v", rec.Body, err)
		}
		return rec.Code, r
	}
	rows := func(query string) int {
		var n int
		if err := db.Pool().QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	code, got := send()
	if code != http.StatusAccepted || got.Accepted != 998 || got.Duplicates != 1 ||
		len(got.Rejected) != 1 || got.Rejected[0].RecipientID != "u_bad" || got.Rejected[0].Reason != domain.ReasonInvalidContact {
		t.Fatalf("first send: %d %+v", code, got)
	}
	if n := rows(`SELECT count(*) FROM deliveries`); n != 998 {
		t.Errorf("deliveries = %d, want 998", n)
	}
	if n := rows(`SELECT count(*) FROM outbox WHERE state = 'NEW'`); n != 998 {
		t.Errorf("outbox rows = %d, want 998", n)
	}
	if n := rows(`SELECT count(*) FROM notifications`); n != 1 {
		t.Errorf("notifications = %d, want 1", n)
	}

	// The same request again creates nothing: every row is now a duplicate.
	code, got = send()
	if code != http.StatusAccepted || got.Accepted != 0 || got.Duplicates != 999 || len(got.Rejected) != 1 {
		t.Fatalf("replay: %d %+v", code, got)
	}
	if n := rows(`SELECT count(*) FROM deliveries`); n != 998 {
		t.Errorf("deliveries after replay = %d, want 998", n)
	}
	if n := rows(`SELECT count(*) FROM outbox`); n != 998 {
		t.Errorf("outbox rows after replay = %d, want 998", n)
	}
}
