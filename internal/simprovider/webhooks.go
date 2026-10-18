package simprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/provider"
	"github.com/mansonxasthur/notifier/internal/adapters/webhookauth"
	"github.com/mansonxasthur/notifier/internal/config"
)

const (
	webhookAttempts = 3
	webhookTimeout  = 5 * time.Second
	webhookRetry    = 200 * time.Millisecond
)

// Webhooks sends signed webhooks to the API after a delay. It is a lifecycle component:
// stopping it drops the webhooks still waiting.
type Webhooks struct {
	base      string
	delay     time.Duration
	providers map[string]config.Provider
	secrets   map[string]config.Secret
	client    *http.Client
	log       *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWebhooks returns a sender that posts to <cfg.SimProvider.WebhookURL>/v1/webhooks/<provider>.
func NewWebhooks(cfg *config.Config, log *slog.Logger) *Webhooks {
	ctx, cancel := context.WithCancel(context.Background())
	return &Webhooks{
		base:      strings.TrimRight(cfg.SimProvider.WebhookURL, "/"),
		delay:     cfg.SimProvider.WebhookDelay,
		providers: cfg.ProvidersByName(),
		secrets:   cfg.WebhookSecrets,
		client:    &http.Client{Timeout: webhookTimeout},
		log:       log,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (w *Webhooks) Name() string { return "simprovider-webhooks" }

// Start does nothing: webhooks can be scheduled from construction, and Stop ends them.
func (w *Webhooks) Start(context.Context) error { return nil }

func (w *Webhooks) Stop(ctx context.Context) error {
	w.cancel()
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop webhooks: %w", ctx.Err())
	}
}

// Schedule sends the webhook for the provider once the configured delay, plus after, has
// passed. after lets a webhook follow a response that is itself delayed.
func (w *Webhooks) Schedule(providerName string, p provider.WebhookPayload, after time.Duration) {
	w.wg.Go(func() {
		if !sleep(w.ctx, w.delay+after) {
			return
		}
		if err := w.send(providerName, p); err != nil {
			w.log.Error("webhook not delivered", "delivery_id", p.DeliveryID, "provider", providerName, "error", err)
		}
	})
}

func (w *Webhooks) send(providerName string, p provider.WebhookPayload) error {
	cfg, ok := w.providers[providerName]
	if !ok {
		return fmt.Errorf("unknown provider %q", providerName)
	}
	body, err := json.Marshal(p)
	if err != nil {
		return err
	}
	sig, err := webhookauth.Sign(cfg.Webhook.Auth, w.secrets[providerName], body)
	if err != nil {
		return err
	}

	var last error
	for attempt := range webhookAttempts {
		if attempt > 0 && !sleep(w.ctx, webhookRetry) {
			return w.ctx.Err()
		}
		last = w.post(providerName, cfg.Webhook.Header, sig, body)
		if last == nil {
			w.log.Info("webhook sent", "delivery_id", p.DeliveryID, "provider", providerName, "status", p.Status)
			return nil
		}
		// A 4xx is the API refusing the webhook, such as a wrong secret. Sending it again
		// cannot help, and the error must reach the log, not wait out the retries.
		var re rejectedError
		if errors.As(last, &re) {
			return last
		}
	}
	return last
}

// rejectedError is a webhook the API answered with a client error.
type rejectedError int

func (e rejectedError) Error() string { return fmt.Sprintf("rejected with status %d", int(e)) }

func (w *Webhooks) post(providerName, header, sig string, body []byte) error {
	//nolint:gosec // the URL is operator configuration plus a configured provider name
	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, w.base+"/v1/webhooks/"+providerName, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(header, sig)
	resp, err := w.client.Do(req) //nolint:gosec // see the request above
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch resp.StatusCode / 100 {
	case 2:
		return nil
	case 4:
		return rejectedError(resp.StatusCode)
	default:
		return fmt.Errorf("status %d", resp.StatusCode)
	}
}

// sleep waits for d or until ctx ends, and reports whether the full wait happened.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
