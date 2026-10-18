package webhookauth

import (
	"testing"

	"github.com/mansonxasthur/notifier/internal/config"
)

func TestSign(t *testing.T) {
	body := []byte(`{"delivery_id":1}`)

	a, err := Sign(config.WebhookHMAC, "s3cret", body)
	if err != nil || len(a) != 64 {
		t.Fatalf("hmac = %q, %v; want 64 hex characters", a, err)
	}
	if b, _ := Sign(config.WebhookHMAC, "s3cret", body); a != b {
		t.Error("the signature must be deterministic")
	}
	if c, _ := Sign(config.WebhookHMAC, "other", body); a == c {
		t.Error("a different secret must change the signature")
	}
	if d, _ := Sign(config.WebhookHMAC, "s3cret", []byte(`{"delivery_id":2}`)); a == d {
		t.Error("a different body must change the signature")
	}

	if got, err := Sign(config.WebhookSharedSecret, "s3cret", body); err != nil || got != "s3cret" {
		t.Errorf("shared secret = %q, %v; want the secret", got, err)
	}
}

func TestSignRefuses(t *testing.T) {
	if _, err := Sign(config.WebhookHMAC, "", nil); err == nil {
		t.Error("an empty secret must be refused")
	}
	if _, err := Sign("bogus", "s3cret", nil); err == nil {
		t.Error("an unknown scheme must be refused")
	}
}
