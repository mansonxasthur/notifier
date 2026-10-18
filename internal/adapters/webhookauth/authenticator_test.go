package webhookauth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
)

func request(header, value string) *http.Request {
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/webhooks/sms-a", nil)
	if header != "" {
		r.Header.Set(header, value)
	}
	return r
}

func TestHMACAuthenticator(t *testing.T) {
	a, err := New(config.Webhook{Auth: config.WebhookHMAC, Header: "X-Signature", SecretEnv: "S"}, "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"delivery_id":1,"status":"delivered"}`)
	good, _ := Sign(config.WebhookHMAC, "s3cret", body)
	wrongKey, _ := Sign(config.WebhookHMAC, "other", body)

	tests := []struct {
		name string
		req  *http.Request
		body []byte
		ok   bool
	}{
		{"valid", request("X-Signature", good), body, true},
		{"upper case hex", request("X-Signature", strings.ToUpper(good)), body, true},
		{"wrong key", request("X-Signature", wrongKey), body, false},
		{"tampered body", request("X-Signature", good), []byte(`{"delivery_id":2,"status":"delivered"}`), false},
		{"missing header", request("", ""), body, false},
		{"empty header", request("X-Signature", ""), body, false},
		{"signature in another header", request("X-Other", good), body, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := a.Authenticate(tt.req, tt.body)
			if tt.ok != (err == nil) {
				t.Errorf("Authenticate = %v, want ok=%v", err, tt.ok)
			}
			if !tt.ok && !errors.Is(err, ErrUnauthenticated) {
				t.Errorf("err = %v, want ErrUnauthenticated", err)
			}
		})
	}
}

func TestSharedSecretAuthenticator(t *testing.T) {
	a, err := New(config.Webhook{Auth: config.WebhookSharedSecret, Header: "X-Webhook-Token", SecretEnv: "S"}, "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	for name, tt := range map[string]struct {
		req *http.Request
		ok  bool
	}{
		"valid":          {request("X-Webhook-Token", "s3cret"), true},
		"wrong secret":   {request("X-Webhook-Token", "nope"), false},
		"missing header": {request("", ""), false},
		"prefix only":    {request("X-Webhook-Token", "s3cre"), false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := a.Authenticate(tt.req, []byte("{}")); tt.ok != (err == nil) {
				t.Errorf("Authenticate = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}

func TestNewRefuses(t *testing.T) {
	if _, err := New(config.Webhook{Auth: config.WebhookHMAC, Header: "H", SecretEnv: "S"}, ""); err == nil {
		t.Error("an empty secret must be refused")
	}
	if _, err := New(config.Webhook{Auth: "bogus", Header: "H", SecretEnv: "S"}, "x"); err == nil {
		t.Error("an unknown scheme must be refused")
	}
}

func TestFromConfig(t *testing.T) {
	provider := func(name string, auth config.WebhookAuth) config.Provider {
		return config.Provider{Name: name, Webhook: config.Webhook{Auth: auth, Header: "H", SecretEnv: "E_" + name}}
	}
	cfg := func(secrets map[string]config.Secret) *config.Config {
		return &config.Config{
			WebhookSecrets: secrets,
			Channels: map[domain.Channel]config.Channel{
				domain.ChannelSMS:   {Providers: []config.Provider{provider("sms-a", config.WebhookHMAC), provider("sms-b", config.WebhookSharedSecret)}},
				domain.ChannelEmail: {Providers: []config.Provider{provider("email-a", config.WebhookHMAC)}},
			},
		}
	}

	got, err := FromConfig(cfg(map[string]config.Secret{"sms-a": "1", "sms-b": "2", "email-a": "3"}))
	if err != nil || len(got) != 3 || got["sms-a"] == nil || got["sms-b"] == nil || got["email-a"] == nil {
		t.Fatalf("FromConfig = %v, %v; want an authenticator per provider", got, err)
	}

	_, err = FromConfig(cfg(map[string]config.Secret{"sms-a": "1", "sms-b": "2"}))
	if err == nil || !strings.Contains(err.Error(), "email-a") {
		t.Errorf("err = %v, want the provider with no secret named", err)
	}
}
