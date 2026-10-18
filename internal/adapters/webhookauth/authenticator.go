package webhookauth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// ErrUnauthenticated means the webhook did not carry a valid signature or secret.
var ErrUnauthenticated = errors.New("webhook not authenticated")

var _ ports.WebhookAuthenticator = (*headerAuthenticator)(nil)

// headerAuthenticator accepts a request whose header equals what Sign produces for the
// body. It covers both configured schemes: the HMAC of the body, and the shared secret.
// A provider with a different scheme, such as a timestamped signature, gets its own type.
type headerAuthenticator struct {
	auth   config.WebhookAuth
	header string
	secret config.Secret
}

// Authenticate implements ports.WebhookAuthenticator.
func (a headerAuthenticator) Authenticate(r *http.Request, body []byte) error {
	want, err := Sign(a.auth, a.secret, body)
	if err != nil {
		return err
	}
	// Hex digests are case-insensitive; the shared secret is not.
	got := r.Header.Get(a.header)
	if a.auth == config.WebhookHMAC {
		got = strings.ToLower(got)
	}
	if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return ErrUnauthenticated
	}
	return nil
}

// New returns the authenticator for a provider's webhook configuration. It refuses an
// empty secret, which would let anyone who knows the scheme forge a webhook.
func New(w config.Webhook, secret config.Secret) (ports.WebhookAuthenticator, error) {
	if secret == "" {
		return nil, fmt.Errorf("webhook secret (%s) is empty", w.SecretEnv)
	}
	switch w.Auth {
	case config.WebhookHMAC, config.WebhookSharedSecret:
		return headerAuthenticator{auth: w.Auth, header: w.Header, secret: secret}, nil
	default:
		return nil, fmt.Errorf("unknown webhook scheme %q", w.Auth)
	}
}

// FromConfig builds the authenticator of every configured provider, by provider name.
func FromConfig(cfg *config.Config) (map[string]ports.WebhookAuthenticator, error) {
	out := map[string]ports.WebhookAuthenticator{}
	var errs []error
	for name, p := range cfg.ProvidersByName() {
		a, err := New(p.Webhook, cfg.WebhookSecrets[name])
		if err != nil {
			errs = append(errs, fmt.Errorf("provider %s: %w", name, err))
			continue
		}
		out[name] = a
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return out, nil
}
