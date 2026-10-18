// Package webhookauth signs and verifies provider webhooks. Each provider names its scheme
// in configuration; the simulated provider signs with the same function the API verifies with.
package webhookauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/mansonxasthur/notifier/internal/config"
)

// Sign returns the header value that authenticates body under the scheme: the hex
// HMAC-SHA256 of the body for hmac_signature, or the secret itself for shared_secret_header.
// An empty secret authenticates nothing and is refused.
func Sign(auth config.WebhookAuth, secret config.Secret, body []byte) (string, error) {
	if secret == "" {
		return "", fmt.Errorf("sign webhook: empty secret")
	}
	switch auth {
	case config.WebhookHMAC:
		mac := hmac.New(sha256.New, []byte(secret.Reveal()))
		mac.Write(body)
		return hex.EncodeToString(mac.Sum(nil)), nil
	case config.WebhookSharedSecret:
		return secret.Reveal(), nil
	default:
		return "", fmt.Errorf("sign webhook: unknown scheme %q", auth)
	}
}
