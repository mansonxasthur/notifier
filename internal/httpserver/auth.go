package httpserver

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
)

// apiKeyHeader carries the key producers authenticate with.
const apiKeyHeader = "X-API-KEY"

// requireAPIKey answers 401 unless the request carries the configured key, which must not be empty. Both values are
// hashed first so the comparison takes the same time whatever their lengths.
func requireAPIKey(key string, next http.Handler) http.Handler {
	want := sha256.Sum256([]byte(key))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := sha256.Sum256([]byte(r.Header.Get(apiKeyHeader)))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			writeError(w, http.StatusUnauthorized, codeUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
