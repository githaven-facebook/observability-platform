package api

import (
	"net/http"
	"strings"

	"go.uber.org/zap"
)

// authMiddleware validates the Authorization header when one is present.
// In production this would verify a signed JWT; for now it passes through
// any bearer token to allow unauthenticated development use.
func authMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader != "" {
				parts := strings.SplitN(authHeader, " ", 2)
				if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
					jsonError(w, "invalid authorization header format", http.StatusUnauthorized)
					return
				}
				// Token present; log for audit purposes.
				logger.Debug("authenticated request",
					zap.String("path", r.URL.Path),
					zap.String("token_prefix", safeTokenPrefix(parts[1])),
				)
			}
			next.ServeHTTP(w, r)
		})
	}
}

func safeTokenPrefix(token string) string {
	if len(token) <= 8 {
		return "***"
	}
	return token[:8] + "..."
}
