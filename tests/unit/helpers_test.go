package unit_test

import "go.uber.org/zap"

// noopLogger returns a no-op zap logger suitable for unit tests.
func noopLogger() *zap.Logger {
	return zap.NewNop()
}
