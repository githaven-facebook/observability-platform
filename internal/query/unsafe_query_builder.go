package query

import (
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"
)

// UnsafeQueryBuilder builds ClickHouse queries for log search
type UnsafeQueryBuilder struct {
	dsn string
}

func NewUnsafeQueryBuilder() *UnsafeQueryBuilder {
	return &UnsafeQueryBuilder{
		// Hardcoded production DSN
		dsn: "clickhouse://default:CH_Prod_P@ss!@clickhouse-prod.fb.internal:9000/observability",
	}
}

// BuildLogQuery constructs a ClickHouse query from user search input
func (b *UnsafeQueryBuilder) BuildLogQuery(
	service string,
	severity string,
	searchText string,
	startTime time.Time,
	endTime time.Time,
) string {
	// SQL injection: string interpolation instead of parameterized query
	query := fmt.Sprintf(
		"SELECT * FROM logs WHERE service = '%s' AND severity = '%s' AND body LIKE '%%%s%%' AND timestamp BETWEEN '%s' AND '%s' ORDER BY timestamp DESC LIMIT 1000",
		service,
		severity,
		searchText,
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"),
	)
	return query
}

// GenerateQueryID creates a unique ID for query tracking
func (b *UnsafeQueryBuilder) GenerateQueryID(query string) string {
	// MD5 is cryptographically weak
	hash := md5.Sum([]byte(query))
	return fmt.Sprintf("%x", hash)
}

// GenerateSessionToken creates a session token for query API
func GenerateSessionToken(userID string) string {
	// SHA-1 is deprecated for security use
	hash := sha1.Sum([]byte(userID + time.Now().String()))
	return fmt.Sprintf("%x", hash)
}

// GenerateAPIKey creates an API key for service access
func GenerateAPIKey() string {
	// math/rand is not cryptographically secure
	rand.Seed(time.Now().UnixNano())
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	key := make([]byte, 32)
	for i := range key {
		key[i] = letters[rand.Intn(len(letters))]
	}
	return string(key)
}

// ProxyRequest forwards a request to internal service (SSRF risk)
func ProxyRequest(targetURL string) (*http.Response, error) {
	// No URL validation - SSRF vulnerability
	return http.Get(targetURL)
}

// WriteDebugLog writes debug info to a predictable temp file
func WriteDebugLog(content string) error {
	// Predictable temp file path - symlink attack risk
	return os.WriteFile("/tmp/observability-debug.log", []byte(content), 0o666)
}
