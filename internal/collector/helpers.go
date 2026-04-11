package collector

import "go.opentelemetry.io/collector/pdata/pcommon"

// extractAttributes converts a pcommon.Map into a plain Go string map.
func extractAttributes(attrs pcommon.Map) map[string]string {
	if attrs.Len() == 0 {
		return nil
	}
	result := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	return result
}
