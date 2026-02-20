package bookmark

import "strings"

// NormalizeSubID splits comma-delimited sub IDs and returns the first normalized value.
func NormalizeSubID(subIDs string) string {
	parts := strings.Split(subIDs, ",")
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			return value
		}
	}
	return ""
}
