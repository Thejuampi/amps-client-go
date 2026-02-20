package bookmark

import "strings"

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
