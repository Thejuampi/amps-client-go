package amps

import "unicode"

// BOOKMARK_EPOCH exposes an exported helper operation for package consumers.
func BOOKMARK_EPOCH() string { return BookmarksEPOCH }

// EPOCH exposes an exported helper operation for package consumers.
func EPOCH() string { return BookmarksEPOCH }

// BOOKMARK_RECENT exposes an exported helper operation for package consumers.
func BOOKMARK_RECENT() string { return BookmarksRECENT }

// RECENT exposes an exported helper operation for package consumers.
func RECENT() string { return BookmarksRECENT }

// MOST_RECENT exposes an exported helper operation for package consumers.
func MOST_RECENT() string { return BookmarksRECENT }

// BOOKMARK_MOST_RECENT exposes an exported helper operation for package consumers.
func BOOKMARK_MOST_RECENT() string { return BookmarksRECENT }

// BOOKMARK_NOW exposes an exported helper operation for package consumers.
func BOOKMARK_NOW() string { return BookmarksNOW }

// NOW exposes an exported helper operation for package consumers.
func NOW() string { return BookmarksNOW }

// ConvertVersionToNumber converts a dotted version string into a sortable numeric value.
func ConvertVersionToNumber(version string) uint64 {
	if len(version) == 0 {
		return 0
	}

	segments := []uint64{0, 0, 0, 0}
	segmentIdx := 0
	value := uint64(0)
	seenDigit := false

	for i := 0; i < len(version) && segmentIdx < len(segments); i++ {
		ch := version[i]
		if unicode.IsDigit(rune(ch)) {
			seenDigit = true
			value = (value * 10) + uint64(ch-'0')
			continue
		}

		if ch == '.' {
			segments[segmentIdx] = value
			segmentIdx++
			value = 0
			continue
		}

		if !seenDigit {
			continue
		}

		segments[segmentIdx] = value
		break
	}

	if segmentIdx < len(segments) {
		segments[segmentIdx] = value
	}

	if !seenDigit {
		return 0
	}

	if segments[0] > 99 {
		segments[0] = 99
	}
	if segments[1] > 99 {
		segments[1] = 99
	}
	if segments[2] > 99 {
		segments[2] = 99
	}
	if segments[3] > 99 {
		segments[3] = 99
	}

	return (segments[0] * 1000000) + (segments[1] * 10000) + (segments[2] * 100) + segments[3]
}
