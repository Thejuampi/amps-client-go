package amps

import "unicode"

// C++ parity bookmark aliases.
func BOOKMARK_EPOCH() string { return BookmarksEPOCH }

func EPOCH() string { return BookmarksEPOCH }

func BOOKMARK_RECENT() string { return BookmarksRECENT }

func RECENT() string { return BookmarksRECENT }

func MOST_RECENT() string { return BookmarksRECENT }

func BOOKMARK_MOST_RECENT() string { return BookmarksRECENT }

func BOOKMARK_NOW() string { return BookmarksNOW }

func NOW() string { return BookmarksNOW }

// ConvertVersionToNumber converts dotted AMPS version strings to a numeric representation.
// Example: 3.8.1.4 -> 3080104.
func ConvertVersionToNumber(version string) uint64 {
	if len(version) == 0 {
		return 0
	}

	segments := []uint64{0, 0, 0, 0}
	segmentIdx := 0
	value := uint64(0)
	seenDigit := false

	for i := 0; i < len(version) && segmentIdx < len(segments); i++ {
		ch := rune(version[i])
		if unicode.IsDigit(ch) {
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
