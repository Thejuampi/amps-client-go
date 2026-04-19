package safecast

import "math"

func Int64FromUint64Saturating(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}

func Int64FromUint64Checked(value uint64) (int64, bool) {
	if value > math.MaxInt64 {
		return 0, false
	}
	return int64(value), true
}

func Uint16FromIntChecked(value int) (uint16, bool) {
	if value < 0 || value > math.MaxUint16 {
		return 0, false
	}
	return uint16(value), true
}

func Uint32FromIntChecked(value int) (uint32, bool) {
	if value < 0 || uint64(value) > math.MaxUint32 {
		return 0, false
	}
	return uint32(value), true // #nosec G115 -- bounds checked above.
}

func Int32FromIntChecked(value int) (int32, bool) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, false
	}
	return int32(value), true
}

func Uint64FromIntChecked(value int) (uint64, bool) {
	if value < 0 {
		return 0, false
	}
	return uint64(value), true
}

func Uint64FromInt64Checked(value int64) (uint64, bool) {
	if value < 0 {
		return 0, false
	}
	return uint64(value), true
}
