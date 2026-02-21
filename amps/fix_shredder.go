package amps

import "strconv"

// FixMessageShredder parses protocol payload data into structured parts.
type FixMessageShredder struct {
	fieldSeparator byte
}

// NewFIXShredder returns a new FIXShredder.
func NewFIXShredder(fieldSep ...byte) *FixMessageShredder {
	var _fieldSep byte

	if len(fieldSep) > 0 {
		_fieldSep = fieldSep[0]
	} else {
		_fieldSep = '\x01'
	}

	return &FixMessageShredder{_fieldSep}
}

// ToMap executes the exported tomap operation.
func (fms *FixMessageShredder) ToMap(fix []byte) map[int]string {
	fixMap := make(map[int]string, 0)
	fieldStart := 0
	equalIndex := -1
	parseField := func(fieldEnd int) {
		if equalIndex <= fieldStart || equalIndex >= fieldEnd {
			return
		}
		key, err := strconv.Atoi(string(fix[fieldStart:equalIndex]))
		if err != nil {
			return
		}
		fixMap[key] = string(fix[equalIndex+1 : fieldEnd])
	}

	for i, c := range fix {
		if c == '=' {
			if equalIndex < fieldStart {
				equalIndex = i
			}
			continue
		}
		if c == fms.fieldSeparator {
			parseField(i)
			fieldStart = i + 1
			equalIndex = -1
		}
	}
	if fieldStart < len(fix) {
		parseField(len(fix))
	}

	return fixMap
}
