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
	delimiterIndex := 0
	equalIndex := 0
	key := 0
	value := ""

	for i, c := range fix {
		if c == '=' {
			equalIndex = i
			if key == 0 && value == "" {
				key, _ = strconv.Atoi(string(fix[delimiterIndex:equalIndex]))
			} else {
				key, _ = strconv.Atoi(string(fix[delimiterIndex+1 : equalIndex]))
			}
		} else if c == fms.fieldSeparator {
			delimiterIndex = i
			value = string(fix[equalIndex+1 : delimiterIndex])
			fixMap[key] = value
		}
	}

	return fixMap
}
