package amps

// NvfixMessageShredder parses protocol payload data into structured parts.
type NvfixMessageShredder struct {
	fieldSeparator byte
}

// NewNVFIXShredder returns a new NVFIXShredder.
func NewNVFIXShredder(fieldSep ...byte) *NvfixMessageShredder {
	var _fieldSep byte

	if len(fieldSep) > 0 {
		_fieldSep = fieldSep[0]
	} else {
		_fieldSep = '\x01'
	}

	return &NvfixMessageShredder{_fieldSep}
}

// ToMap executes the exported tomap operation.
func (nfs *NvfixMessageShredder) ToMap(nvfix []byte) map[string]string {
	nvfixMap := make(map[string]string, 0)
	delimiterIndex := 0
	equalIndex := 0
	key := ""
	value := ""

	for i, c := range nvfix {
		if c == '=' {
			equalIndex = i
			if key == "" && value == "" {
				key = string(nvfix[delimiterIndex:equalIndex])
			} else {
				key = string(nvfix[delimiterIndex+1 : equalIndex])
			}
		} else if c == nfs.fieldSeparator {
			delimiterIndex = i
			value = string(nvfix[equalIndex+1 : delimiterIndex])
			nvfixMap[key] = value
		}
	}

	return nvfixMap
}
