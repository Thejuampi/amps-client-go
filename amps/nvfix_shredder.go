package amps


// import "errors"


// NvfixMessageShredder struct
type NvfixMessageShredder struct {
	fieldSeparator byte
}


// Public API

// NewNVFIXShredder use this method to parse a FIX message.
//
// Example:
// shredder := amps.NewNVFIXShredder()
//
// Arguments:
// fieldSep [byte] (optional)  -  The default is \x01, a different one can be provided
func NewNVFIXShredder(fieldSep ...byte) *NvfixMessageShredder {
	var _fieldSep byte

	if len(fieldSep) > 0 {
		_fieldSep = fieldSep[0]
	} else {
		_fieldSep = '\x01'
	}

	return &NvfixMessageShredder{_fieldSep}
}


// ToMap use this method to parse a NVFIX message.
//
// Example:
// shredder := amps.NewNVFIXShredder()
//
// sow, _ := client.Sow("nvfix-topic")
// for sow.HasNext() {
//     message := sow.Next()
//     fields := shredder.ToMap(message.Data())
//     for key, value := range fields {
//         fmt.Println("key:", key, "value:", value)
//     }    
// }
//
// Arguments:
// nvfix [byte buffer]  -  The byte buffer containing the NVFIX message to be parsed
//
// Returns:
// map  -  A map with string keys and string values
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
				key = string(nvfix[delimiterIndex + 1:equalIndex])
			}
		} else if c == nfs.fieldSeparator {
			delimiterIndex = i
			value = string(nvfix[equalIndex + 1:delimiterIndex])
			nvfixMap[key] = value
		}
	}

	return nvfixMap
}
