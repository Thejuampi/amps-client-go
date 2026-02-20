package amps


import "strconv"
// import "fmt"



// FixMessageShredder struct
type FixMessageShredder struct {
	fieldSeparator byte
}


// Public API

// NewFIXShredder use this method to parse a FIX message.
//
// Example:
// shredder := amps.NewFIXShredder()
//
// Arguments:
// fieldSep [byte] (optional)  -  The default is \x01, a different one can be provided
func NewFIXShredder(fieldSep ...byte) *FixMessageShredder {
	var _fieldSep byte

	if len(fieldSep) > 0 {
		_fieldSep = fieldSep[0]
	} else {
		_fieldSep = '\x01'
	}

	return &FixMessageShredder{_fieldSep}
}


// ToMap use this method to parse a FIX message.
//
// Example:
// shredder := amps.NewFIXShredder()
//
// sow, _ := client.Sow("fix-topic")
// for sow.HasNext() {
//     message := sow.Next()
//     fields := shredder.ToMap(message.Data())
//     for key, value := range fields {
//         fmt.Println("key:", key, "value:", value)
//     }    
// }
//
// Arguments:
// fix [byte buffer]  -  The byte buffer containing the FIX message to be parsed
//
// Returns:
// map  -  A map with integer keys and string values
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
				key, _ = strconv.Atoi(string(fix[delimiterIndex + 1:equalIndex]))
			}
		} else if c == fms.fieldSeparator {
			delimiterIndex = i
			value = string(fix[equalIndex + 1:delimiterIndex])
			fixMap[key] = value
		}
	}

	return fixMap
}
