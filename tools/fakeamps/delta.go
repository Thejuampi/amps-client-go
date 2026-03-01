package main

import "strings"

// ---------------------------------------------------------------------------
// Delta merge — merge a delta JSON document into an existing base document.
//
// Real AMPS merges delta_publish payloads into the existing SOW record:
// - New fields are added
// - Existing fields are updated
// - Fields set to null are removed (JSON merge patch semantics, RFC 7396)
// - Nested objects are recursively merged (not replaced wholesale)
//
// This implementation does recursive merge for JSON objects.
// ---------------------------------------------------------------------------

// mergeJSON merges delta into base, returning the merged result.
// Both must be JSON objects (e.g. {"a":1,"b":{"c":2}}).
// Nested objects are recursively merged per RFC 7396.
func mergeJSON(base, delta []byte) []byte {
	baseFields := parseJSONFieldsRaw(base)
	deltaFields := parseJSONFieldsRaw(delta)

	// Apply delta over base.
	for k, dv := range deltaFields {
		if dv == "null" {
			delete(baseFields, k)
			continue
		}
		bv, exists := baseFields[k]
		// If both are objects, merge recursively.
		if exists && isJSONObject(bv) && isJSONObject(dv) {
			baseFields[k] = string(mergeJSON([]byte(bv), []byte(dv)))
		} else {
			baseFields[k] = dv
		}
	}

	// Reconstruct JSON.
	return buildFlatJSON(baseFields)
}

func isJSONObject(raw string) bool {
	s := strings.TrimSpace(raw)
	return len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}'
}

// parseJSONFieldsRaw extracts top-level key-value pairs from a JSON object.
// Returns a map of key → raw value string (preserving quotes, nested objects, etc.).
// This is the authoritative parser used by delta merge, projection, etc.
func parseJSONFieldsRaw(data []byte) map[string]string {
	fields := make(map[string]string)
	n := len(data)
	if n < 2 || data[0] != '{' {
		return fields
	}

	i := 1
	for i < n {
		// Skip whitespace / commas.
		for i < n && (data[i] == ' ' || data[i] == ',' || data[i] == '\n' || data[i] == '\r' || data[i] == '\t') {
			i++
		}
		if i >= n || data[i] == '}' {
			break
		}
		if data[i] != '"' {
			i++
			continue
		}

		// Key.
		i++
		keyStart := i
		for i < n && data[i] != '"' {
			if data[i] == '\\' {
				i++
			}
			i++
		}
		if i >= n {
			break
		}
		key := string(data[keyStart:i])
		i++ // skip closing "

		// Skip colon.
		for i < n && (data[i] == ' ' || data[i] == ':') {
			i++
		}
		if i >= n {
			break
		}

		// Value — capture raw.
		valueStart := i
		if data[i] == '"' {
			// String value.
			i++
			for i < n && data[i] != '"' {
				if data[i] == '\\' {
					i++
				}
				i++
			}
			if i < n {
				i++ // skip closing "
			}
		} else if data[i] == '{' || data[i] == '[' {
			// Object or array — find matching close.
			depth := 1
			opener := data[i]
			closer := byte('}')
			if opener == '[' {
				closer = ']'
			}
			i++
			for i < n && depth > 0 {
				if data[i] == opener {
					depth++
				} else if data[i] == closer {
					depth--
				} else if data[i] == '"' {
					i++
					for i < n && data[i] != '"' {
						if data[i] == '\\' {
							i++
						}
						i++
					}
				}
				i++
			}
		} else {
			// Number, boolean, null.
			for i < n && data[i] != ',' && data[i] != '}' {
				i++
			}
		}

		value := string(data[valueStart:i])
		fields[key] = value
	}
	return fields
}

// parseJSONFields is the backward-compatible name used by other files.
func parseJSONFields(data []byte) map[string]string {
	return parseJSONFieldsRaw(data)
}

// buildFlatJSON constructs a JSON object from key-value pairs.
func buildFlatJSON(fields map[string]string) []byte {
	if len(fields) == 0 {
		return []byte("{}")
	}

	buf := make([]byte, 0, 256)
	buf = append(buf, '{')
	first := true
	for k, v := range fields {
		if !first {
			buf = append(buf, ',')
		}
		buf = append(buf, '"')
		buf = append(buf, k...)
		buf = append(buf, '"', ':')
		buf = append(buf, v...)
		first = false
	}
	buf = append(buf, '}')
	return buf
}
