package main

// ---------------------------------------------------------------------------
// Content filtering — evaluate AMPS filter expressions against JSON payloads.
//
// Real AMPS supports a rich expression language with XPath-like paths,
// logical operators, regex, LIKE, IN, IS NULL, NOT, etc. This implementation
// covers the patterns used in integration tests and perf benchmarks:
//
//   - "1=1" (always true)
//   - "/field = value" or "/field == value"
//   - "/field > value" or "/field < value" or "/field >= value" or "/field <= value"
//   - "/field != value"
//   - "/field LIKE 'pattern'"
//   - "/field IN ('a','b','c')"
//   - "/field IS NULL"
//   - "/field IS NOT NULL"
//   - "/field BETWEEN a AND b"
//   - "NOT expr"
//   - Parenthesized groups: "(expr) AND (expr)"
//   - Nested paths: "/parent/child = value"
//   - Regex match: "/field ~ 'pattern'"
//
// All comparisons are string-based for simplicity. The filter is
// evaluated against the raw JSON payload bytes.
// ---------------------------------------------------------------------------

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// Regex cache for filter patterns.
// ---------------------------------------------------------------------------

var (
	filterRegexMu    sync.RWMutex
	filterRegexCache = make(map[string]*regexp.Regexp)
)

func getFilterRegex(pattern string) (*regexp.Regexp, bool) {
	filterRegexMu.RLock()
	re, ok := filterRegexCache[pattern]
	filterRegexMu.RUnlock()
	if ok {
		return re, true
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, false
	}
	filterRegexMu.Lock()
	filterRegexCache[pattern] = compiled
	filterRegexMu.Unlock()
	return compiled, true
}

// evaluateFilter returns true if the payload matches the filter expression.
// An empty filter always matches. Unknown expressions match (permissive).
func evaluateFilter(filter string, payload []byte) bool {
	if filter == "" {
		return true
	}

	filter = strings.TrimSpace(filter)

	// Always-true filter.
	if filter == "1=1" || filter == "true" {
		return true
	}

	// Always-false filter.
	if filter == "0=1" || filter == "false" {
		return false
	}

	// Check for string functions first (before logical ops).
	if strings.HasPrefix(filter, "BEGINS WITH ") || strings.HasPrefix(filter, "begins with ") {
		return evaluateBeginsWith(filter, payload)
	}
	if strings.HasPrefix(filter, "ENDS WITH ") || strings.HasPrefix(filter, "ends with ") {
		return evaluateEndsWith(filter, payload)
	}
	if strings.HasPrefix(filter, "CONTAINS ") || strings.HasPrefix(filter, "contains ") {
		return evaluateContains(filter, payload)
	}
	if strings.HasPrefix(filter, "UPPER(") || strings.HasPrefix(filter, "upper(") {
		return evaluateStringFunction(filter, payload, "upper")
	}
	if strings.HasPrefix(filter, "LOWER(") || strings.HasPrefix(filter, "lower(") {
		return evaluateStringFunction(filter, payload, "lower")
	}
	if strings.HasPrefix(filter, "LEN(") || strings.HasPrefix(filter, "len(") {
		return evaluateLenFunction(filter, payload)
	}
	if strings.HasPrefix(filter, "INSTR(") || strings.HasPrefix(filter, "instr(") {
		return evaluateInstrFunction(filter, payload)
	}
	if strings.HasPrefix(filter, "SUBSTR(") || strings.HasPrefix(filter, "substr(") {
		return evaluateSubstrFunction(filter, payload)
	}

	// Check for math expression: /field + 1 > 10, etc.
	if isMathExpression(filter) {
		return evaluateMathExpression(filter, payload)
	}

	// Strip outer parentheses if the whole expression is wrapped.
	filter = stripOuterParens(filter)

	// NOT prefix.
	if strings.HasPrefix(filter, "NOT ") || strings.HasPrefix(filter, "not ") {
		return !evaluateFilter(filter[4:], payload)
	}

	// AND / OR with proper parenthesis-aware splitting.
	if parts, found := splitLogicalOp(filter, " AND "); found {
		return evaluateFilter(parts[0], payload) && evaluateFilter(parts[1], payload)
	}
	if parts, found := splitLogicalOp(filter, " OR "); found {
		return evaluateFilter(parts[0], payload) || evaluateFilter(parts[1], payload)
	}

	// Try to parse as a comparison: /path op value
	if len(filter) > 1 && filter[0] == '/' {
		return evaluateComparison(filter, payload)
	}

	// Unknown filter — be permissive.
	return true
}

// stripOuterParens removes a single layer of balanced outer parentheses.
func stripOuterParens(s string) string {
	for len(s) > 2 && s[0] == '(' && s[len(s)-1] == ')' {
		// Verify the closing paren matches the opening one.
		depth := 0
		matched := true
		for i, ch := range s {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			if depth == 0 && i < len(s)-1 {
				matched = false
				break
			}
		}
		if matched {
			s = strings.TrimSpace(s[1 : len(s)-1])
		} else {
			break
		}
	}
	return s
}

// splitLogicalOp splits an expression at a top-level logical operator,
// respecting parenthesized sub-expressions and quoted strings.
func splitLogicalOp(expr, op string) ([2]string, bool) {
	depth := 0
	inQuote := byte(0)
	opLen := len(op)
	upperExpr := strings.ToUpper(expr)
	upperOp := strings.ToUpper(op)

	for i := 0; i < len(expr)-opLen+1; i++ {
		ch := expr[i]
		if inQuote != 0 {
			if ch == inQuote {
				inQuote = 0
			} else if ch == '\\' {
				i++ // skip escaped char
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			depth--
			continue
		}
		if depth == 0 && upperExpr[i:i+opLen] == upperOp {
			left := strings.TrimSpace(expr[:i])
			right := strings.TrimSpace(expr[i+opLen:])
			if left != "" && right != "" {
				return [2]string{left, right}, true
			}
		}
	}
	return [2]string{}, false
}

func evaluateComparison(filter string, payload []byte) bool {
	// Parse: /path op value
	// Find operator — check multi-char ops first.
	ops := []string{"IS NOT NULL", "IS NULL", "NOT IN ", "BETWEEN ", "!=", ">=", "<=", "==", ">", "<", "=", " LIKE ", " IN ", " ~ "}

	for _, op := range ops {
		var idx int
		if op == "IS NOT NULL" || op == "IS NULL" {
			idx = strings.Index(strings.ToUpper(filter), " "+op)
			if idx <= 0 {
				continue
			}
			idx++ // adjust for leading space in search
		} else if op == "NOT IN " || op == "BETWEEN " {
			idx = strings.Index(strings.ToUpper(filter), " "+strings.ToUpper(op))
			if idx <= 0 {
				continue
			}
			idx++ // adjust for leading space
		} else {
			idx = strings.Index(filter, op)
			if idx <= 0 {
				continue
			}
		}

		path := strings.TrimSpace(filter[:idx])
		value := strings.TrimSpace(filter[idx+len(op):])

		// Strip leading '/' from path and support nested paths.
		if len(path) > 0 && path[0] == '/' {
			path = path[1:]
		}

		// Extract field value from payload (supports nested via / separator).
		fieldValue := extractNestedField(payload, path)

		switch op {
		case "=", "==":
			return fieldValue == stripQuotes(value)
		case "!=":
			return fieldValue != stripQuotes(value)
		case ">":
			return compareNumeric(fieldValue, stripQuotes(value)) > 0
		case "<":
			return compareNumeric(fieldValue, stripQuotes(value)) < 0
		case ">=":
			return compareNumeric(fieldValue, stripQuotes(value)) >= 0
		case "<=":
			return compareNumeric(fieldValue, stripQuotes(value)) <= 0
		case " LIKE ":
			return matchLike(fieldValue, stripQuotes(value))
		case " ~ ":
			return matchRegex(fieldValue, stripQuotes(value))
		case " IN ":
			return matchIn(fieldValue, value)
		case "NOT IN ":
			return !matchIn(fieldValue, value)
		case "IS NULL":
			return fieldValue == ""
		case "IS NOT NULL":
			return fieldValue != ""
		case "BETWEEN ":
			return matchBetween(fieldValue, value)
		}
	}

	return true // unknown expression — permissive
}

// extractNestedField supports XPath-like paths: "parent/child" extracts
// from nested JSON objects.
func extractNestedField(payload []byte, path string) string {
	parts := strings.Split(path, "/")
	if len(parts) == 1 {
		return extractJSONStringField(payload, path)
	}

	// Walk nested objects.
	current := payload
	for i, part := range parts {
		if i == len(parts)-1 {
			return extractJSONStringField(current, part)
		}
		// Extract the raw value of the intermediate field.
		nested := extractJSONRawValue(current, part)
		if nested == nil {
			return ""
		}
		current = nested
	}
	return ""
}

// extractJSONRawValue extracts the raw bytes of a JSON field value,
// including nested objects/arrays.
func extractJSONRawValue(data []byte, field string) []byte {
	target := `"` + field + `":`
	n := len(target)
	for i := 0; i+n < len(data); i++ {
		if string(data[i:i+n]) == target {
			j := i + n
			for j < len(data) && (data[j] == ' ' || data[j] == '\t') {
				j++
			}
			if j >= len(data) {
				return nil
			}
			start := j
			if data[j] == '{' || data[j] == '[' {
				opener := data[j]
				closer := byte('}')
				if opener == '[' {
					closer = ']'
				}
				depth := 1
				j++
				for j < len(data) && depth > 0 {
					if data[j] == opener {
						depth++
					} else if data[j] == closer {
						depth--
					} else if data[j] == '"' {
						j++
						for j < len(data) && data[j] != '"' {
							if data[j] == '\\' {
								j++
							}
							j++
						}
					}
					j++
				}
				return data[start:j]
			}
			return nil // not a nested object
		}
	}
	return nil
}

func stripQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func compareNumeric(a, b string) int {
	fa, errA := strconv.ParseFloat(a, 64)
	fb, errB := strconv.ParseFloat(b, 64)
	if errA == nil && errB == nil {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}
	// Fall back to string comparison.
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func matchLike(value, pattern string) bool {
	// Simple LIKE: % matches any substring, _ matches single char.
	if pattern == "%" {
		return true
	}
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		return strings.Contains(value, pattern[1:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "%") {
		return strings.HasSuffix(value, pattern[1:])
	}
	if strings.HasSuffix(pattern, "%") {
		return strings.HasPrefix(value, pattern[:len(pattern)-1])
	}
	// Check for _ wildcard.
	if strings.Contains(pattern, "_") {
		return matchLikePattern(value, pattern)
	}
	return value == pattern
}

// matchLikePattern handles % and _ wildcards via recursive matching.
func matchLikePattern(value, pattern string) bool {
	v, p := 0, 0
	starV, starP := -1, -1
	for v < len(value) {
		if p < len(pattern) && (pattern[p] == '_' || pattern[p] == value[v]) {
			v++
			p++
		} else if p < len(pattern) && pattern[p] == '%' {
			starV = v
			starP = p
			p++
		} else if starP >= 0 {
			starV++
			v = starV
			p = starP + 1
		} else {
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '%' {
		p++
	}
	return p == len(pattern)
}

// matchRegex matches a field value against a regex pattern.
func matchRegex(value, pattern string) bool {
	re, ok := getFilterRegex(pattern)
	if !ok {
		return false
	}
	return re.MatchString(value)
}

// matchIn checks if a field value is in a list: IN ('a','b','c') or IN (1,2,3).
func matchIn(fieldValue, listExpr string) bool {
	listExpr = strings.TrimSpace(listExpr)
	// Strip outer parens.
	if strings.HasPrefix(listExpr, "(") && strings.HasSuffix(listExpr, ")") {
		listExpr = listExpr[1 : len(listExpr)-1]
	}
	for _, item := range strings.Split(listExpr, ",") {
		item = strings.TrimSpace(item)
		item = stripQuotes(item)
		if fieldValue == item {
			return true
		}
	}
	return false
}

// matchBetween checks if a field value is between two values.
// Format: "low AND high"
func matchBetween(fieldValue, betweenExpr string) bool {
	betweenExpr = strings.TrimSpace(betweenExpr)
	parts := strings.SplitN(betweenExpr, " AND ", 2)
	if len(parts) != 2 {
		parts = strings.SplitN(betweenExpr, " and ", 2)
	}
	if len(parts) != 2 {
		return false // can't parse, fail closed
	}
	low := stripQuotes(strings.TrimSpace(parts[0]))
	high := stripQuotes(strings.TrimSpace(parts[1]))
	return compareNumeric(fieldValue, low) >= 0 && compareNumeric(fieldValue, high) <= 0
}

// ---------------------------------------------------------------------------
// String functions: BEGINS WITH, ENDS WITH, CONTAINS, UPPER, LOWER, LEN, INSTR, SUBSTR
// ---------------------------------------------------------------------------

func evaluateBeginsWith(filter string, payload []byte) bool {
	prefix := "BEGINS WITH "
	if !strings.HasPrefix(filter, prefix) {
		prefix = "begins with "
		if !strings.HasPrefix(filter, prefix) {
			return false
		}
	}
	rest := strings.TrimSpace(filter[len(prefix):])
	// Format: /field 'value' (no = operator)
	parts := strings.Fields(rest)
	if len(parts) < 2 {
		return false
	}
	fieldPath := parts[0]
	// Strip leading "/" from path
	if len(fieldPath) > 0 && fieldPath[0] == '/' {
		fieldPath = fieldPath[1:]
	}
	value := strings.Join(parts[1:], " ")
	value = stripQuotes(value)
	fieldValue := extractNestedField(payload, fieldPath)
	return strings.HasPrefix(fieldValue, value)
}

func evaluateEndsWith(filter string, payload []byte) bool {
	prefix := "ENDS WITH "
	if !strings.HasPrefix(filter, prefix) {
		prefix = "ends with "
		if !strings.HasPrefix(filter, prefix) {
			return false
		}
	}
	rest := strings.TrimSpace(filter[len(prefix):])
	// Format: /field 'value' (no = operator)
	parts := strings.Fields(rest)
	if len(parts) < 2 {
		return false
	}
	fieldPath := parts[0]
	// Strip leading "/" from path
	if len(fieldPath) > 0 && fieldPath[0] == '/' {
		fieldPath = fieldPath[1:]
	}
	value := strings.Join(parts[1:], " ")
	value = stripQuotes(value)
	fieldValue := extractNestedField(payload, fieldPath)
	return strings.HasSuffix(fieldValue, value)
}

func evaluateContains(filter string, payload []byte) bool {
	prefix := "CONTAINS "
	if !strings.HasPrefix(filter, prefix) {
		prefix = "contains "
		if !strings.HasPrefix(filter, prefix) {
			return false
		}
	}
	rest := strings.TrimSpace(filter[len(prefix):])
	// Format: /field 'value' (no = operator)
	parts := strings.Fields(rest)
	if len(parts) < 2 {
		return false
	}
	fieldPath := parts[0]
	// Strip leading "/" from path
	if len(fieldPath) > 0 && fieldPath[0] == '/' {
		fieldPath = fieldPath[1:]
	}
	value := strings.Join(parts[1:], " ")
	value = stripQuotes(value)
	fieldValue := extractNestedField(payload, fieldPath)
	return strings.Contains(fieldValue, value)
}

func evaluateStringFunction(filter string, payload []byte, fn string) bool {
	rest := filter
	if fn == "upper" {
		rest = strings.TrimPrefix(strings.TrimPrefix(filter, "UPPER("), "upper(")
	} else if fn == "lower" {
		rest = strings.TrimPrefix(strings.TrimPrefix(filter, "LOWER("), "lower(")
	}

	// Find the closing parenthesis
	closeIdx := strings.Index(rest, ")")
	if closeIdx >= 0 {
		rest = rest[:closeIdx]
	}

	// Now rest is like "/name='JOHN'" - split by =
	var fieldPath, compareValue string
	eqIdx := strings.Index(rest, "=")
	if eqIdx >= 0 {
		fieldPath = strings.TrimSpace(rest[:eqIdx])
		compareValue = strings.TrimSpace(rest[eqIdx+1:])
		compareValue = stripQuotes(compareValue)
	} else {
		// No = found, try space-separated
		parts := strings.Fields(rest)
		if len(parts) >= 1 {
			fieldPath = parts[0]
			if len(parts) >= 2 {
				compareValue = stripQuotes(strings.Join(parts[1:], " "))
			}
		}
	}

	// Strip leading "/" from path
	if len(fieldPath) > 0 && fieldPath[0] == '/' {
		fieldPath = fieldPath[1:]
	}

	fieldValue := extractNestedField(payload, fieldPath)
	if fn == "upper" {
		compareValue = strings.ToUpper(compareValue)
		fieldValue = strings.ToUpper(fieldValue)
	} else if fn == "lower" {
		compareValue = strings.ToLower(compareValue)
		fieldValue = strings.ToLower(fieldValue)
	}
	return fieldValue == compareValue
}

func evaluateLenFunction(filter string, payload []byte) bool {
	rest := filter
	rest = strings.TrimPrefix(strings.TrimPrefix(filter, "LEN("), "len(")

	// Find the closing parenthesis
	closeIdx := strings.Index(rest, ")")
	if closeIdx >= 0 {
		rest = rest[:closeIdx]
	}

	// Now rest is like "/name=5" - split by =
	var fieldPath, value string
	eqIdx := strings.Index(rest, "=")
	if eqIdx >= 0 {
		fieldPath = strings.TrimSpace(rest[:eqIdx])
		value = strings.TrimSpace(rest[eqIdx+1:])
	} else {
		parts := strings.Fields(rest)
		if len(parts) >= 1 {
			fieldPath = parts[0]
			if len(parts) >= 2 {
				value = strings.Join(parts[1:], " ")
			}
		}
	}

	// Strip leading "/" from path
	if len(fieldPath) > 0 && fieldPath[0] == '/' {
		fieldPath = fieldPath[1:]
	}

	fieldValue := extractNestedField(payload, fieldPath)
	lenValue := len(fieldValue)
	targetValue, err := strconv.Atoi(stripQuotes(value))
	if err != nil {
		return false
	}
	return lenValue == targetValue
}

func evaluateInstrFunction(filter string, payload []byte) bool {
	rest := strings.TrimPrefix(strings.TrimPrefix(filter, "INSTR("), "instr(")
	rest = strings.TrimSuffix(rest, ")")
	rest = strings.TrimSpace(rest)

	parts := strings.Split(rest, ",")
	if len(parts) != 2 {
		return true
	}
	fieldValue := extractNestedField(payload, strings.TrimSpace(parts[0]))
	substr := stripQuotes(strings.TrimSpace(parts[1]))
	idx := strings.Index(fieldValue, substr)
	if idx < 0 {
		return false
	}
	return idx >= 0
}

func evaluateSubstrFunction(filter string, payload []byte) bool {
	rest := strings.TrimPrefix(strings.TrimPrefix(filter, "SUBSTR("), "substr(")
	rest = strings.TrimSuffix(rest, ")")
	rest = strings.TrimSpace(rest)

	parts := strings.Split(rest, ",")
	if len(parts) != 3 {
		return true
	}
	fieldValue := extractNestedField(payload, strings.TrimSpace(parts[0]))
	start, err1 := strconv.Atoi(strings.TrimSpace(parts[1]))
	length, err2 := strconv.Atoi(strings.TrimSpace(parts[2]))
	if err1 != nil || err2 != nil {
		return true
	}
	if start < 0 || start > len(fieldValue) {
		return false
	}
	end := start + length
	if end > len(fieldValue) {
		end = len(fieldValue)
	}
	substr := fieldValue[start:end]
	compareValue := stripQuotes(strings.TrimSpace(parts[2]))
	return substr == compareValue
}

func splitCompExpr(expr string) []string {
	ops := []string{"=", "==", "!=", ">=", "<=", ">", "<"}
	for _, op := range ops {
		idx := strings.Index(expr, op)
		if idx > 0 {
			return []string{strings.TrimSpace(expr[:idx]), strings.TrimSpace(expr[idx+len(op):])}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Math expressions: +, -, *, /, %, NaN, INF handling
// ---------------------------------------------------------------------------

func isMathExpression(filter string) bool {
	mathOps := []string{"+", "-", "*", "/", "%"}
	upper := strings.ToUpper(filter)
	for _, op := range mathOps {
		if strings.Contains(upper, " "+op+" ") || strings.Contains(upper, op+" ") || strings.Contains(upper, " "+op) {
			return true
		}
	}
	return false
}

func evaluateMathExpression(filter string, payload []byte) bool {
	ops := []string{"<=", ">=", "!=", "==", ">", "<", "=", "+", "-", "*", "/", "%"}
	var op string
	var idx int

	for _, o := range ops {
		if len(o) > 1 {
			idx = strings.Index(filter, o)
			if idx > 0 {
				op = o
				break
			}
		}
	}
	if op == "" {
		for _, o := range []string{"+", "-", "*", "/", "%"} {
			idx = strings.Index(filter, o)
			if idx > 0 {
				op = o
				break
			}
		}
	}
	if idx <= 0 {
		return true
	}

	left := strings.TrimSpace(filter[:idx])
	right := strings.TrimSpace(filter[idx+len(op):])

	if isMathExpr(left) || isMathExpr(right) {
		leftVal := evaluateMathExprValue(left, payload)
		rightVal := evaluateMathExprValue(right, payload)
		return compareMathValues(leftVal, rightVal, op)
	}

	switch op {
	case "=", "==":
		return extractNestedField(payload, left) == stripQuotes(right)
	case "!=":
		return extractNestedField(payload, left) != stripQuotes(right)
	case ">", "<", ">=", "<=":
		leftVal := evaluateMathExprValue(left, payload)
		rightVal := evaluateMathExprValue(right, payload)
		return compareMathValues(leftVal, rightVal, op)
	default:
		return true
	}
}

func isMathExpr(s string) bool {
	mathOps := []string{"+", "-", "*", "/", "%", "(", ")"}
	upper := strings.ToUpper(s)
	for _, op := range mathOps {
		if strings.Contains(upper, op) {
			return true
		}
	}
	return false
}

func evaluateMathExprValue(expr string, payload []byte) float64 {
	expr = strings.TrimSpace(expr)

	if strings.HasPrefix(expr, "/") {
		fieldValue := extractNestedField(payload, expr[1:])
		if v, err := strconv.ParseFloat(fieldValue, 64); err == nil {
			return v
		}
	}

	if v, err := strconv.ParseFloat(expr, 64); err == nil {
		return v
	}

	if strings.ToUpper(expr) == "NAN" {
		return math.NaN()
	}
	if strings.ToUpper(expr) == "INF" || strings.ToUpper(expr) == "+INF" {
		return math.Inf(1)
	}
	if strings.ToUpper(expr) == "-INF" {
		return math.Inf(-1)
	}

	return 0
}

func compareMathValues(a, b float64, op string) bool {
	nanA := math.IsNaN(a)
	nanB := math.IsNaN(b)

	switch op {
	case "=":
		if nanA || nanB {
			return nanA == nanB
		}
		return a == b
	case "==":
		if nanA || nanB {
			return nanA == nanB
		}
		return a == b
	case "!=":
		if nanA || nanB {
			return nanA != nanB
		}
		return a != b
	case ">":
		if nanA || nanB {
			return false
		}
		return a > b
	case "<":
		if nanA || nanB {
			return false
		}
		return a < b
	case ">=":
		if nanA || nanB {
			return false
		}
		return a >= b
	case "<=":
		if nanA || nanB {
			return false
		}
		return a <= b
	}
	return true
}
