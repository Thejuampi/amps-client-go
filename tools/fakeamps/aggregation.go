package main

import (
	"fmt"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// Aggregation & Projection for SOW queries.
//
// Real AMPS supports SQL-like aggregation on SOW queries:
//   - Aggregate functions: COUNT(*), SUM(field), AVG(field), MIN(field), MAX(field)
//   - GROUP BY: group results by a field value
//   - Projection: return only specific fields
//   - HAVING: filter on aggregate results (not implemented here)
//
// These are specified via the options header:
//   options="projection=[field1,field2]"
//   options="projection=[SUM(amount) AS total,COUNT(*) AS cnt],groupby=region"
//
// This implementation supports basic aggregation via the options string.
// ---------------------------------------------------------------------------

// aggQuery represents a parsed aggregation query.
type aggQuery struct {
	projections []projectionField
	groupBy     string
	hasAgg      bool
	having      string // HAVING filter applied to aggregate results
}

type projectionField struct {
	isAggregate bool
	function    string // count, sum, avg, min, max
	field       string // source field (or * for count)
	alias       string // output field name
}

// parseAggQuery parses aggregation/projection from options string.
func parseAggQuery(options string) *aggQuery {
	if options == "" {
		return nil
	}

	q := &aggQuery{}

	// Parse projection=[field1,field2,SUM(amount) AS total]
	if idx := strings.Index(options, "projection=["); idx >= 0 {
		start := idx + len("projection=[")
		end := strings.IndexByte(options[start:], ']')
		if end < 0 {
			return nil
		}
		projStr := options[start : start+end]
		for _, p := range splitProjectionFields(projStr) {
			pf := parseProjectionField(p)
			if pf.isAggregate {
				q.hasAgg = true
			}
			q.projections = append(q.projections, pf)
		}
	}

	// Parse groupby=field
	if idx := strings.Index(options, "groupby="); idx >= 0 {
		start := idx + len("groupby=")
		end := start
		for end < len(options) && options[end] != ',' && options[end] != ']' && options[end] != ' ' {
			end++
		}
		q.groupBy = options[start:end]
	}

	// Parse having=expression (HAVING clause for post-aggregation filtering).
	if idx := strings.Index(options, "having="); idx >= 0 {
		start := idx + len("having=")
		end := start
		// Having expression may be enclosed in brackets for complex expressions.
		if start < len(options) && options[start] == '[' {
			start++
			close := strings.IndexByte(options[start:], ']')
			if close >= 0 {
				q.having = options[start : start+close]
			}
		} else {
			for end < len(options) && options[end] != ',' && options[end] != ' ' {
				end++
			}
			q.having = options[start:end]
		}
	}

	if len(q.projections) == 0 && q.groupBy == "" {
		return nil
	}

	return q
}

func splitProjectionFields(s string) []string {
	// Split by comma, but respect parentheses.
	var fields []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				fields = append(fields, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(s) {
		fields = append(fields, strings.TrimSpace(s[start:]))
	}
	return fields
}

func parseProjectionField(s string) projectionField {
	s = strings.TrimSpace(s)

	// Check for "AS alias" suffix.
	alias := ""
	if idx := strings.Index(strings.ToUpper(s), " AS "); idx > 0 {
		alias = strings.TrimSpace(s[idx+4:])
		s = strings.TrimSpace(s[:idx])
	}

	// Check for aggregate function: SUM(field), COUNT(*), etc.
	for _, fn := range []string{"COUNT", "SUM", "AVG", "MIN", "MAX",
		"count", "sum", "avg", "min", "max"} {
		if strings.HasPrefix(strings.ToUpper(s), strings.ToUpper(fn)+"(") {
			field := s[len(fn)+1:]
			field = strings.TrimRight(field, ")")
			if alias == "" {
				alias = strings.ToLower(fn) + "_" + field
			}
			return projectionField{
				isAggregate: true,
				function:    strings.ToLower(fn),
				field:       field,
				alias:       alias,
			}
		}
	}

	// Simple field projection.
	if alias == "" {
		alias = s
	}
	return projectionField{field: s, alias: alias}
}

// executeAggQuery runs an aggregation query over SOW records.
func executeAggQuery(q *aggQuery, records []sowRecord) [][]byte {
	if q == nil || !q.hasAgg {
		// No aggregation — just project fields.
		var results [][]byte
		for _, r := range records {
			results = append(results, projectFieldsFromQuery(r.payload, q))
		}
		return results
	}

	if q.groupBy == "" {
		// Single group: aggregate all records.
		result := computeAggregateResult(q.projections, "", records)
		// Apply HAVING filter.
		if q.having != "" && !evaluateFilter(q.having, result) {
			return nil
		}
		return [][]byte{result}
	}

	// Group by field value.
	groups := make(map[string][]sowRecord)
	var groupOrder []string
	for _, r := range records {
		key := extractJSONStringField(r.payload, q.groupBy)
		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], r)
	}

	var results [][]byte
	for _, key := range groupOrder {
		result := computeAggregateResult(q.projections, key, groups[key])
		// Apply HAVING filter to each group's aggregate result.
		if q.having != "" && !evaluateFilter(q.having, result) {
			continue // skip groups that don't meet HAVING criteria
		}
		results = append(results, result)
	}
	return results
}

func computeAggregateResult(projections []projectionField, groupKey string, records []sowRecord) []byte {
	var b []byte
	b = append(b, '{')
	first := true

	for _, p := range projections {
		if !first {
			b = append(b, ',')
		}
		first = false

		b = append(b, '"')
		b = append(b, p.alias...)
		b = append(b, '"', ':')

		if !p.isAggregate {
			// Non-aggregate field — use first record's value.
			if len(records) > 0 {
				v := extractJSONStringField(records[0].payload, p.field)
				b = append(b, '"')
				b = append(b, v...)
				b = append(b, '"')
			} else {
				b = append(b, "null"...)
			}
			continue
		}

		switch p.function {
		case "count":
			b = appendInt(b, len(records))
		case "sum":
			sum := 0.0
			for _, r := range records {
				sum += parseFloat(extractJSONStringField(r.payload, p.field))
			}
			b = appendFloat(b, sum)
		case "avg":
			sum := 0.0
			for _, r := range records {
				sum += parseFloat(extractJSONStringField(r.payload, p.field))
			}
			if len(records) > 0 {
				b = appendFloat(b, sum/float64(len(records)))
			} else {
				b = append(b, '0')
			}
		case "min":
			min := 0.0
			first := true
			for _, r := range records {
				v := parseFloat(extractJSONStringField(r.payload, p.field))
				if first || v < min {
					min = v
					first = false
				}
			}
			b = appendFloat(b, min)
		case "max":
			max := 0.0
			first := true
			for _, r := range records {
				v := parseFloat(extractJSONStringField(r.payload, p.field))
				if first || v > max {
					max = v
					first = false
				}
			}
			b = appendFloat(b, max)
		}
	}

	b = append(b, '}')
	return b
}

func projectFieldsFromQuery(payload []byte, q *aggQuery) []byte {
	if q == nil || len(q.projections) == 0 {
		return payload
	}
	fields := make([]string, 0, len(q.projections))
	for _, p := range q.projections {
		if !p.isAggregate {
			fields = append(fields, p.field)
		}
	}
	if len(fields) == 0 {
		return payload
	}
	return projectFields(payload, fields)
}

// ---------------------------------------------------------------------------
// Numeric helpers
// ---------------------------------------------------------------------------

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func appendInt(b []byte, v int) []byte {
	return strconv.AppendInt(b, int64(v), 10)
}

func appendFloat(b []byte, v float64) []byte {
	return append(b, []byte(fmt.Sprintf("%.6f", v))...)
}
