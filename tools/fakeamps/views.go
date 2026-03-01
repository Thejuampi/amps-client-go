package main

import (
	"log"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// Views — virtual topics computed from source topics.
//
// Real AMPS supports "views" which are virtual topics that aggregate,
// filter, and transform data from one or more source topics. When data
// is published to a source topic, the view is recomputed and subscribers
// to the view receive updated results.
//
// Supported view types:
//   - Passthrough: mirrors source topic with optional filter
//   - Aggregation: computes aggregates (count, sum, avg, min, max)
//     grouped by a key field
//   - Join: combines records from multiple source topics
//
// Configuration: views are defined via the -view flag:
//   -view "view_name:source_topic:filter_expr"
//   -view "summary:orders:/status='active':count,sum(amount):region"
//   -view "joined:orders,customers:join=inner,left_key=order_id,right_key=customer_id"
// ---------------------------------------------------------------------------

type joinType int

const (
	joinPassthrough joinType = iota
	joinInner
	joinLeft
	joinRight
)

type viewDef struct {
	name         string   // view topic name
	sources      []string // source topic patterns
	filter       string   // content filter on source data
	projection   []string // fields to include (empty = all)
	aggregates   []aggregateDef
	groupByField string

	// Join configuration
	joinType     joinType // inner, left, right
	joinKeyLeft  string   // field from left source to join on
	joinKeyRight string   // field from right source to join on
}

type aggregateDef struct {
	function string // count, sum, avg, min, max
	field    string // field to aggregate
}

var (
	viewsMu sync.RWMutex
	views   = make(map[string]*viewDef)   // view topic → definition
	viewSrc = make(map[string][]*viewDef) // source topic → views that depend on it
)

func registerView(v *viewDef) {
	viewsMu.Lock()
	views[v.name] = v
	for _, src := range v.sources {
		viewSrc[src] = append(viewSrc[src], v)
	}
	viewsMu.Unlock()
	log.Printf("fakeamps: view registered: %s ← %s (filter=%q groupBy=%q)",
		v.name, strings.Join(v.sources, ","), v.filter, v.groupByField)
}

func getView(topic string) *viewDef {
	viewsMu.RLock()
	v := views[topic]
	viewsMu.RUnlock()
	return v
}

// getViewsForSource returns all views that depend on the given source topic.
func getViewsForSource(sourceTopic string) []*viewDef {
	viewsMu.RLock()
	defer viewsMu.RUnlock()

	var result []*viewDef
	for pattern, vs := range viewSrc {
		if topicMatches(sourceTopic, pattern) {
			result = append(result, vs...)
		}
	}
	return result
}

// recomputeView recomputes a view's data from its source SOW records
// and delivers updated results to view subscribers.
func recomputeView(v *viewDef) {
	if sow == nil {
		return
	}

	// Gather all source records.
	var allRecords []sowRecord
	for _, src := range v.sources {
		result := sow.query(src, v.filter, -1, "")
		allRecords = append(allRecords, result.records...)
	}

	if len(v.sources) >= 2 && v.joinType != joinPassthrough {
		// Join view: combine records from multiple sources.
		deliverJoinView(v)
		return
	}

	if len(v.aggregates) > 0 && v.groupByField != "" {
		// Compute aggregations grouped by field.
		deliverAggregateView(v, allRecords)
	} else {
		// Passthrough view: deliver filtered source records.
		deliverPassthroughView(v, allRecords)
	}
}

func deliverPassthroughView(v *viewDef, records []sowRecord) {
	buf := getWriteBuf()
	defer putWriteBuf(buf)

	mt := getTopicMessageType(v.name)

	forEachMatchingSubscriber(v.name, nil, func(sub *subscription) {
		for _, r := range records {
			payload := r.payload
			if len(v.projection) > 0 {
				payload = projectFields(r.payload, v.projection)
			}
			frame := buildPublishDelivery(buf, v.name, sub.subID,
				payload, r.bookmark, r.timestamp, r.sowKey, mt, false)
			sub.writer.send(frame)
		}
	})
}

func deliverJoinView(v *viewDef) {
	if len(v.sources) < 2 || v.joinKeyLeft == "" || v.joinKeyRight == "" {
		return
	}

	// Query each source.
	results := make([][]sowRecord, len(v.sources))
	for i, src := range v.sources {
		result := sow.query(src, "", -1, "")
		results[i] = result.records
	}

	// Build lookup map from right source.
	rightRecords := results[len(results)-1]
	rightLookup := make(map[string][]sowRecord)
	for _, r := range rightRecords {
		key := extractJSONStringField(r.payload, v.joinKeyRight)
		rightLookup[key] = append(rightLookup[key], r)
	}

	// Perform join.
	var joined []sowRecord
	leftRecords := results[0]

	for _, left := range leftRecords {
		leftKey := extractJSONStringField(left.payload, v.joinKeyLeft)
		matches := rightLookup[leftKey]

		if len(matches) == 0 {
			// No match found.
			if v.joinType == joinLeft {
				// Left join: include left record with null right.
				joined = append(joined, left)
			}
			continue
		}

		// Found matches - join them.
		for _, right := range matches {
			// Merge left and right payloads.
			merged := mergeJoinRecords(left.payload, right.payload, v.projection)
			joined = append(joined, sowRecord{
				topic:     v.name,
				sowKey:    left.sowKey + "|" + right.sowKey,
				payload:   merged,
				bookmark:  left.bookmark,
				timestamp: left.timestamp,
				seqNum:    left.seqNum,
			})
		}
	}

	// For right join, include right records with no left match.
	if v.joinType == joinRight {
		leftKeys := make(map[string]bool)
		for _, left := range leftRecords {
			key := extractJSONStringField(left.payload, v.joinKeyLeft)
			leftKeys[key] = true
		}
		for _, right := range rightRecords {
			rightKey := extractJSONStringField(right.payload, v.joinKeyRight)
			if !leftKeys[rightKey] {
				merged := mergeJoinRecords(nil, right.payload, v.projection)
				joined = append(joined, sowRecord{
					topic:     v.name,
					sowKey:    right.sowKey,
					payload:   merged,
					bookmark:  right.bookmark,
					timestamp: right.timestamp,
					seqNum:    right.seqNum,
				})
			}
		}
	}

	// Deliver joined results.
	buf := getWriteBuf()
	defer putWriteBuf(buf)

	mt := getTopicMessageType(v.name)

	forEachMatchingSubscriber(v.name, nil, func(sub *subscription) {
		for _, r := range joined {
			seq := globalBookmarkSeq.Add(1)
			bm := makeBookmark(seq)
			frame := buildPublishDelivery(buf, v.name, sub.subID,
				r.payload, bm, r.timestamp, r.sowKey, mt, false)
			sub.writer.send(frame)
		}
	})
}

func mergeJoinRecords(left, right []byte, projection []string) []byte {
	leftFields := parseJSONFields(left)
	rightFields := parseJSONFields(right)

	merged := make(map[string]string)
	for k, v := range leftFields {
		merged["left_"+k] = v
	}
	for k, v := range rightFields {
		merged["right_"+k] = v
	}

	// Apply projection if specified.
	if len(projection) > 0 {
		projected := make(map[string]string)
		for _, p := range projection {
			if v, ok := merged[p]; ok {
				projected[p] = v
			}
		}
		return buildFlatJSON(projected)
	}

	return buildFlatJSON(merged)
}

func deliverAggregateView(v *viewDef, records []sowRecord) {
	// Group records by the groupBy field.
	groups := make(map[string][]sowRecord)
	for _, r := range records {
		key := extractJSONStringField(r.payload, v.groupByField)
		groups[key] = append(groups[key], r)
	}

	buf := getWriteBuf()
	defer putWriteBuf(buf)

	mt := getTopicMessageType(v.name)

	forEachMatchingSubscriber(v.name, nil, func(sub *subscription) {
		for groupKey, groupRecords := range groups {
			payload := computeAggregates(v.aggregates, v.groupByField, groupKey, groupRecords)
			seq := globalBookmarkSeq.Add(1)
			bm := makeBookmark(seq)
			ts := makeTimestamp()
			frame := buildPublishDelivery(buf, v.name, sub.subID,
				payload, bm, ts, groupKey, mt, false)
			sub.writer.send(frame)
		}
	})
}

func computeAggregates(aggs []aggregateDef, groupByField, groupKey string, records []sowRecord) []byte {
	var b []byte
	b = append(b, '{')
	b = append(b, '"')
	b = append(b, groupByField...)
	b = append(b, "\":\""...)
	b = append(b, groupKey...)
	b = append(b, '"')

	for _, agg := range aggs {
		b = append(b, ',')
		b = append(b, '"')
		b = append(b, agg.function...)
		b = append(b, '_')
		b = append(b, agg.field...)
		b = append(b, '"', ':')

		switch agg.function {
		case "count":
			b = appendInt(b, len(records))
		case "sum":
			sum := 0.0
			for _, r := range records {
				sum += parseFloat(extractJSONStringField(r.payload, agg.field))
			}
			b = appendFloat(b, sum)
		case "avg":
			sum := 0.0
			for _, r := range records {
				sum += parseFloat(extractJSONStringField(r.payload, agg.field))
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
				v := parseFloat(extractJSONStringField(r.payload, agg.field))
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
				v := parseFloat(extractJSONStringField(r.payload, agg.field))
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

// projectFields returns a JSON object containing only the specified fields.
func projectFields(payload []byte, fields []string) []byte {
	fieldSet := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		fieldSet[f] = struct{}{}
	}

	allFields := parseJSONFields(payload)
	projected := make(map[string]string, len(fields))
	for k, v := range allFields {
		if _, ok := fieldSet[k]; ok {
			projected[k] = v
		}
	}
	return buildFlatJSON(projected)
}

// ---------------------------------------------------------------------------
// View definition parsing from command-line flag.
//
// Format: "viewName:sourceTopic:filter:aggregates:groupBy"
//   - aggregates: comma-separated list like "count,sum(amount),avg(price)"
//   - groupBy: field name to group by
//
// Join format: "viewName:source1,source2:join=inner:left_key=id,right_key=customer_id"
//   - join: inner, left, right
//   - left_key: field from first source to join on
//   - right_key: field from second source to join on
//
// Example: "order_summary:orders:/status='active':count,sum(amount):region"
// Example: "joined_view:orders,customers:join=inner:left_key=customer_id,right_key=id"
// ---------------------------------------------------------------------------

func parseViewDef(spec string) *viewDef {
	// Check for join specification (contains '=' for key-value pairs).
	if strings.Contains(spec, "join=") || strings.Contains(spec, "left_key=") || strings.Contains(spec, "right_key=") {
		return parseJoinViewDef(spec)
	}

	parts := strings.SplitN(spec, ":", 5)
	if len(parts) < 2 {
		return nil
	}

	v := &viewDef{
		name:    parts[0],
		sources: []string{parts[1]},
	}

	if len(parts) >= 3 && parts[2] != "" {
		v.filter = parts[2]
	}
	if len(parts) >= 4 && parts[3] != "" {
		for _, aggStr := range strings.Split(parts[3], ",") {
			agg := parseAggregateDef(aggStr)
			if agg.function != "" {
				v.aggregates = append(v.aggregates, agg)
			}
		}
	}
	if len(parts) >= 5 && parts[4] != "" {
		v.groupByField = parts[4]
	}

	return v
}

func parseJoinViewDef(spec string) *viewDef {
	// Format: name:sources:joinopts
	// sources is comma-separated list
	parts := strings.SplitN(spec, ":", 3)
	if len(parts) < 2 {
		return nil
	}

	v := &viewDef{
		name:    parts[0],
		sources: strings.Split(parts[1], ","),
	}

	if len(parts) >= 3 {
		opts := parts[2]
		// Parse join options: join=inner:left_key=xxx:right_key=yyy
		for _, opt := range strings.Split(opts, ":") {
			kv := strings.SplitN(opt, "=", 2)
			if len(kv) != 2 {
				continue
			}
			switch kv[0] {
			case "join":
				switch kv[1] {
				case "inner":
					v.joinType = joinInner
				case "left":
					v.joinType = joinLeft
				case "right":
					v.joinType = joinRight
				}
			case "left_key":
				v.joinKeyLeft = kv[1]
			case "right_key":
				v.joinKeyRight = kv[1]
			}
		}
	}

	log.Printf("fakeamps: join view registered: %s ← %s (type=%d left_key=%s right_key=%s)",
		v.name, strings.Join(v.sources, ","), v.joinType, v.joinKeyLeft, v.joinKeyRight)

	return v
}

func parseAggregateDef(s string) aggregateDef {
	s = strings.TrimSpace(s)
	// "count" (no field needed)
	if s == "count" {
		return aggregateDef{function: "count", field: "*"}
	}
	// "sum(field)", "avg(field)", "min(field)", "max(field)"
	if idx := strings.IndexByte(s, '('); idx > 0 {
		fn := s[:idx]
		field := strings.TrimRight(s[idx+1:], ")")
		return aggregateDef{function: fn, field: field}
	}
	return aggregateDef{}
}
