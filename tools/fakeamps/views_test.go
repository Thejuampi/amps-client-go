package main

import (
	"strings"
	"testing"
)

func resetViewsForTest() {
	viewsMu.Lock()
	views = make(map[string]*viewDef)
	viewSrc = make(map[string][]*viewDef)
	viewsMu.Unlock()
}

func TestParseViewDef(t *testing.T) {
	cases := []struct {
		spec     string
		expected *viewDef
	}{
		{
			"myview:mytopic",
			&viewDef{
				name:    "myview",
				sources: []string{"mytopic"},
			},
		},
		{
			"myview:mytopic:/filter",
			&viewDef{
				name:    "myview",
				sources: []string{"mytopic"},
				filter:  "/filter",
			},
		},
		{
			"myview:mytopic:/filter:count,sum(amount)",
			&viewDef{
				name:    "myview",
				sources: []string{"mytopic"},
				filter:  "/filter",
				aggregates: []aggregateDef{
					{function: "count", field: "*"},
					{function: "sum", field: "amount"},
				},
			},
		},
		{
			"myview:mytopic:/filter:count,sum(amount):region",
			&viewDef{
				name:    "myview",
				sources: []string{"mytopic"},
				filter:  "/filter",
				aggregates: []aggregateDef{
					{function: "count", field: "*"},
					{function: "sum", field: "amount"},
				},
				groupByField: "region",
			},
		},
	}

	for _, tc := range cases {
		result := parseViewDef(tc.spec)
		if result == nil {
			t.Errorf("parseViewDef(%q) returned nil", tc.spec)
			continue
		}
		if result.name != tc.expected.name {
			t.Errorf("parseViewDef(%q).name = %q, want %q", tc.spec, result.name, tc.expected.name)
		}
		if len(result.sources) != len(tc.expected.sources) {
			t.Errorf("parseViewDef(%q).sources = %v, want %v", tc.spec, result.sources, tc.expected.sources)
		}
	}
}

func TestParseViewDefInvalid(t *testing.T) {
	cases := []struct {
		spec string
	}{
		{""},
		{"noview"},
	}

	for _, tc := range cases {
		result := parseViewDef(tc.spec)
		// Empty or invalid specs should return nil
		if result != nil && result.name == "" {
			t.Logf("parseViewDef(%q) returned empty view", tc.spec)
		}
	}
}

func TestParseJoinViewDef(t *testing.T) {
	spec := "myview:orders,customers:join=inner:left_key=customer_id:right_key=id"
	result := parseViewDef(spec)

	if result == nil {
		t.Fatal("expected view")
	}

	if result.name != "myview" {
		t.Errorf("name = %q, want %q", result.name, "myview")
	}

	if len(result.sources) != 2 {
		t.Errorf("sources count = %d, want 2", len(result.sources))
	}

	if result.joinType != joinInner {
		t.Errorf("joinType = %d, want %d", result.joinType, joinInner)
	}

	if result.joinKeyLeft != "customer_id" {
		t.Errorf("joinKeyLeft = %q, want %q", result.joinKeyLeft, "customer_id")
	}

	if result.joinKeyRight != "id" {
		t.Errorf("joinKeyRight = %q, want %q", result.joinKeyRight, "id")
	}
}

func TestParseAggregateDef(t *testing.T) {
	cases := []struct {
		input    string
		expected aggregateDef
	}{
		{"count", aggregateDef{function: "count", field: "*"}},
		{"sum(amount)", aggregateDef{function: "sum", field: "amount"}},
		{"avg(price)", aggregateDef{function: "avg", field: "price"}},
		{"min(qty)", aggregateDef{function: "min", field: "qty"}},
		{"max(value)", aggregateDef{function: "max", field: "value"}},
		{"invalid", aggregateDef{}},
	}

	for _, tc := range cases {
		result := parseAggregateDef(tc.input)
		if result.function != tc.expected.function {
			t.Errorf("parseAggregateDef(%q).function = %q, want %q", tc.input, result.function, tc.expected.function)
		}
	}
}

func TestViewRegistryHelpers(t *testing.T) {
	resetViewsForTest()
	defer resetViewsForTest()

	v := &viewDef{name: "v_orders", sources: []string{"orders"}}
	registerView(v)

	got := getView("v_orders")
	if got == nil || got.name != "v_orders" {
		t.Fatalf("expected registered view lookup")
	}

	deps := getViewsForSource("orders")
	if len(deps) == 0 {
		t.Fatalf("expected source dependencies")
	}
}

func TestMergeJoinRecordsAndComputeAggregates(t *testing.T) {
	merged := mergeJoinRecords([]byte(`{"id":"1","a":"x"}`), []byte(`{"id":"1","b":"y"}`), nil)
	text := string(merged)
	if !strings.Contains(text, `"left_id"`) || !strings.Contains(text, `"right_b"`) {
		t.Fatalf("unexpected merged join record: %s", text)
	}

	aggs := []aggregateDef{{function: "count", field: "*"}, {function: "sum", field: "amount"}}
	records := []sowRecord{{payload: []byte(`{"amount":10}`)}, {payload: []byte(`{"amount":5}`)}}
	out := string(computeAggregates(aggs, "region", "us", records))
	if !strings.Contains(out, `"region":"us"`) || !strings.Contains(out, `"count_*":2`) {
		t.Fatalf("unexpected aggregate output: %s", out)
	}
}

func TestRecomputeViewVariants(t *testing.T) {
	resetViewsForTest()
	defer resetViewsForTest()

	oldSow := sow
	sow = newSOWCache()
	defer func() { sow = oldSow }()

	// Populate source topics
	sow.upsert("orders", "o1", []byte(`{"id":"1","region":"us","amount":10}`), "bm1", "ts1", 1, 0)
	sow.upsert("orders", "o2", []byte(`{"id":"2","region":"eu","amount":5}`), "bm2", "ts2", 2, 0)
	sow.upsert("customers", "c1", []byte(`{"id":"1","name":"alice"}`), "bm3", "ts3", 3, 0)

	// Passthrough recompute
	passthrough := &viewDef{name: "v_passthrough", sources: []string{"orders"}}
	recomputeView(passthrough)

	// Aggregate recompute
	agg := &viewDef{
		name:         "v_agg",
		sources:      []string{"orders"},
		aggregates:   []aggregateDef{{function: "count", field: "*"}},
		groupByField: "region",
	}
	recomputeView(agg)

	// Join recompute
	join := &viewDef{
		name:         "v_join",
		sources:      []string{"orders", "customers"},
		joinType:     joinInner,
		joinKeyLeft:  "id",
		joinKeyRight: "id",
	}
	recomputeView(join)
}
