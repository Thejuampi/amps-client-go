package main

import (
	"strings"
	"testing"
)

func TestParseAggQuery(t *testing.T) {
	q := parseAggQuery("projection=[SUM(amount) AS total,COUNT(*) AS cnt],groupby=region,having=[/cnt > 0]")
	if q == nil {
		t.Fatalf("expected parsed query")
	}
	if !q.hasAgg {
		t.Fatalf("expected hasAgg=true")
	}
	if q.groupBy != "region" {
		t.Fatalf("expected groupBy=region got %q", q.groupBy)
	}
	if q.having != "/cnt > 0" {
		t.Fatalf("expected having parsed got %q", q.having)
	}
	if len(q.projections) != 2 {
		t.Fatalf("expected 2 projections got %d", len(q.projections))
	}
}

func TestSplitProjectionFields(t *testing.T) {
	parts := splitProjectionFields("id,SUM(amount),COUNT(*),AVG(price) AS p")
	if len(parts) != 4 {
		t.Fatalf("expected 4 parts got %d: %v", len(parts), parts)
	}
}

func TestParseProjectionField(t *testing.T) {
	count := parseProjectionField("COUNT(*) AS cnt")
	if !count.isAggregate || count.function != "count" || count.alias != "cnt" {
		t.Fatalf("unexpected count projection: %+v", count)
	}

	simple := parseProjectionField("region")
	if simple.isAggregate || simple.field != "region" || simple.alias != "region" {
		t.Fatalf("unexpected simple projection: %+v", simple)
	}
}

func TestExecuteAggQueryGrouped(t *testing.T) {
	q := parseAggQuery("projection=[COUNT(*) AS cnt,SUM(amount) AS total],groupby=region")
	records := []sowRecord{
		{payload: []byte(`{"region":"us","amount":10}`)},
		{payload: []byte(`{"region":"us","amount":15}`)},
		{payload: []byte(`{"region":"eu","amount":5}`)},
	}

	out := executeAggQuery(q, records)
	if len(out) != 2 {
		t.Fatalf("expected 2 grouped results got %d", len(out))
	}

	joined := string(out[0]) + string(out[1])
	if !strings.Contains(joined, `"cnt":2`) && !strings.Contains(joined, `"cnt":1`) {
		t.Fatalf("expected count fields in grouped output: %s", joined)
	}
}

func TestExecuteAggQueryNoAggProjection(t *testing.T) {
	q := parseAggQuery("projection=[id,region]")
	records := []sowRecord{{payload: []byte(`{"id":1,"region":"us","amount":10}`)}}
	out := executeAggQuery(q, records)
	if len(out) != 1 {
		t.Fatalf("expected single projected record got %d", len(out))
	}
	if !strings.Contains(string(out[0]), `"id"`) {
		t.Fatalf("expected projected id field: %s", string(out[0]))
	}
}

func TestComputeAggregateResult(t *testing.T) {
	projections := []projectionField{
		{isAggregate: true, function: "count", field: "*", alias: "cnt"},
		{isAggregate: true, function: "sum", field: "amount", alias: "total"},
		{isAggregate: true, function: "avg", field: "amount", alias: "avg_amount"},
		{isAggregate: true, function: "min", field: "amount", alias: "min_amount"},
		{isAggregate: true, function: "max", field: "amount", alias: "max_amount"},
	}
	records := []sowRecord{
		{payload: []byte(`{"amount":10}`)},
		{payload: []byte(`{"amount":20}`)},
	}

	out := string(computeAggregateResult(projections, "", records))
	if !strings.Contains(out, `"cnt":2`) {
		t.Fatalf("expected count field in output: %s", out)
	}
	if !strings.Contains(out, `"total":30.000000`) {
		t.Fatalf("expected sum field in output: %s", out)
	}
}

func TestProjectFieldsFromQuery(t *testing.T) {
	q := &aggQuery{projections: []projectionField{{field: "id"}, {field: "name"}}}
	payload := []byte(`{"id":"1","name":"alice","ignored":"x"}`)
	projected := projectFieldsFromQuery(payload, q)
	text := string(projected)
	if !strings.Contains(text, `"id"`) || !strings.Contains(text, `"name"`) {
		t.Fatalf("expected projected fields in output: %s", text)
	}
}

func TestParseFloatAndAppendHelpers(t *testing.T) {
	if parseFloat("12.5") != 12.5 {
		t.Fatalf("expected parseFloat 12.5")
	}

	b := appendInt(nil, 42)
	if string(b) != "42" {
		t.Fatalf("expected appendInt output 42 got %s", string(b))
	}

	b = appendFloat(nil, 3.5)
	if string(b) != "3.500000" {
		t.Fatalf("expected appendFloat fixed precision got %s", string(b))
	}
}
