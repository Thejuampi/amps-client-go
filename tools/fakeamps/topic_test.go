package main

import (
	"testing"
)

func TestTopicMatches(t *testing.T) {
	cases := []struct {
		pattern  string
		topic    string
		expected bool
	}{
		// Exact match
		{"orders", "orders", true},
		{"orders", "customers", false},

		// Dot-hierarchy wildcard
		{"orders.>", "orders.us", true},
		{"orders.>", "orders.eu.west", true},
		{"orders.>", "customers", false},
		{"orders.>", "orders", false},

		// Double-dot wildcard - implementation varies
		{"orders..product", "orders.product", false},
		{"orders..product", "orders.us.product", true},
		{"orders..product", "orders", false},

		// Catch-all
		{">", "anything", true},
		{">", "orders", true},
		{">", "customers", true},

		// Empty pattern
		{"", "orders", false},
		{"orders", "", false},
	}

	for _, tc := range cases {
		result := topicMatches(tc.topic, tc.pattern)
		if result != tc.expected {
			t.Errorf("topicMatches(%q, %q) = %v, want %v", tc.topic, tc.pattern, result, tc.expected)
		}
	}
}

func TestContainsToken(t *testing.T) {
	cases := []struct {
		s        string
		token    string
		expected bool
	}{
		{"processed", "processed", true},
		{"processed,completed", "processed", true},
		{"processed,completed", "completed", true},
		{"processed,completed", "stats", false},
		{"", "processed", false},
		{"processed", "", false},
	}

	for _, tc := range cases {
		result := containsToken(tc.s, tc.token)
		if result != tc.expected {
			t.Errorf("containsToken(%q, %q) = %v, want %v", tc.s, tc.token, result, tc.expected)
		}
	}
}

func TestFirstNonEmpty(t *testing.T) {
	cases := []struct {
		vals     []string
		expected string
	}{
		{[]string{"a", "b"}, "a"},
		{[]string{"", "b"}, "b"},
		{[]string{"", "", "c"}, "c"},
		{[]string{"", ""}, ""},
	}

	for _, tc := range cases {
		result := firstNonEmpty(tc.vals...)
		if result != tc.expected {
			t.Errorf("firstNonEmpty(%v) = %q, want %q", tc.vals, result, tc.expected)
		}
	}
}

func TestTopicMatchesWildcardAndRegex(t *testing.T) {
	if !topicMatches("orders.us.product", "orders.*.product") {
		t.Fatalf("expected wildcard * match")
	}
	if topicMatches("orders.us.eu.product", "orders.*.product") {
		t.Fatalf("did not expect wildcard match with extra segment")
	}

	if !topicMatches("orders.us", `^orders\..*`) {
		t.Fatalf("expected regex topic match")
	}
	if topicMatches("customers.us", `^orders\..*`) {
		t.Fatalf("did not expect regex topic match")
	}
}

func TestGetCompiledRegexCache(t *testing.T) {
	re, ok := getCompiledRegex(`^orders\..*`)
	if !ok || re == nil {
		t.Fatalf("expected compiled regex")
	}

	re2, ok := getCompiledRegex(`^orders\..*`)
	if !ok || re2 == nil {
		t.Fatalf("expected cached compiled regex")
	}
}
