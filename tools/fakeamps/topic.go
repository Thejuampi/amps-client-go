package main

import (
	"regexp"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// Topic matching — supports exact, wildcard, and full regex.
//
// Real AMPS supports:
//   - Exact: "orders" matches only "orders"
//   - Dot-hierarchy with ">" suffix: "orders.>" matches "orders.us", "orders.eu.west"
//   - Dot-hierarchy with ".." wildcard: "orders..product" matches "orders.us.product"
//   - Regex: "^orders\..*" (patterns starting with ^)
//   - Single ">" matches everything
// ---------------------------------------------------------------------------

// regexCache caches compiled regex patterns to avoid recompilation on every match.
var (
	regexCacheMu sync.RWMutex
	regexCache   = make(map[string]*regexp.Regexp)
)

func getCompiledRegex(pattern string) (*regexp.Regexp, bool) {
	regexCacheMu.RLock()
	re, ok := regexCache[pattern]
	regexCacheMu.RUnlock()
	if ok {
		return re, true
	}

	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, false
	}

	regexCacheMu.Lock()
	regexCache[pattern] = compiled
	regexCacheMu.Unlock()
	return compiled, true
}

// topicMatches returns true if a published topic matches a subscription pattern.
func topicMatches(publishTopic, subscriptionPattern string) bool {
	// Exact match (most common case — check first for speed).
	if publishTopic == subscriptionPattern {
		return true
	}

	// Single ">" matches everything.
	if subscriptionPattern == ">" {
		return true
	}

	// Regex pattern (starts with ^).
	if len(subscriptionPattern) > 0 && subscriptionPattern[0] == '^' {
		re, ok := getCompiledRegex(subscriptionPattern)
		if !ok {
			return false
		}
		return re.MatchString(publishTopic)
	}

	// Wildcard ">" matches any suffix in the dot hierarchy.
	// E.g. "orders.>" matches "orders.us", "orders.eu.west", but NOT "orders".
	if strings.HasSuffix(subscriptionPattern, ".>") {
		prefix := subscriptionPattern[:len(subscriptionPattern)-1] // "orders."
		return strings.HasPrefix(publishTopic, prefix)
	}

	// "*" wildcard — matches a single segment.
	// E.g. "orders.*.product" matches "orders.us.product"
	if strings.Contains(subscriptionPattern, "*") {
		return matchWildcard(publishTopic, subscriptionPattern)
	}

	// ".." double-dot wildcard: matches any single segment.
	// E.g. "orders..product" matches "orders.us.product", "orders.eu.product"
	if strings.Contains(subscriptionPattern, "..") {
		return matchDoubleDot(publishTopic, subscriptionPattern)
	}

	return false
}

func matchWildcard(topic, pattern string) bool {
	topicParts := strings.Split(topic, ".")
	patternParts := strings.Split(pattern, ".")

	if len(topicParts) != len(patternParts) {
		return false
	}

	for i := range patternParts {
		if patternParts[i] == "*" {
			continue // matches any single segment
		}
		if topicParts[i] != patternParts[i] {
			return false
		}
	}
	return true
}

func matchDoubleDot(topic, pattern string) bool {
	topicParts := strings.Split(topic, ".")
	patternParts := strings.Split(pattern, ".")

	ti, pi := 0, 0
	for ti < len(topicParts) && pi < len(patternParts) {
		pp := patternParts[pi]
		if pp == "" {
			// ".." was split into an empty segment — matches any single part.
			pi++
			ti++
			continue
		}
		if topicParts[ti] != pp {
			return false
		}
		ti++
		pi++
	}
	return ti == len(topicParts) && pi == len(patternParts)
}
