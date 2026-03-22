package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDecodeInboundFrameRejectsInvalidCompressedPayload(t *testing.T) {
	if _, err := decodeInboundFrame([]byte{'z', 'n', 'o', 't'}); err == nil {
		t.Fatalf("expected invalid compressed frame to return error")
	}
}

func TestParseAMPSHeaderRejectsTruncatedHeader(t *testing.T) {
	header, payload := parseAMPSHeader([]byte(`{"c":"publish"`))
	if header.c != "" {
		t.Fatalf("expected truncated header to be rejected, got %+v", header)
	}
	if payload != nil {
		t.Fatalf("expected truncated header payload to be nil")
	}
}

func TestIssueAuthChallengeNonceDoesNotAdvanceBookmarkSequence(t *testing.T) {
	original := globalBookmarkSeq.Load()
	globalBookmarkSeq.Store(42)
	t.Cleanup(func() {
		globalBookmarkSeq.Store(original)
	})

	nonce := issueAuthChallengeNonce()
	if nonce == "" {
		t.Fatalf("expected nonce to be populated")
	}
	if got := globalBookmarkSeq.Load(); got != 42 {
		t.Fatalf("globalBookmarkSeq = %d, want 42", got)
	}
}

func TestMergeJSONAcceptsLeadingWhitespace(t *testing.T) {
	merged := string(mergeJSON([]byte(" \n\t{\"a\":1}"), []byte(`{"b":2}`)))
	fields := parseJSONFields([]byte(merged))
	if fields["a"] != "1" || fields["b"] != "2" {
		t.Fatalf("unexpected merged fields: %v", fields)
	}
}

func TestParseStartupOptionsRespectsExplicitFalseSampleConfig(t *testing.T) {
	options, err := parseStartupOptions([]string{"--sample-config=false"})
	if err != nil {
		t.Fatalf("parseStartupOptions returned error: %v", err)
	}
	if options.Mode != startupModeRun {
		t.Fatalf("Mode = %q, want %q", options.Mode, startupModeRun)
	}
}

func TestHandleAdminJournalClampsSequenceRangeLowerBound(t *testing.T) {
	originalJournal := journal
	originalSeq := globalBookmarkSeq.Load()
	journal = &messageJournal{maxSize: 10, count: 5, head: 3}
	globalBookmarkSeq.Store(2)
	t.Cleanup(func() {
		journal = originalJournal
		globalBookmarkSeq.Store(originalSeq)
	})

	request := httptest.NewRequest("GET", "/admin/journal", nil)
	recorder := httptest.NewRecorder()
	handleAdminJournal(recorder, request)

	var payload struct {
		SeqRange []uint64 `json:"seq_range"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal returned error: %v", err)
	}
	if len(payload.SeqRange) != 2 {
		t.Fatalf("SeqRange length = %d, want 2", len(payload.SeqRange))
	}
	if payload.SeqRange[0] != 0 {
		t.Fatalf("SeqRange[0] = %d, want 0", payload.SeqRange[0])
	}
	if payload.SeqRange[1] != 2 {
		t.Fatalf("SeqRange[1] = %d, want 2", payload.SeqRange[1])
	}
}

func TestMergeJoinRecordsProjectionAcceptsUnprefixedFields(t *testing.T) {
	merged := string(mergeJoinRecords(
		[]byte(`{"id":"1","left_only":"x"}`),
		[]byte(`{"id":"1","right_only":"y"}`),
		[]string{"left_only", "right_only"},
	))
	fields := parseJSONFields([]byte(merged))
	if fields["left_left_only"] != `"x"` || fields["right_right_only"] != `"y"` {
		t.Fatalf("unexpected projected join fields: %v", fields)
	}
}

func TestNewConflationBufferWithKeyAssignsKeyBeforeStartingTimer(t *testing.T) {
	originalScheduler := scheduleConflationTimer
	scheduleConflationTimer = func(cb *conflationBuffer, interval time.Duration) *time.Timer {
		if cb.conflationKey != "sym" {
			t.Fatalf("conflationKey = %q, want %q", cb.conflationKey, "sym")
		}
		return time.NewTimer(time.Hour)
	}
	t.Cleanup(func() {
		scheduleConflationTimer = originalScheduler
	})

	cb := newConflationBufferWithKey(&subscription{}, time.Millisecond, "sym")
	if cb == nil {
		t.Fatalf("expected conflation buffer")
	}
	cb.stop()
}
