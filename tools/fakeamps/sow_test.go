package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSOWCacheUpsert(t *testing.T) {
	cache := newSOWCache()

	inserted, updated, previous := cache.upsert("orders", "order-1", []byte(`{"id":1,"amount":100}`), "bm1", "ts1", 1, 0)
	if !inserted || updated || previous != nil {
		t.Fatalf("unexpected insert state inserted=%v updated=%v previous=%v", inserted, updated, previous)
	}

	inserted, updated, previous = cache.upsert("orders", "order-1", []byte(`{"id":1,"amount":200}`), "bm2", "ts2", 2, 0)
	if inserted || !updated {
		t.Fatalf("unexpected update state inserted=%v updated=%v", inserted, updated)
	}
	if string(previous) != `{"id":1,"amount":100}` {
		t.Fatalf("unexpected previous payload: %s", string(previous))
	}
}

func TestSOWCacheDeltaUpsert(t *testing.T) {
	cache := newSOWCache()
	cache.upsert("orders", "order-1", []byte(`{"id":1,"amount":100,"status":"open"}`), "bm1", "ts1", 1, 0)

	inserted, updated, merged := cache.deltaUpsert("orders", "order-1", []byte(`{"amount":150}`), "bm2", "ts2", 2, 0)
	if inserted || !updated || len(merged) == 0 {
		t.Fatalf("unexpected delta result inserted=%v updated=%v merged=%s", inserted, updated, string(merged))
	}
}

func TestSOWCacheDeleteAndDeleteByKeys(t *testing.T) {
	cache := newSOWCache()
	cache.upsert("orders", "order-1", []byte(`{"id":1}`), "bm1", "ts1", 1, 0)
	cache.upsert("orders", "order-2", []byte(`{"id":2}`), "bm2", "ts2", 2, 0)

	if !cache.delete("orders", "order-1") {
		t.Fatalf("expected delete true")
	}

	deleted := cache.deleteByKeys("orders", "order-2, order-3")
	if deleted != 1 {
		t.Fatalf("expected one deleted by keys, got %d", deleted)
	}
}

func TestSOWCacheDeleteByFilter(t *testing.T) {
	cache := newSOWCache()
	cache.upsert("orders", "order-1", []byte(`{"id":1,"status":"active"}`), "bm1", "ts1", 1, 0)
	cache.upsert("orders", "order-2", []byte(`{"id":2,"status":"inactive"}`), "bm2", "ts2", 2, 0)

	deleted, keys := cache.deleteByFilter("orders", "/status = 'active'")
	if deleted != 1 || len(keys) != 1 || keys[0] != "order-1" {
		t.Fatalf("unexpected deleteByFilter result deleted=%d keys=%v", deleted, keys)
	}
}

func TestSOWCacheQueryVariants(t *testing.T) {
	cache := newSOWCache()
	cache.upsert("orders", "order-1", []byte(`{"id":1,"amount":100}`), "bm1", "ts1", 1, 0)
	cache.upsert("orders", "order-2", []byte(`{"id":2,"amount":200}`), "bm2", "ts2", 2, 0)
	cache.upsert("orders", "order-3", []byte(`{"id":3,"amount":50}`), "bm3", "ts3", 3, 0)

	all := cache.query("orders", "", -1, "")
	if all.totalCount != 3 || len(all.records) != 3 {
		t.Fatalf("unexpected all query result count=%d len=%d", all.totalCount, len(all.records))
	}

	filtered := cache.query("orders", "/amount > 100", -1, "")
	if filtered.totalCount != 1 {
		t.Fatalf("unexpected filtered count=%d", filtered.totalCount)
	}

	top2 := cache.query("orders", "", 2, "")
	if len(top2.records) != 2 {
		t.Fatalf("unexpected top2 len=%d", len(top2.records))
	}

	countOnly := cache.query("orders", "", 0, "")
	if countOnly.totalCount != 3 || len(countOnly.records) != 0 {
		t.Fatalf("unexpected count-only result count=%d len=%d", countOnly.totalCount, len(countOnly.records))
	}

	orderedAsc := cache.query("orders", "", -1, "amount")
	if len(orderedAsc.records) != 3 {
		t.Fatalf("unexpected ordered asc len=%d", len(orderedAsc.records))
	}

	orderedDesc := cache.query("orders", "", -1, "-amount")
	if len(orderedDesc.records) != 3 {
		t.Fatalf("unexpected ordered desc len=%d", len(orderedDesc.records))
	}
}

func TestSOWCacheExpirationAndGC(t *testing.T) {
	cache := newSOWCache()
	cache.upsert("orders", "expiring", []byte(`{"id":1}`), "bm1", "ts1", 1, 20*time.Millisecond)
	cache.upsert("orders", "live", []byte(`{"id":2}`), "bm2", "ts2", 2, 0)

	time.Sleep(35 * time.Millisecond)

	removed := cache.gcExpired()
	if removed < 1 {
		t.Fatalf("expected at least one expired record removed, got %d", removed)
	}

	result := cache.query("orders", "", -1, "")
	if result.totalCount != 1 {
		t.Fatalf("expected one live record after gc, got %d", result.totalCount)
	}
}

func TestSOWCacheCountAndTopics(t *testing.T) {
	cache := newSOWCache()
	if cache.count("orders") != 0 {
		t.Fatalf("expected empty count")
	}

	cache.upsert("orders", "order-1", []byte(`{"id":1}`), "bm1", "ts1", 1, 0)
	cache.upsert("customers", "cust-1", []byte(`{"id":1}`), "bm2", "ts2", 2, 0)

	if cache.count("orders") != 1 {
		t.Fatalf("expected count 1 for orders")
	}

	topics := cache.allTopics()
	if len(topics) != 2 {
		t.Fatalf("expected two topics, got %v", topics)
	}
}

func TestExtractSowKey(t *testing.T) {
	cases := []struct {
		payload string
		want    string
	}{
		{`{"_sow_key":"mykey"}`, "mykey"},
		{`{"id":123}`, "123"},
		{`{"key":"test"}`, "test"},
		{`{"ID":1}`, "1"},
		{`{"Id":1}`, "1"},
		{`{}`, ""},
	}

	for _, c := range cases {
		got := extractSowKey([]byte(c.payload))
		if got != c.want {
			t.Fatalf("extractSowKey(%s)=%s want %s", c.payload, got, c.want)
		}
	}
}

func TestDiskSOWPersistenceHelpers(t *testing.T) {
	dir := t.TempDir()

	saver := &sowCache{diskPath: dir}
	records := map[string]*sowRecord{
		"order-1": {
			topic:      "orders",
			sowKey:     "order-1",
			payload:    []byte(`{"id":1}`),
			bookmark:   "bm1",
			timestamp:  "20240101T000000000000000",
			seqNum:     1,
			lastAccess: time.Now(),
		},
	}

	saver.saveTopicToDisk("orders", records)

	path := filepath.Join(dir, "sow", "orders")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected persisted sow file at %s: %v", path, err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected persisted sow file to be non-empty")
	}
}
