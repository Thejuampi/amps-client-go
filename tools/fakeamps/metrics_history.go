package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	metricsHistoryMkdirAll  = os.MkdirAll
	metricsHistoryWriteFile = os.WriteFile
)

type metricSample struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

type metricsHistoryOptions struct {
	Retention time.Duration
}

type metricsHistoryStore struct {
	mu        sync.RWMutex
	writeMu   sync.Mutex
	retention time.Duration
	samples   map[string][]metricSample
}

func newMetricsHistoryStore(options metricsHistoryOptions) *metricsHistoryStore {
	var retention = options.Retention
	if retention <= 0 {
		retention = 15 * time.Minute
	}

	return &metricsHistoryStore{
		retention: retention,
		samples:   make(map[string][]metricSample),
	}
}

func (store *metricsHistoryStore) Add(metric string, timestamp time.Time, value float64) {
	if store == nil || metric == "" {
		return
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	var list = append(store.samples[metric], metricSample{
		Timestamp: timestamp.UTC(),
		Value:     value,
	})
	var cutoff = timestamp.Add(-store.retention)
	var trimmed []metricSample
	for _, sample := range list {
		if sample.Timestamp.Before(cutoff) {
			continue
		}
		trimmed = append(trimmed, sample)
	}
	store.samples[metric] = trimmed
}

func (store *metricsHistoryStore) Range(metric string, start time.Time, end time.Time) []metricSample {
	if store == nil || metric == "" {
		return nil
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	var out []metricSample
	for _, sample := range store.samples[metric] {
		if sample.Timestamp.Before(start) {
			continue
		}
		if sample.Timestamp.After(end) {
			continue
		}
		out = append(out, sample)
	}

	sort.Slice(out, func(left int, right int) bool {
		return out[left].Timestamp.Before(out[right].Timestamp)
	})
	return out
}

func (store *metricsHistoryStore) SnapshotAt(metric string, at time.Time) (metricSample, bool) {
	if store == nil || metric == "" {
		return metricSample{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	var found bool
	var current metricSample
	for _, sample := range store.samples[metric] {
		if sample.Timestamp.After(at) {
			continue
		}
		if !found || sample.Timestamp.After(current.Timestamp) {
			current = sample
			found = true
		}
	}
	return current, found
}

func (store *metricsHistoryStore) LoadFile(path string) error {
	if store == nil || path == "" {
		return nil
	}

	var payload, err = os.ReadFile(path)
	if err != nil {
		return err
	}

	var decoded map[string][]metricSample
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return err
	}

	store.mu.Lock()
	store.samples = decoded
	store.mu.Unlock()
	return nil
}

func (store *metricsHistoryStore) SaveFile(path string) error {
	if store == nil || path == "" {
		return nil
	}

	store.writeMu.Lock()
	defer store.writeMu.Unlock()

	store.mu.RLock()
	var payload, err = json.Marshal(store.samples)
	store.mu.RUnlock()
	if err != nil {
		return err
	}

	if err := metricsHistoryMkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return metricsHistoryWriteFile(path, payload, 0o600)
}
