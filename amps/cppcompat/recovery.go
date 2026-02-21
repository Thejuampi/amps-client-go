package cppcompat

import "sync"

// RecoveryPoint provides bookmark recovery values.
type RecoveryPoint interface {
	Next() string
	Reset()
}

// FixedRecoveryPoint always returns the same bookmark.
type FixedRecoveryPoint struct {
	bookmark string
}

// NewFixedRecoveryPoint creates a fixed recovery point.
func NewFixedRecoveryPoint(bookmark string) *FixedRecoveryPoint {
	return &FixedRecoveryPoint{bookmark: bookmark}
}

// Next returns fixed bookmark value.
func (point *FixedRecoveryPoint) Next() string {
	if point == nil {
		return ""
	}
	return point.bookmark
}

// Reset resets fixed recovery point state.
func (point *FixedRecoveryPoint) Reset() {}

// DynamicRecoveryPoint computes bookmarks using callback.
type DynamicRecoveryPoint struct {
	producer func() string
}

// NewDynamicRecoveryPoint creates a dynamic recovery point.
func NewDynamicRecoveryPoint(producer func() string) *DynamicRecoveryPoint {
	return &DynamicRecoveryPoint{producer: producer}
}

// Next returns next dynamic bookmark.
func (point *DynamicRecoveryPoint) Next() string {
	if point == nil || point.producer == nil {
		return ""
	}
	return point.producer()
}

// Reset resets dynamic point state.
func (point *DynamicRecoveryPoint) Reset() {}

// RecoveryPointAdapter adapts multiple recovery points into ordered sequence.
type RecoveryPointAdapter struct {
	lock   sync.Mutex
	points []RecoveryPoint
	index  int
}

// NewRecoveryPointAdapter creates a recovery point adapter.
func NewRecoveryPointAdapter(points ...RecoveryPoint) *RecoveryPointAdapter {
	adapter := &RecoveryPointAdapter{points: []RecoveryPoint{}, index: 0}
	for _, point := range points {
		if point != nil {
			adapter.points = append(adapter.points, point)
		}
	}
	return adapter
}

// Add appends a recovery point.
func (adapter *RecoveryPointAdapter) Add(point RecoveryPoint) {
	if adapter == nil || point == nil {
		return
	}
	adapter.lock.Lock()
	adapter.points = append(adapter.points, point)
	adapter.lock.Unlock()
}

// Next returns next bookmark from configured points.
func (adapter *RecoveryPointAdapter) Next() string {
	if adapter == nil {
		return ""
	}
	adapter.lock.Lock()
	defer adapter.lock.Unlock()
	if len(adapter.points) == 0 {
		return ""
	}
	for i := 0; i < len(adapter.points); i++ {
		idx := (adapter.index + i) % len(adapter.points)
		bookmark := adapter.points[idx].Next()
		if bookmark != "" {
			adapter.index = idx
			return bookmark
		}
	}
	return ""
}

// Reset resets all underlying points.
func (adapter *RecoveryPointAdapter) Reset() {
	if adapter == nil {
		return
	}
	adapter.lock.Lock()
	for _, point := range adapter.points {
		point.Reset()
	}
	adapter.index = 0
	adapter.lock.Unlock()
}

// ConflatingRecoveryPointAdapter returns latest non-empty point value.
type ConflatingRecoveryPointAdapter struct {
	adapter *RecoveryPointAdapter
}

// NewConflatingRecoveryPointAdapter creates a conflating adapter.
func NewConflatingRecoveryPointAdapter(points ...RecoveryPoint) *ConflatingRecoveryPointAdapter {
	return &ConflatingRecoveryPointAdapter{adapter: NewRecoveryPointAdapter(points...)}
}

// Next returns latest bookmark from underlying adapter.
func (adapter *ConflatingRecoveryPointAdapter) Next() string {
	if adapter == nil || adapter.adapter == nil {
		return ""
	}
	latest := ""
	for {
		next := adapter.adapter.Next()
		if next == "" {
			break
		}
		latest = next
	}
	return latest
}

// Reset resets adapter state.
func (adapter *ConflatingRecoveryPointAdapter) Reset() {
	if adapter == nil || adapter.adapter == nil {
		return
	}
	adapter.adapter.Reset()
}

// SOWRecoveryPointAdapter aliases recovery adapter for SOW workflows.
type SOWRecoveryPointAdapter struct {
	*RecoveryPointAdapter
}

// NewSOWRecoveryPointAdapter creates a SOW recovery adapter.
func NewSOWRecoveryPointAdapter(points ...RecoveryPoint) *SOWRecoveryPointAdapter {
	return &SOWRecoveryPointAdapter{RecoveryPointAdapter: NewRecoveryPointAdapter(points...)}
}
