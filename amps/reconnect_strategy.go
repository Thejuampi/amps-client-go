package amps

import (
	"math"
	"sync"
	"time"
)

// FixedDelayStrategy stores reconnect delay parameters.
type FixedDelayStrategy struct {
	Delay time.Duration
}

// NewFixedDelayStrategy returns a new FixedDelayStrategy.
func NewFixedDelayStrategy(delay time.Duration) *FixedDelayStrategy {
	if delay < 0 {
		delay = 0
	}
	return &FixedDelayStrategy{Delay: delay}
}

// GetConnectWaitDuration returns the current connect wait duration value.
func (strategy *FixedDelayStrategy) GetConnectWaitDuration(uri string) (time.Duration, error) {
	if strategy == nil {
		return 0, nil
	}
	return strategy.Delay, nil
}

// Reset executes the exported reset operation.
func (strategy *FixedDelayStrategy) Reset() {
	if strategy == nil {
		return
	}
}

// ExponentialDelayStrategy stores reconnect delay parameters.
type ExponentialDelayStrategy struct {
	lock      sync.Mutex
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Factor    float64
	attempts  map[string]uint32
}

// NewExponentialDelayStrategy returns a new ExponentialDelayStrategy.
func NewExponentialDelayStrategy(baseDelay time.Duration, maxDelay time.Duration, factor float64) *ExponentialDelayStrategy {
	if baseDelay < 0 {
		baseDelay = 0
	}
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}
	if factor < 1 {
		factor = 2
	}
	return &ExponentialDelayStrategy{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		Factor:    factor,
		attempts:  make(map[string]uint32),
	}
}

// GetConnectWaitDuration returns the current connect wait duration value.
func (strategy *ExponentialDelayStrategy) GetConnectWaitDuration(uri string) (time.Duration, error) {
	if strategy == nil {
		return 0, nil
	}

	strategy.lock.Lock()
	defer strategy.lock.Unlock()

	if uri == "" {
		uri = "_default"
	}

	attempt := strategy.attempts[uri]
	strategy.attempts[uri] = attempt + 1

	delay := strategy.BaseDelay
	if attempt > 0 && delay > 0 {
		delayFloat := float64(delay) * math.Pow(strategy.Factor, float64(attempt))
		if delayFloat > float64(strategy.MaxDelay) {
			delayFloat = float64(strategy.MaxDelay)
		}
		delay = time.Duration(delayFloat)
	}
	if delay > strategy.MaxDelay {
		delay = strategy.MaxDelay
	}
	if delay < 0 {
		delay = 0
	}
	return delay, nil
}

// Reset executes the exported reset operation.
func (strategy *ExponentialDelayStrategy) Reset() {
	if strategy == nil {
		return
	}
	strategy.lock.Lock()
	strategy.attempts = make(map[string]uint32)
	strategy.lock.Unlock()
}
