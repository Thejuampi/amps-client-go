package main

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
)

const requiredPairs = 6

type benchmarkPair struct {
	Reference benchmarkResult `json:"reference"`
	Candidate benchmarkResult `json:"candidate"`
}

type benchmarkCalibration struct {
	Reference  *benchmarkResult `json:"reference,omitempty"`
	Candidate  *benchmarkResult `json:"candidate,omitempty"`
	Iterations uint64           `json:"iterations"`
}

type incompleteBenchmarkPair struct {
	Number    int              `json:"number"`
	Reference *benchmarkResult `json:"reference,omitempty"`
	Candidate *benchmarkResult `json:"candidate,omitempty"`
}

type pairedResult struct {
	Name                 string                   `json:"name"`
	Verdict              string                   `json:"verdict"`
	NormalizedNS         float64                  `json:"normalized_ns_op"`
	MinimumNS            float64                  `json:"minimum_normalized_ns_op"`
	MaximumNS            float64                  `json:"maximum_normalized_ns_op"`
	LimitNS              float64                  `json:"limit_ns_op"`
	MaxAllocs            float64                  `json:"maximum_allocs_op"`
	MaxExactAllocs       float64                  `json:"maximum_exact_allocs_op"`
	FractionalRegression bool                     `json:"consistent_fractional_allocation_regression"`
	LimitAllocs          float64                  `json:"limit_allocs_op"`
	Pairs                []benchmarkPair          `json:"pairs"`
	Calibration          benchmarkCalibration     `json:"calibration"`
	IncompletePair       *incompleteBenchmarkPair `json:"incomplete_pair,omitempty"`
	MeasurementIssues    []string                 `json:"measurement_issues,omitempty"`
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validMetric(result benchmarkResult) bool {
	return finite(result.NSOp) && result.NSOp > 0 && finite(result.AllocsOp) && result.AllocsOp >= 0 && finite(result.BytesOp) && result.BytesOp >= 0
}

func reportedAllocations(result benchmarkResult) float64 {
	if result.N > 0 {
		return float64(result.MemAllocs / result.N)
	}
	return math.Floor(result.AllocsOp)
}

func exactAllocationRate(result benchmarkResult) *big.Rat {
	if result.N > 0 {
		return new(big.Rat).SetFrac(new(big.Int).SetUint64(result.MemAllocs), new(big.Int).SetUint64(result.N))
	}
	return new(big.Rat).SetFloat64(result.AllocsOp)
}

func allocationBudgetMultiplier(budget float64) (*big.Rat, error) {
	var decimal = strconv.FormatFloat(budget, 'g', -1, 64)
	var percentage, ok = new(big.Rat).SetString(decimal)
	if !ok {
		return nil, fmt.Errorf("invalid allocation budget %q", decimal)
	}
	percentage.Quo(percentage, big.NewRat(100, 1))
	return percentage.Add(percentage, big.NewRat(1, 1)), nil
}

func comparePairs(name string, baseline benchmarkBaseline, timeBudget, allocBudget float64, pairs []benchmarkPair) (pairedResult, error) {
	var result = pairedResult{Name: name, Verdict: "INCONCLUSIVE", Pairs: pairs}
	if len(pairs) != requiredPairs {
		return result, fmt.Errorf("%s: need exactly %d pairs, got %d", name, requiredPairs, len(pairs))
	}
	if !validMetric(benchmarkResult{NSOp: baseline.NSOp, AllocsOp: baseline.AllocsOp}) || !finite(timeBudget) || timeBudget < 0 || !finite(allocBudget) || allocBudget < 0 {
		return result, fmt.Errorf("%s: invalid baseline or regression budget", name)
	}
	result.LimitNS = baseline.NSOp*(1+timeBudget/100) + nsComparisonSlack
	result.LimitAllocs = baseline.AllocsOp * (1 + allocBudget/100)
	if !finite(result.LimitNS) || !finite(result.LimitAllocs) {
		return result, fmt.Errorf("%s: regression limit overflows", name)
	}
	var multiplier, err = allocationBudgetMultiplier(allocBudget)
	if err != nil {
		return result, fmt.Errorf("%s: %w", name, err)
	}
	var normalized = make([]float64, 0, len(pairs))
	var fractionalRegression = true
	result.MinimumNS = math.Inf(1)
	for index, pair := range pairs {
		if !validMetric(pair.Reference) || !validMetric(pair.Candidate) {
			return result, fmt.Errorf("%s pair %d: invalid observation", name, index+1)
		}
		// The original budget remains in baseline units. A common hardware
		// multiplier cancels in candidate/reference; the reference never advances
		// to the preceding release, so successive regressions cannot accumulate.
		var ns = pair.Candidate.NSOp * (baseline.NSOp / pair.Reference.NSOp)
		if !finite(ns) || ns <= 0 {
			return result, fmt.Errorf("%s pair %d: normalization overflows or underflows", name, index+1)
		}
		normalized = append(normalized, ns)
		result.MinimumNS = min(result.MinimumNS, ns)
		result.MaximumNS = max(result.MaximumNS, ns)
		result.MaxAllocs = max(result.MaxAllocs, reportedAllocations(pair.Candidate))
		result.MaxExactAllocs = max(result.MaxExactAllocs, pair.Candidate.AllocsOp)
		var referenceLimit = new(big.Rat).Mul(exactAllocationRate(pair.Reference), multiplier)
		if exactAllocationRate(pair.Candidate).Cmp(referenceLimit) <= 0 {
			fractionalRegression = false
		}
	}
	result.FractionalRegression = fractionalRegression
	result.NormalizedNS = medianFloat64(normalized)
	switch {
	case result.MaxAllocs > result.LimitAllocs, result.FractionalRegression, result.MinimumNS > result.LimitNS:
		result.Verdict = "FAIL"
	case result.MaximumNS <= result.LimitNS:
		result.Verdict = "PASS"
	}
	return result, nil
}

func regressionLimits(baseline baselineFile, expected benchmarkBaseline, fallback float64) (float64, float64) {
	var timeBudget, allocBudget = fallback, fallback
	if expected.Group != "" {
		var group = baseline.Groups[expected.Group]
		if group.MaxRegressionPct > 0 {
			timeBudget = group.MaxRegressionPct
		}
		if group.MaxAllocsRegressionPct > 0 {
			allocBudget = group.MaxAllocsRegressionPct
		}
	}
	return timeBudget, allocBudget
}
