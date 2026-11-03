package main

import (
	"testing"
	"time"
)

func TestCalibrationHeadroomBounds(t *testing.T) {
	for _, tc := range []struct{ n, w uint64 }{{1, 2}, {4, 5}, {5, 7}, {100000000, 125000000}, {800000000, 1000000000}, {999999999, 1000000000}, {1000000000, 1000000000}, {^uint64(0), 1000000000}} {
		if g := calibratedIterations(tc.n, 1); g != tc.w || calibratedIterations(1, tc.n) != tc.w {
			t.Fatalf("%d: %d !=%d", tc.n, g, tc.w)
		}
	}
}

func TestCalibrationHeadroomPreservesVetoAndRegression(t *testing.T) {
	for _, tc := range []struct {
		name         string
		speed, ratio float64
		spike, alloc bool
		want         string
	}{{"stable AA", .98, 1, false, false, "PASS"}, {"faster than headroom", .79, 1, false, false, "INCONCLUSIVE"}, {"sustained regression", .98, 1.2, false, false, "FAIL"}, {"minority regression", .98, 1, true, false, "INCONCLUSIVE"}, {"allocation regression", .98, 1, false, true, "FAIL"}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			runner := func(q benchmarkRequest) (benchmarkRun, error) {
				calls++
				n := q.Iterations
				d := time.Second
				ratio := 1.
				if q.Pair < 0 {
					n = 10000000
				} else {
					if q.Side == "candidate" {
						ratio = tc.ratio
						if tc.spike && q.Pair == 2 {
							ratio = 1.2
						}
					}
					d = time.Duration(float64(n) * 100 * tc.speed * ratio)
				}
				r := testBenchmarkRun(q.Name, n, d)
				r.Measurements[q.Name][0].NSOp = float64(d) / float64(n)
				if q.Side == "candidate" && q.Pair >= 0 && tc.alloc {
					r.Measurements[q.Name][0].AllocsOp = 1
					r.Measurements[q.Name][0].MemAllocs = n
				}
				return r, nil
			}
			r, _, e := collectPairs(runner, testBenchmarkBaseline(), 10, 6, time.Second)
			if e != nil || len(r) != 1 || r[0].Verdict != tc.want || calls != 14 || len(r[0].Pairs) != 6 {
				t.Fatalf("%+v %v calls%d", r, e, calls)
			}
			for _, p := range r[0].Pairs {
				if p.Reference.N != 12500000 || p.Candidate.N != 12500000 {
					t.Fatal("unequal counts")
				}
			}
		})
	}
}
