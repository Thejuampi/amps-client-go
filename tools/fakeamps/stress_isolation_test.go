package main

import (
	"testing"

	"github.com/Thejuampi/amps-client-go/internal/ampsconfig"
)

func TestRuntimeFlagSnapshotRestoresEffectiveConfig(t *testing.T) {
	var original = effectiveConfig
	defer func() { effectiveConfig = original }()
	var before = &ampsconfig.ExpandedConfig{Runtime: ampsconfig.RuntimeConfig{Name: "before"}}
	effectiveConfig = before
	var restore = snapshotRuntimeFlags()
	effectiveConfig = &ampsconfig.ExpandedConfig{Runtime: ampsconfig.RuntimeConfig{Name: "after"}}
	restore()
	if effectiveConfig != before {
		t.Fatal("runtime fixture leaked the loaded config into the next test")
	}
}
