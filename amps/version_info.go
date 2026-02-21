package amps

import (
	"fmt"
	"strconv"
	"strings"
)

// VersionInfo stores AMPS version components.
type VersionInfo struct {
	Major       uint
	Minor       uint
	Maintenance uint
	Hotfix      uint
}

// Number converts version components into sortable numeric representation.
func (version VersionInfo) Number() uint64 {
	return uint64(version.Major*1000000 + version.Minor*10000 + version.Maintenance*100 + version.Hotfix)
}

// String returns dotted version format.
func (version VersionInfo) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", version.Major, version.Minor, version.Maintenance, version.Hotfix)
}

// ParseVersionInfo parses a dotted AMPS version string.
func ParseVersionInfo(version string) VersionInfo {
	parts := strings.Split(version, ".")
	values := []uint{0, 0, 0, 0}
	for index := 0; index < len(parts) && index < 4; index++ {
		value, err := strconv.ParseUint(parts[index], 10, 32)
		if err != nil {
			break
		}
		values[index] = uint(value)
	}
	return VersionInfo{
		Major:       values[0],
		Minor:       values[1],
		Maintenance: values[2],
		Hotfix:      values[3],
	}
}
