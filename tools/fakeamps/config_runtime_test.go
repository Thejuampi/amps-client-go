package main

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Thejuampi/amps-client-go/internal/ampsconfig"
)

func TestApplyRuntimeConfigUsesTransportAndExtensions(t *testing.T) {
	restore := snapshotRuntimeFlags()
	defer restore()

	var enabled = false
	var logConn = false
	var queueEnabled = false
	var runtimeConfig = ampsconfig.RuntimeConfig{
		Transports: []ampsconfig.TransportConfig{
			{
				Name:        "json-tcp",
				Type:        "tcp",
				InetAddr:    "127.0.0.1:19099",
				Protocol:    "amps",
				MessageType: "json",
			},
		},
		Admin: ampsconfig.AdminConfig{
			InetAddr: "127.0.0.1:18085",
		},
		Extensions: ampsconfig.ExtensionsConfig{
			FakeAMPS: ampsconfig.FakeAMPSExtension{
				Version:            "6.3.2.0",
				SOWEnabled:         &enabled,
				LogConnections:     &logConn,
				QueueEnabled:       &queueEnabled,
				JournalMax:         10_000,
				SOWGCInterval:      time.Minute,
				QueueLeaseInterval: 45 * time.Second,
				StatsInterval:      10 * time.Second,
				ReadBuffer:         32_000,
				WriteBuffer:        48_000,
				OutDepth:           512,
			},
		},
	}

	if err := applyRuntimeConfig(runtimeConfig, startupOptions{}); err != nil {
		t.Fatalf("applyRuntimeConfig returned error: %v", err)
	}

	if *flagAddr != "127.0.0.1:19099" {
		t.Fatalf("flagAddr = %q, want transport inet addr", *flagAddr)
	}
	if *flagAdminAddr != "127.0.0.1:18085" {
		t.Fatalf("flagAdminAddr = %q, want admin inet addr", *flagAdminAddr)
	}
	if *flagVersion != "6.3.2.0" {
		t.Fatalf("flagVersion = %q, want extension version", *flagVersion)
	}
	if *flagSOWEnabled {
		t.Fatalf("flagSOWEnabled = true, want false")
	}
	if *flagLogConn {
		t.Fatalf("flagLogConn = true, want false")
	}
	if *flagQueue {
		t.Fatalf("flagQueue = true, want false")
	}
	if *flagJournalMax != 10_000 {
		t.Fatalf("flagJournalMax = %d, want 10000", *flagJournalMax)
	}
	if *flagSOWGCIvl != time.Minute {
		t.Fatalf("flagSOWGCIvl = %v, want 1m", *flagSOWGCIvl)
	}
	if *flagLeaseIvl != 45*time.Second {
		t.Fatalf("flagLeaseIvl = %v, want 45s", *flagLeaseIvl)
	}
	if *flagStatsIvl != 10*time.Second {
		t.Fatalf("flagStatsIvl = %v, want 10s", *flagStatsIvl)
	}
	if *flagReadBuf != 32_000 {
		t.Fatalf("flagReadBuf = %d, want 32000", *flagReadBuf)
	}
	if *flagWriteBuf != 48_000 {
		t.Fatalf("flagWriteBuf = %d, want 48000", *flagWriteBuf)
	}
	if *flagOutDepth != 512 {
		t.Fatalf("flagOutDepth = %d, want 512", *flagOutDepth)
	}
}

func TestApplyRuntimeConfigPreservesExplicitFlags(t *testing.T) {
	restore := snapshotRuntimeFlags()
	defer restore()

	*flagAddr = "127.0.0.1:19100"
	*flagAdminAddr = "127.0.0.1:28085"
	*flagVersion = "6.3.9.9"

	var runtimeConfig = ampsconfig.RuntimeConfig{
		Transports: []ampsconfig.TransportConfig{
			{
				InetAddr: "127.0.0.1:19099",
			},
		},
		Admin: ampsconfig.AdminConfig{
			InetAddr: "127.0.0.1:18085",
		},
		Extensions: ampsconfig.ExtensionsConfig{
			FakeAMPS: ampsconfig.FakeAMPSExtension{
				Version:    "6.3.2.0",
				JournalMax: 10_000,
			},
		},
	}

	var options = startupOptions{
		userSet: map[string]bool{
			"addr":    true,
			"admin":   true,
			"version": true,
		},
	}
	if err := applyRuntimeConfig(runtimeConfig, options); err != nil {
		t.Fatalf("applyRuntimeConfig returned error: %v", err)
	}

	if *flagAddr != "127.0.0.1:19100" {
		t.Fatalf("flagAddr = %q, want explicit CLI value", *flagAddr)
	}
	if *flagAdminAddr != "127.0.0.1:28085" {
		t.Fatalf("flagAdminAddr = %q, want explicit CLI value", *flagAdminAddr)
	}
	if *flagVersion != "6.3.9.9" {
		t.Fatalf("flagVersion = %q, want explicit CLI value", *flagVersion)
	}
	if *flagJournalMax != 10_000 {
		t.Fatalf("flagJournalMax = %d, want config value for unset flag", *flagJournalMax)
	}
}

func TestApplyRuntimeConfigLeavesDefaultsWhenConfigValuesAreUnset(t *testing.T) {
	restore := snapshotRuntimeFlags()
	defer restore()

	*flagSOWGCIvl = 30 * time.Second
	*flagLeaseIvl = 5 * time.Second

	var runtimeConfig = ampsconfig.RuntimeConfig{
		Transports: []ampsconfig.TransportConfig{
			{
				InetAddr: "127.0.0.1:19099",
			},
		},
	}

	if err := applyRuntimeConfig(runtimeConfig, startupOptions{}); err != nil {
		t.Fatalf("applyRuntimeConfig returned error: %v", err)
	}

	if *flagSOWGCIvl != 30*time.Second {
		t.Fatalf("flagSOWGCIvl = %v, want existing default", *flagSOWGCIvl)
	}
	if *flagLeaseIvl != 5*time.Second {
		t.Fatalf("flagLeaseIvl = %v, want existing default", *flagLeaseIvl)
	}
}

func TestApplyRuntimeConfigConfiguresExtendedOptions(t *testing.T) {
	restore := snapshotRuntimeFlags()
	defer restore()

	var noDelay = false
	var echo = true
	var benchmark = true
	var challenge = true
	var runtimeConfig = ampsconfig.RuntimeConfig{
		Extensions: ampsconfig.ExtensionsConfig{
			FakeAMPS: ampsconfig.FakeAMPSExtension{
				NoDelay:            &noDelay,
				Latency:            50 * time.Millisecond,
				HasLatency:         true,
				Lease:              75 * time.Second,
				HasLease:           true,
				Echo:               &echo,
				BenchmarkStability: &benchmark,
				Auth:               "alice:pwd",
				AuthChallenge:      &challenge,
				Peers:              "127.0.0.1:19001",
				ReplicationID:      "replica-2",
				RedirectURI:        "tcp://127.0.0.1:19002/amps/json",
				SOWMax:             42,
				HasSOWMax:          true,
				SOWEviction:        "lru",
				SOWDisk:            "cache-dir",
				Views:              []string{"orders_view:orders:/region = 'US'::"},
				Actions:            []string{"on-publish:orders:log:archive"},
			},
		},
	}

	if err := applyRuntimeConfig(runtimeConfig, startupOptions{}); err != nil {
		t.Fatalf("applyRuntimeConfig returned error: %v", err)
	}

	if *flagNoDelay {
		t.Fatalf("flagNoDelay = true, want false")
	}
	if *flagLatency != 50*time.Millisecond {
		t.Fatalf("flagLatency = %v, want 50ms", *flagLatency)
	}
	if *flagLease != 75*time.Second {
		t.Fatalf("flagLease = %v, want 75s", *flagLease)
	}
	if !*flagEcho {
		t.Fatalf("flagEcho = false, want true")
	}
	if !*flagBenchStable {
		t.Fatalf("flagBenchStable = false, want true")
	}
	if *flagAuth != "alice:pwd" {
		t.Fatalf("flagAuth = %q, want alice:pwd", *flagAuth)
	}
	if !*flagAuthChallenge {
		t.Fatalf("flagAuthChallenge = false, want true")
	}
	if *flagPeers != "127.0.0.1:19001" {
		t.Fatalf("flagPeers = %q, want configured peers", *flagPeers)
	}
	if *flagReplID != "replica-2" {
		t.Fatalf("flagReplID = %q, want replica-2", *flagReplID)
	}
	if *flagRedirectURI != "tcp://127.0.0.1:19002/amps/json" {
		t.Fatalf("flagRedirectURI = %q, want configured redirect URI", *flagRedirectURI)
	}
	if *flagSOWMax != 42 {
		t.Fatalf("flagSOWMax = %d, want 42", *flagSOWMax)
	}
	if *flagSOWEviction != "lru" {
		t.Fatalf("flagSOWEviction = %q, want lru", *flagSOWEviction)
	}
	if *flagSOWDisk != "cache-dir" {
		t.Fatalf("flagSOWDisk = %q, want cache-dir", *flagSOWDisk)
	}
	if len(flagViews) != 1 || flagViews[0] != "orders_view:orders:/region = 'US'::" {
		t.Fatalf("flagViews = %v, want configured view", flagViews)
	}
	if len(flagActions) != 1 || flagActions[0] != "on-publish:orders:log:archive" {
		t.Fatalf("flagActions = %v, want configured action", flagActions)
	}
}

func TestConfigureLoggingTargetsWritesToConfiguredFile(t *testing.T) {
	var tempDir = t.TempDir()
	var logPath = filepath.Join(tempDir, "logs", "fakeamps.log")
	var originalOutput = log.Writer()
	defer log.SetOutput(originalOutput)
	defer closeConfiguredLogOutputs()

	var err = configureLoggingTargets(ampsconfig.LoggingConfig{
		Targets: []ampsconfig.LoggingTargetConfig{
			{
				Protocol: "file",
				FileName: logPath,
				Level:    "info",
			},
		},
	})
	if err != nil {
		t.Fatalf("configureLoggingTargets returned error: %v", err)
	}

	log.Print("config-driven log entry")

	var content, readErr = os.ReadFile(logPath)
	if readErr != nil {
		t.Fatalf("ReadFile(logPath): %v", readErr)
	}
	if !strings.Contains(string(content), "config-driven log entry") {
		t.Fatalf("log file content = %q, want written log entry", string(content))
	}
	var dirInfo, statErr = os.Stat(filepath.Dir(logPath))
	if statErr != nil {
		t.Fatalf("Stat(log dir): %v", statErr)
	}
	if runtime.GOOS != "windows" && dirInfo.Mode().Perm() != 0o750 {
		t.Fatalf("log directory mode = %o, want 0750", dirInfo.Mode().Perm())
	}
}

func TestConfigureLoggingTargetsSupportsMultipleStandardOutputs(t *testing.T) {
	var originalOutput = log.Writer()
	defer log.SetOutput(originalOutput)
	defer closeConfiguredLogOutputs()

	var err = configureLoggingTargets(ampsconfig.LoggingConfig{
		Targets: []ampsconfig.LoggingTargetConfig{
			{Protocol: "stdout"},
			{Protocol: "stderr"},
		},
	})
	if err != nil {
		t.Fatalf("configureLoggingTargets returned error: %v", err)
	}
}

func snapshotRuntimeFlags() func() {
	var addr = *flagAddr
	var version = *flagVersion
	var sowEnabled = *flagSOWEnabled
	var journalEnabled = *flagJournal
	var journalMax = *flagJournalMax
	var journalDisk = *flagJournalDisk
	var logConn = *flagLogConn
	var logStats = *flagLogStats
	var statsInterval = *flagStatsIvl
	var writeBuf = *flagWriteBuf
	var readBuf = *flagReadBuf
	var noDelay = *flagNoDelay
	var latency = *flagLatency
	var queueEnabled = *flagQueue
	var lease = *flagLease
	var echo = *flagEcho
	var outDepth = *flagOutDepth
	var sowGCInterval = *flagSOWGCIvl
	var queueLeaseInterval = *flagLeaseIvl
	var benchmarkStability = *flagBenchStable
	var auth = *flagAuth
	var authChallenge = *flagAuthChallenge
	var peers = *flagPeers
	var replID = *flagReplID
	var redirectURI = *flagRedirectURI
	var adminAddr = *flagAdminAddr
	var sowMax = *flagSOWMax
	var sowEviction = *flagSOWEviction
	var sowDisk = *flagSOWDisk
	var views = append(viewFlags(nil), flagViews...)
	var actions = append(actionFlags(nil), flagActions...)

	return func() {
		*flagAddr = addr
		*flagVersion = version
		*flagSOWEnabled = sowEnabled
		*flagJournal = journalEnabled
		*flagJournalMax = journalMax
		*flagJournalDisk = journalDisk
		*flagLogConn = logConn
		*flagLogStats = logStats
		*flagStatsIvl = statsInterval
		*flagWriteBuf = writeBuf
		*flagReadBuf = readBuf
		*flagNoDelay = noDelay
		*flagLatency = latency
		*flagQueue = queueEnabled
		*flagLease = lease
		*flagEcho = echo
		*flagOutDepth = outDepth
		*flagSOWGCIvl = sowGCInterval
		*flagLeaseIvl = queueLeaseInterval
		*flagBenchStable = benchmarkStability
		*flagAuth = auth
		*flagAuthChallenge = authChallenge
		*flagPeers = peers
		*flagReplID = replID
		*flagRedirectURI = redirectURI
		*flagAdminAddr = adminAddr
		*flagSOWMax = sowMax
		*flagSOWEviction = sowEviction
		*flagSOWDisk = sowDisk
		flagViews = views
		flagActions = actions
	}
}
