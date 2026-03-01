// Package main implements fakeamps — a deterministic, stateful AMPS-protocol
// TCP responder for integration and performance testing of custom AMPS client
// implementations. It models the core behaviors of a real 60East AMPS server
// including message journal, SOW cache, content filtering, topic wildcards,
// delta merge, queue topics, views, actions, aggregation, authentication,
// conflation, per-topic message types, and multi-instance replication.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

type viewFlags []string

func (v *viewFlags) String() string { return fmt.Sprintf("%v", *v) }
func (v *viewFlags) Set(s string) error {
	*v = append(*v, s)
	return nil
}

type actionFlags []string

func (a *actionFlags) String() string { return fmt.Sprintf("%v", *a) }
func (a *actionFlags) Set(s string) error {
	*a = append(*a, s)
	return nil
}

var (
	flagAddr        = flag.String("addr", "127.0.0.1:19000", "listen address")
	flagVersion     = flag.String("version", "6.3.1.0", "AMPS server version echoed in logon ack")
	flagFanout      = flag.Bool("fanout", true, "fan-out publishes to matching subscribers")
	flagSOWEnabled  = flag.Bool("sow", true, "enable in-memory SOW cache")
	flagJournal     = flag.Bool("journal", true, "enable in-memory message journal for bookmark replay")
	flagJournalMax  = flag.Int("journal-max", 1_000_000, "maximum journal entries before eviction")
	flagJournalDisk = flag.String("journal-disk", "", "directory for disk-backed journal persistence")
	flagLogConn     = flag.Bool("log-conn", true, "log connect/disconnect events")
	flagLogStats    = flag.Bool("stats", false, "log per-connection throughput stats")
	flagStatsIvl    = flag.Duration("stats-interval", 5*time.Second, "stats logging interval")
	flagWriteBuf    = flag.Int("write-buf", 64*1024, "per-connection TCP write buffer size")
	flagReadBuf     = flag.Int("read-buf", 64*1024, "per-connection TCP read buffer size")
	flagNoDelay     = flag.Bool("nodelay", true, "set TCP_NODELAY")
	flagLatency     = flag.Duration("latency", 0, "artificial per-message latency")
	flagQueue       = flag.Bool("queue", true, "enable queue:// topic support")
	flagLease       = flag.Duration("lease", 30*time.Second, "default queue lease period")
	flagEcho        = flag.Bool("echo", false, "echo publishes back to same connection")
	flagOutDepth    = flag.Int("out-depth", 65536, "per-connection outbound channel depth")
	flagSOWGCIvl    = flag.Duration("sow-gc-interval", 30*time.Second, "interval for SOW expiration sweep (0 disables sweep)")
	flagLeaseIvl    = flag.Duration("queue-lease-interval", 5*time.Second, "interval for queue lease expiration checks (0 disables watcher)")
	flagBenchStable = flag.Bool("benchmark-stability", false, "apply low-noise benchmark defaults without changing protocol semantics")

	// Authentication
	flagAuth          = flag.String("auth", "", "enable auth with user:pass pairs (e.g. 'user1:pass1,user2:pass2')")
	flagAuthChallenge = flag.Bool("auth-challenge", false, "require two-step challenge-response logon when auth is enabled")

	// Replication
	flagPeers  = flag.String("peers", "", "comma-separated peer addresses for HA replication")
	flagReplID = flag.String("repl-id", "instance-1", "unique replication instance ID")
	// Redirect behavior for HA client failover testing.
	flagRedirectURI = flag.String("redirect-uri", "", "if set, logon responds with redirect command to this URI")

	// Admin API
	flagAdminAddr = flag.String("admin", "", "admin REST API listen address (e.g. ':8085')")

	// SOW eviction
	flagSOWMax      = flag.Int("sow-max", 0, "max SOW records per topic (0=unlimited)")
	flagSOWEviction = flag.String("sow-eviction", "oldest", "SOW eviction policy: none, lru, oldest, capacity")
	flagSOWDisk     = flag.String("sow-disk", "", "directory for disk-backed SOW persistence")

	// Views and Actions (multi-value flags)
	flagViews   viewFlags
	flagActions actionFlags
)

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------

var (
	globalConnectionsAccepted atomic.Uint64
	globalConnectionsCurrent  atomic.Int64
	globalBookmarkSeq         atomic.Uint64

	journal *messageJournal
	sow     *sowCache
)

type benchmarkStabilitySettings struct {
	logConn            bool
	logStats           bool
	sowGCInterval      time.Duration
	queueLeaseInterval time.Duration
}

type benchmarkStabilityOverrides struct {
	logConnSet            bool
	logStatsSet           bool
	sowGCIntervalSet      bool
	queueLeaseIntervalSet bool
}

func flagSetByUser(flagSet *flag.FlagSet, name string) bool {
	if flagSet == nil {
		return false
	}

	var found bool
	flagSet.Visit(func(current *flag.Flag) {
		if current != nil && current.Name == name {
			found = true
		}
	})
	return found
}

func applyBenchmarkStabilitySettings(enabled bool, settings benchmarkStabilitySettings, overrides benchmarkStabilityOverrides) benchmarkStabilitySettings {
	if !enabled {
		return settings
	}

	if !overrides.logConnSet {
		settings.logConn = false
	}
	if !overrides.logStatsSet {
		settings.logStats = false
	}
	if !overrides.sowGCIntervalSet {
		settings.sowGCInterval = 5 * time.Minute
	}
	if !overrides.queueLeaseIntervalSet {
		settings.queueLeaseInterval = 30 * time.Second
	}

	return settings
}

func applyBenchmarkStabilityDefaults(flagSet *flag.FlagSet) {
	var settings = benchmarkStabilitySettings{
		logConn:            *flagLogConn,
		logStats:           *flagLogStats,
		sowGCInterval:      *flagSOWGCIvl,
		queueLeaseInterval: *flagLeaseIvl,
	}
	var overrides = benchmarkStabilityOverrides{
		logConnSet:            flagSetByUser(flagSet, "log-conn"),
		logStatsSet:           flagSetByUser(flagSet, "stats"),
		sowGCIntervalSet:      flagSetByUser(flagSet, "sow-gc-interval"),
		queueLeaseIntervalSet: flagSetByUser(flagSet, "queue-lease-interval"),
	}

	var updated = applyBenchmarkStabilitySettings(*flagBenchStable, settings, overrides)
	*flagLogConn = updated.logConn
	*flagLogStats = updated.logStats
	*flagSOWGCIvl = updated.sowGCInterval
	*flagLeaseIvl = updated.queueLeaseInterval
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	flag.Var(&flagViews, "view", "register a view: 'name:source:filter:aggregates:groupBy' (repeatable)")
	flag.Var(&flagActions, "action", "register an action: 'trigger:topic:type:target' (repeatable)")
	flag.Parse()
	applyBenchmarkStabilityDefaults(flag.CommandLine)

	// Initialize server-global state.
	if *flagJournal {
		if *flagJournalDisk != "" {
			journal = newDiskJournal(*flagJournalMax, *flagJournalDisk)
		} else {
			journal = newMessageJournal(*flagJournalMax)
		}
	}
	if *flagSOWEnabled {
		policy := evictionOldest
		switch *flagSOWEviction {
		case "none":
			policy = evictionNone
		case "lru":
			policy = evictionLRU
		case "oldest":
			policy = evictionOldest
		case "capacity":
			policy = evictionCapacity
		}
		if *flagSOWDisk != "" {
			sow = newDiskSOWCache(*flagSOWDisk, *flagSOWMax, policy)
		} else if *flagSOWMax > 0 {
			sow = newSOWCacheWithEviction(*flagSOWMax, policy)
		} else {
			sow = newSOWCache()
		}
	}

	// Authentication.
	if *flagAuth != "" {
		configureAuth(*flagAuth)
	}

	// Views.
	for _, spec := range flagViews {
		v := parseViewDef(spec)
		if v != nil {
			registerView(v)
		} else {
			log.Printf("fakeamps: invalid view spec: %q", spec)
		}
	}

	// Actions.
	for _, spec := range flagActions {
		a := parseActionDef(spec)
		if a != nil {
			registerAction(*a)
		} else {
			log.Printf("fakeamps: invalid action spec: %q", spec)
		}
	}

	listener, err := net.Listen("tcp", *flagAddr)
	if err != nil {
		log.Fatalf("fakeamps: listen %s failed: %v", *flagAddr, err)
	}

	// Replication.
	if *flagPeers != "" {
		initReplication(*flagPeers, *flagReplID)
	}

	// Admin API.
	if *flagAdminAddr != "" {
		startAdminServer(*flagAdminAddr)
	}

	// SOW expiration GC — periodically sweep expired records.
	if *flagSOWEnabled && *flagSOWGCIvl > 0 {
		go func() {
			ticker := time.NewTicker(*flagSOWGCIvl)
			defer ticker.Stop()
			var stats connStats
			for range ticker.C {
				if sow != nil {
					var expired = sow.gcExpiredRecords()
					if len(expired) > 0 {
						for _, record := range expired {
							fanoutOOFWithReason(nil, record.topic, record.sowKey, record.bookmark, "expire", &stats)
						}
						log.Printf("fakeamps: sow gc removed %d expired records", len(expired))
					}
				}
			}
		}()
	}

	// Queue lease watcher — requeue messages on lease timeout.
	if *flagQueue && *flagLeaseIvl > 0 {
		StartLeaseWatcher(*flagLeaseIvl)
	}

	// Graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("fakeamps: received %v, shutting down", sig)
		stopReplication()
		if journal != nil {
			journal.Close()
		}
		_ = listener.Close()
	}()

	log.Printf("fakeamps %s listening on %s  (fanout=%v sow=%v journal=%v journal_disk=%q queue=%v echo=%v auth=%v views=%d actions=%d peers=%q redirect=%q admin=%q GOMAXPROCS=%d)",
		*flagVersion, *flagAddr, *flagFanout, *flagSOWEnabled, *flagJournal,
		*flagJournalDisk, *flagQueue, *flagEcho, *flagAuth != "", len(flagViews), len(flagActions),
		*flagPeers, *flagRedirectURI, *flagAdminAddr, runtime.GOMAXPROCS(0))

	for {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			if isClosedError(acceptErr) {
				log.Printf("fakeamps: listener closed, exiting")
				return
			}
			log.Printf("fakeamps: accept: %v", acceptErr)
			continue
		}
		globalConnectionsAccepted.Add(1)
		globalConnectionsCurrent.Add(1)
		go handleConnection(conn)
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(os.Stderr)

	if n := runtime.NumCPU(); runtime.GOMAXPROCS(0) < n {
		runtime.GOMAXPROCS(n)
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "fakeamps — deterministic AMPS-protocol TCP responder for perf testing\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}
}
