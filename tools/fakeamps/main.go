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

	// Authentication
	flagAuth = flag.String("auth", "", "enable auth with user:pass pairs (e.g. 'user1:pass1,user2:pass2')")

	// Replication
	flagPeers  = flag.String("peers", "", "comma-separated peer addresses for HA replication")
	flagReplID = flag.String("repl-id", "instance-1", "unique replication instance ID")

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

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	flag.Var(&flagViews, "view", "register a view: 'name:source:filter:aggregates:groupBy' (repeatable)")
	flag.Var(&flagActions, "action", "register an action: 'trigger:topic:type:target' (repeatable)")
	flag.Parse()

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
	if *flagSOWEnabled {
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				if sow != nil {
					if n := sow.gcExpired(); n > 0 {
						log.Printf("fakeamps: sow gc removed %d expired records", n)
					}
				}
			}
		}()
	}

	// Queue lease watcher — requeue messages on lease timeout.
	if *flagQueue {
		StartLeaseWatcher(5 * time.Second)
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

	log.Printf("fakeamps %s listening on %s  (fanout=%v sow=%v journal=%v journal_disk=%q queue=%v echo=%v auth=%v views=%d actions=%d peers=%q admin=%q GOMAXPROCS=%d)",
		*flagVersion, *flagAddr, *flagFanout, *flagSOWEnabled, *flagJournal,
		*flagJournalDisk, *flagQueue, *flagEcho, *flagAuth != "", len(flagViews), len(flagActions),
		*flagPeers, *flagAdminAddr, runtime.GOMAXPROCS(0))

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
