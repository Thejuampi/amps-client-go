package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Admin REST API — provides operational visibility into fakeamps state.
//
// Real AMPS exposes a comprehensive RESTful admin API (typically on port 8085)
// for querying server state, inspecting SOW sizes, dropping clients, and
// monitoring statistics.
//
// Endpoints:
//   GET /admin/status         — server status and uptime
//   GET /admin/stats          — global message statistics
//   GET /admin/connections    — list active connections
//   GET /admin/sow            — SOW cache summary per topic
//   GET /admin/sow/:topic     — SOW details for a specific topic
//   GET /admin/subscriptions  — list all active subscriptions
//   GET /admin/views          — list registered views
//   GET /admin/actions        — list registered actions
//   GET /admin/journal        — journal statistics
//   DELETE /admin/sow/:topic  — clear SOW for a topic
//   DELETE /admin/connections/:id — drop a connection (future)
// ---------------------------------------------------------------------------

var (
	serverStartTime time.Time
	adminAddr       string
)

func init() {
	serverStartTime = time.Now()
}

func startAdminServer(addr string) {
	if addr == "" {
		return
	}
	adminAddr = addr

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/status", handleAdminStatus)
	mux.HandleFunc("/admin/stats", handleAdminStats)
	mux.HandleFunc("/admin/sow", handleAdminSOW)
	mux.HandleFunc("/admin/sow/", handleAdminSOWTopic)
	mux.HandleFunc("/admin/subscriptions", handleAdminSubscriptions)
	mux.HandleFunc("/admin/views", handleAdminViews)
	mux.HandleFunc("/admin/actions", handleAdminActions)
	mux.HandleFunc("/admin/journal", handleAdminJournal)

	go func() {
		log.Printf("fakeamps: admin API listening on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("fakeamps: admin API error: %v", err)
		}
	}()
}

func jsonResponse(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func handleAdminStatus(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	jsonResponse(w, map[string]interface{}{
		"server":    "fakeamps",
		"version":   *flagVersion,
		"uptime":    time.Since(serverStartTime).String(),
		"uptime_ms": time.Since(serverStartTime).Milliseconds(),
		"started":   serverStartTime.Format(time.RFC3339),
		"connections": map[string]interface{}{
			"total_accepted": globalConnectionsAccepted.Load(),
			"current":        globalConnectionsCurrent.Load(),
		},
		"memory": map[string]interface{}{
			"alloc_mb":       m.Alloc / 1024 / 1024,
			"total_alloc_mb": m.TotalAlloc / 1024 / 1024,
			"sys_mb":         m.Sys / 1024 / 1024,
			"num_gc":         m.NumGC,
		},
		"goroutines": runtime.NumGoroutine(),
		"gomaxprocs": runtime.GOMAXPROCS(0),
		"config": map[string]interface{}{
			"addr":        *flagAddr,
			"fanout":      *flagFanout,
			"sow":         *flagSOWEnabled,
			"journal":     *flagJournal,
			"journal_max": *flagJournalMax,
			"queue":       *flagQueue,
			"echo":        *flagEcho,
			"auth":        *flagAuth != "",
		},
	})
}

func handleAdminStats(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, map[string]interface{}{
		"connections_accepted": globalConnectionsAccepted.Load(),
		"connections_current":  globalConnectionsCurrent.Load(),
		"bookmark_seq":         globalBookmarkSeq.Load(),
	})
}

func handleAdminSOW(w http.ResponseWriter, r *http.Request) {
	if r.Method == "DELETE" {
		// Clear all SOW data.
		if sow != nil {
			for _, t := range sow.allTopics() {
				raw, ok := sow.topics.Load(t)
				if !ok {
					continue
				}
				ts := raw.(*topicSOW)
				ts.mu.Lock()
				ts.records = make(map[string]*sowRecord)
				ts.mu.Unlock()
			}
		}
		jsonResponse(w, map[string]string{"status": "cleared"})
		return
	}

	if sow == nil {
		jsonResponse(w, map[string]interface{}{"topics": []string{}})
		return
	}

	topicStats := make(map[string]interface{})
	sow.topics.Range(func(key, value interface{}) bool {
		t := value.(*topicSOW)
		t.mu.RLock()
		active := 0
		expired := 0
		for _, r := range t.records {
			if r.isExpired() {
				expired++
			} else {
				active++
			}
		}
		t.mu.RUnlock()
		topicStats[key.(string)] = map[string]interface{}{
			"records_active":  active,
			"records_expired": expired,
			"records_total":   active + expired,
			"message_type":    getTopicMessageType(key.(string)),
		}
		return true
	})

	jsonResponse(w, map[string]interface{}{"topics": topicStats})
}

func handleAdminSOWTopic(w http.ResponseWriter, r *http.Request) {
	topic := strings.TrimPrefix(r.URL.Path, "/admin/sow/")
	if topic == "" {
		http.Error(w, "topic required", http.StatusBadRequest)
		return
	}

	if r.Method == "DELETE" {
		if sow != nil {
			raw, ok := sow.topics.Load(topic)
			if ok {
				t := raw.(*topicSOW)
				t.mu.Lock()
				cleared := len(t.records)
				t.records = make(map[string]*sowRecord)
				t.mu.Unlock()
				jsonResponse(w, map[string]interface{}{
					"status":          "cleared",
					"topic":           topic,
					"records_cleared": cleared,
				})
				return
			}
		}
		jsonResponse(w, map[string]interface{}{"status": "not_found", "topic": topic})
		return
	}

	if sow == nil {
		jsonResponse(w, map[string]interface{}{"topic": topic, "records": []string{}})
		return
	}

	result := sow.query(topic, "", -1, "")
	records := make([]map[string]interface{}, 0, len(result.records))
	for _, rec := range result.records {
		records = append(records, map[string]interface{}{
			"sow_key":   rec.sowKey,
			"bookmark":  rec.bookmark,
			"timestamp": rec.timestamp,
			"seq_num":   rec.seqNum,
			"payload":   string(rec.payload),
			"expired":   rec.isExpired(),
		})
	}

	jsonResponse(w, map[string]interface{}{
		"topic":         topic,
		"total_records": result.totalCount,
		"records":       records,
	})
}

func handleAdminSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs := make([]map[string]interface{}, 0)
	topicSubscribers.Range(func(key, value interface{}) bool {
		ss := value.(*subscriberSet)
		ss.mu.RLock()
		for sub := range ss.subs {
			subs = append(subs, map[string]interface{}{
				"sub_id":      sub.subID,
				"topic":       sub.topic,
				"filter":      sub.filter,
				"is_delta":    sub.isDelta,
				"is_queue":    sub.isQueue,
				"is_bookmark": sub.isBookmark,
				"paused":      sub.paused.Load(),
				"remote_addr": sub.conn.RemoteAddr().String(),
			})
		}
		ss.mu.RUnlock()
		return true
	})

	jsonResponse(w, map[string]interface{}{
		"count":         len(subs),
		"subscriptions": subs,
	})
}

func handleAdminViews(w http.ResponseWriter, r *http.Request) {
	viewsMu.RLock()
	viewList := make([]map[string]interface{}, 0, len(views))
	for name, v := range views {
		aggs := make([]string, 0, len(v.aggregates))
		for _, a := range v.aggregates {
			aggs = append(aggs, fmt.Sprintf("%s(%s)", a.function, a.field))
		}
		viewList = append(viewList, map[string]interface{}{
			"name":        name,
			"sources":     v.sources,
			"filter":      v.filter,
			"group_by":    v.groupByField,
			"aggregates":  aggs,
			"projections": v.projection,
		})
	}
	viewsMu.RUnlock()

	jsonResponse(w, map[string]interface{}{
		"count": len(viewList),
		"views": viewList,
	})
}

func handleAdminActions(w http.ResponseWriter, r *http.Request) {
	actionsMu.RLock()
	actionList := make([]map[string]interface{}, 0, len(actions))
	for _, a := range actions {
		triggerName := "on-publish"
		if a.trigger == triggerOnDeliver {
			triggerName = "on-deliver"
		}
		actionName := "route"
		if a.action == actionLog {
			actionName = "log"
		} else if a.action == actionTransform {
			actionName = "transform"
		}
		actionList = append(actionList, map[string]interface{}{
			"trigger": triggerName,
			"topic":   a.topicMatch,
			"action":  actionName,
			"target":  a.target,
		})
	}
	actionsMu.RUnlock()

	jsonResponse(w, map[string]interface{}{
		"count":   len(actionList),
		"actions": actionList,
	})
}

func handleAdminJournal(w http.ResponseWriter, r *http.Request) {
	if journal == nil {
		jsonResponse(w, map[string]interface{}{"enabled": false})
		return
	}

	journal.mu.RLock()
	jsonResponse(w, map[string]interface{}{
		"enabled":   true,
		"max_size":  journal.maxSize,
		"count":     journal.count,
		"head":      journal.head,
		"seq_range": []uint64{globalBookmarkSeq.Load() - uint64(journal.count), globalBookmarkSeq.Load()},
	})
	journal.mu.RUnlock()
}
