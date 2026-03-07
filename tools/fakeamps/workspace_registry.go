package main

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
)

type workspaceLiveQuery struct {
	RequestID string
	Mode      string
	Topic     string
	Filter    string
	Options   sqlWorkspaceOptions
}

type workspaceSessionRegistry struct {
	mu      sync.RWMutex
	queries map[*websocketConn]workspaceLiveQuery
}

type workspaceTopic struct {
	Name              string   `json:"name"`
	MessageType       string   `json:"message_type"`
	Sources           []string `json:"sources"`
	SOWRecords        int      `json:"sow_records"`
	SubscriptionCount int      `json:"subscription_count"`
	IsView            bool     `json:"is_view"`
	IsQueue           bool     `json:"is_queue"`
}

var workspaceSessions = &workspaceSessionRegistry{
	queries: make(map[*websocketConn]workspaceLiveQuery),
}

func (registry *workspaceSessionRegistry) Set(conn *websocketConn, query workspaceLiveQuery) {
	if registry == nil || conn == nil {
		return
	}

	registry.mu.Lock()
	registry.queries[conn] = query
	registry.mu.Unlock()
}

func (registry *workspaceSessionRegistry) Remove(conn *websocketConn) {
	if registry == nil || conn == nil {
		return
	}

	registry.mu.Lock()
	delete(registry.queries, conn)
	registry.mu.Unlock()
}

func (registry *workspaceSessionRegistry) Stop(conn *websocketConn, requestID string) bool {
	if registry == nil || conn == nil {
		return false
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	var query, ok = registry.queries[conn]
	if !ok {
		return false
	}
	if requestID != "" && query.RequestID != requestID {
		return false
	}

	delete(registry.queries, conn)
	return true
}

func (registry *workspaceSessionRegistry) NotifyRow(topic string, previousPayload []byte, payload []byte, bookmark string, timestamp string, sowKey string) {
	if registry == nil {
		return
	}

	registry.mu.RLock()
	var snapshot = make(map[*websocketConn]workspaceLiveQuery, len(registry.queries))
	for conn, query := range registry.queries {
		snapshot[conn] = query
	}
	registry.mu.RUnlock()

	for conn, query := range snapshot {
		if !topicMatches(topic, query.Topic) {
			continue
		}
		var matchedBefore = len(previousPayload) > 0 && workspaceQueryMatches(query, topic, previousPayload)
		if !workspaceQueryMatches(query, topic, payload) {
			if matchedBefore && sowKey != "" {
				var removeMessage = mustJSON(map[string]interface{}{
					"type":       "workspace_remove",
					"request_id": query.RequestID,
					"topic":      topic,
					"sow_key":    sowKey,
					"bookmark":   bookmark,
					"reason":     "match",
				})
				if _, err := conn.WriteText(removeMessage); err != nil {
					registry.Remove(conn)
				}
			}
			continue
		}

		var message = mustJSON(map[string]interface{}{
			"type":       "workspace_row",
			"request_id": query.RequestID,
			"row":        workspaceRow(topic, sowKey, bookmark, timestamp, "record", payload),
		})
		if _, err := conn.WriteText(message); err != nil {
			registry.Remove(conn)
		}
	}
}

func (registry *workspaceSessionRegistry) NotifyRemove(topic string, sowKey string, bookmark string, reason string) {
	if registry == nil {
		return
	}

	registry.mu.RLock()
	var snapshot = make(map[*websocketConn]workspaceLiveQuery, len(registry.queries))
	for conn, query := range registry.queries {
		snapshot[conn] = query
	}
	registry.mu.RUnlock()

	for conn, query := range snapshot {
		if !topicMatches(topic, query.Topic) {
			continue
		}

		var message = mustJSON(map[string]interface{}{
			"type":       "workspace_remove",
			"request_id": query.RequestID,
			"topic":      topic,
			"sow_key":    sowKey,
			"bookmark":   bookmark,
			"reason":     reason,
		})
		if _, err := conn.WriteText(message); err != nil {
			registry.Remove(conn)
		}
	}
}

func workspaceQueryMatches(query workspaceLiveQuery, topic string, payload []byte) bool {
	if query.Topic == "" || !topicMatches(topic, query.Topic) {
		return false
	}
	if query.Filter == "" {
		return true
	}
	if len(payload) == 0 {
		return false
	}
	return evaluateFilter(query.Filter, payload)
}

func workspaceRow(topic string, sowKey string, bookmark string, timestamp string, rowType string, payload []byte) map[string]interface{} {
	var row = map[string]interface{}{
		"topic":     topic,
		"sow_key":   sowKey,
		"bookmark":  bookmark,
		"timestamp": timestamp,
		"type":      rowType,
	}

	if len(payload) == 0 {
		return row
	}

	var object map[string]interface{}
	if err := json.Unmarshal(payload, &object); err == nil && object != nil {
		row["payload"] = object
		return row
	}

	row["raw_payload"] = string(payload)
	return row
}

func (service *monitoringService) topicInventory(search string) []workspaceTopic {
	type accumulator struct {
		workspaceTopic
		sourceSet map[string]struct{}
	}

	var inventory = make(map[string]*accumulator)
	var addTopic = func(name string, source string) *accumulator {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil
		}
		var current = inventory[name]
		if current == nil {
			current = &accumulator{
				workspaceTopic: workspaceTopic{
					Name:        name,
					MessageType: getTopicMessageType(name),
					IsQueue:     strings.HasPrefix(name, "queue://"),
				},
				sourceSet: make(map[string]struct{}),
			}
			inventory[name] = current
		}
		if source != "" {
			current.sourceSet[source] = struct{}{}
		}
		return current
	}

	if sow != nil {
		for _, topic := range sow.allTopics() {
			var current = addTopic(topic, "sow")
			if current != nil {
				current.SOWRecords = sow.count(topic)
			}
		}
	}

	topicConfigsMu.RLock()
	for topic := range topicConfigs {
		addTopic(topic, "published")
	}
	topicConfigsMu.RUnlock()

	topicSubscribers.Range(func(key, value interface{}) bool {
		var topic = key.(string)
		var current = addTopic(topic, "subscriptions")
		if current != nil {
			var set = value.(*subscriberSet)
			set.mu.RLock()
			current.SubscriptionCount = len(set.subs)
			set.mu.RUnlock()
		}
		return true
	})

	viewsMu.RLock()
	for _, view := range views {
		var current = addTopic(view.name, "views")
		if current != nil {
			current.IsView = true
		}
		for _, source := range view.sources {
			addTopic(source, "views")
		}
	}
	viewsMu.RUnlock()

	actionsMu.RLock()
	for _, action := range actions {
		addTopic(action.topicMatch, "actions")
		addTopic(action.target, "actions")
	}
	actionsMu.RUnlock()

	var matcher = strings.ToLower(strings.TrimSpace(search))
	var topics = make([]workspaceTopic, 0, len(inventory))
	for _, current := range inventory {
		if matcher != "" && !strings.Contains(strings.ToLower(current.Name), matcher) {
			continue
		}

		current.Sources = current.Sources[:0]
		for source := range current.sourceSet {
			current.Sources = append(current.Sources, source)
		}
		sort.Strings(current.Sources)
		topics = append(topics, current.workspaceTopic)
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})
	return topics
}
