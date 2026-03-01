package main

import (
	"log"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// Actions — on-publish and on-deliver triggers.
//
// Real AMPS supports configurable actions that fire on certain events:
//   - on-publish: fired when a message is published to a topic
//   - on-deliver: fired when a message is about to be delivered to a subscriber
//
// Action types supported:
//   - route: republish the message to another topic
//   - log: log the message to server output
//   - transform: apply a simple field transformation
//
// Configuration via -action flag:
//   -action "on-publish:orders:route:orders_archive"
//   -action "on-publish:trades:log"
//   -action "on-deliver:alerts:transform:add_timestamp"
// ---------------------------------------------------------------------------

type actionType int

const (
	actionRoute     actionType = iota // republish to another topic
	actionLog                         // log to server output
	actionTransform                   // field transformation
)

type actionTrigger int

const (
	triggerOnPublish actionTrigger = iota
	triggerOnDeliver
)

type actionDef struct {
	trigger    actionTrigger
	topicMatch string // topic pattern to match
	action     actionType
	target     string // target topic for route, transform name for transform
	filter     string // optional content filter
}

var (
	actionsMu sync.RWMutex
	actions   []actionDef
)

func registerAction(a actionDef) {
	actionsMu.Lock()
	actions = append(actions, a)
	actionsMu.Unlock()
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
	log.Printf("fakeamps: action registered: %s:%s → %s:%s",
		triggerName, a.topicMatch, actionName, a.target)
}

// fireOnPublish executes all on-publish actions matching the topic.
func fireOnPublish(topic string, payload []byte, bookmark string) {
	actionsMu.RLock()
	defer actionsMu.RUnlock()

	for _, a := range actions {
		if a.trigger != triggerOnPublish {
			continue
		}
		if !topicMatches(topic, a.topicMatch) {
			continue
		}
		if a.filter != "" && !evaluateFilter(a.filter, payload) {
			continue
		}

		switch a.action {
		case actionRoute:
			// Republish to target topic.
			if a.target != "" && sow != nil {
				mt := getOrSetTopicMessageType(a.target, getTopicMessageType(topic))
				seq := globalBookmarkSeq.Add(1)
				bm := makeBookmark(seq)
				ts := makeTimestamp()
				sowKey := extractSowKey(payload)
				if sowKey == "" {
					sowKey = makeSowKey(a.target, seq)
				}
				sow.upsert(a.target, sowKey, payload, bm, ts, seq, 0)
				if journal != nil {
					journal.append(a.target, sowKey, payload)
				}
				// Fan-out to subscribers on target topic.
				buf := getWriteBuf()
				forEachMatchingSubscriber(a.target, payload, func(sub *subscription) {
					frame := buildPublishDelivery(buf, a.target, sub.subID,
						payload, bm, ts, sowKey, mt, false)
					sub.writer.send(frame)
				})
				putWriteBuf(buf)
			}
		case actionLog:
			log.Printf("fakeamps: action-log topic=%s bm=%s payload=%s",
				topic, bookmark, string(payload))
		case actionTransform:
			// Transform is applied in-place (future: configurable transforms).
		}
	}
}

// fireOnDeliver can modify the payload before delivery.
// Returns the (possibly modified) payload.
func fireOnDeliver(topic string, payload []byte, subID string) []byte {
	actionsMu.RLock()
	defer actionsMu.RUnlock()

	result := payload
	for _, a := range actions {
		if a.trigger != triggerOnDeliver {
			continue
		}
		if !topicMatches(topic, a.topicMatch) {
			continue
		}

		switch a.action {
		case actionTransform:
			if a.target == "add_timestamp" {
				// Inject a delivery timestamp into the payload.
				ts := makeTimestamp()
				result = injectField(result, "_delivered_at", `"`+ts+`"`)
			}
		case actionLog:
			log.Printf("fakeamps: action-deliver-log topic=%s sub=%s payload=%s",
				topic, subID, string(result))
		}
	}

	return result
}

// injectField adds a field to a flat JSON object.
func injectField(payload []byte, key, value string) []byte {
	if len(payload) < 2 || payload[0] != '{' {
		return payload
	}
	// Insert before the closing brace.
	result := make([]byte, 0, len(payload)+len(key)+len(value)+6)
	result = append(result, payload[:len(payload)-1]...)
	if len(payload) > 2 {
		result = append(result, ',')
	}
	result = append(result, '"')
	result = append(result, key...)
	result = append(result, '"', ':')
	result = append(result, value...)
	result = append(result, '}')
	return result
}

// ---------------------------------------------------------------------------
// Action definition parsing from command-line flag.
//
// Format: "trigger:topic_pattern:action_type:target"
//   trigger: on-publish, on-deliver
//   topic_pattern: topic to match (supports wildcards)
//   action_type: route, log, transform
//   target: target topic (for route), transform name (for transform)
//
// Examples:
//   "on-publish:orders:route:orders_archive"
//   "on-publish:trades:log"
//   "on-deliver:alerts:transform:add_timestamp"
// ---------------------------------------------------------------------------

func parseActionDef(spec string) *actionDef {
	parts := strings.SplitN(spec, ":", 4)
	if len(parts) < 3 {
		return nil
	}

	a := &actionDef{topicMatch: parts[1]}

	switch parts[0] {
	case "on-publish":
		a.trigger = triggerOnPublish
	case "on-deliver":
		a.trigger = triggerOnDeliver
	default:
		return nil
	}

	switch parts[2] {
	case "route":
		a.action = actionRoute
	case "log":
		a.action = actionLog
	case "transform":
		a.action = actionTransform
	default:
		return nil
	}

	if len(parts) >= 4 {
		a.target = parts[3]
	}

	return a
}
