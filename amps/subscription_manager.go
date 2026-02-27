package amps

import (
	"sort"
	"sync"
)

// DefaultSubscriptionManager stores exported state used by AMPS client APIs.
type DefaultSubscriptionManager struct {
	lock                     sync.RWMutex
	subscriptions            map[string]trackedSubscription
	failedResubscribeHandler FailedResubscribeHandler
}

// NewDefaultSubscriptionManager returns a new DefaultSubscriptionManager.
func NewDefaultSubscriptionManager() *DefaultSubscriptionManager {
	return &DefaultSubscriptionManager{
		subscriptions: make(map[string]trackedSubscription),
	}
}

func trackedSubscriptionID(command *Command) string {
	if command == nil {
		return ""
	}
	if subID, hasSubID := command.SubID(); hasSubID && subID != "" {
		return subID
	}
	if queryID, hasQueryID := command.QueryID(); hasQueryID && queryID != "" {
		return queryID
	}
	if commandID, hasCommandID := command.CommandID(); hasCommandID && commandID != "" {
		return commandID
	}
	return ""
}

// Subscribe executes a subscription command and returns a MessageStream.
func (manager *DefaultSubscriptionManager) Subscribe(messageHandler func(*Message) error, command *Command, requestedAckTypes int) {
	if manager == nil || command == nil {
		return
	}

	commandType, _ := command.Command()
	if commandType != "subscribe" &&
		commandType != "delta_subscribe" &&
		commandType != "sow_and_subscribe" &&
		commandType != "sow_and_delta_subscribe" {
		return
	}

	subscriptionID := trackedSubscriptionID(command)
	if subscriptionID == "" {
		return
	}

	var existingHandler func(*Message) error
	manager.lock.RLock()
	if messageHandler == nil {
		if existing, exists := manager.subscriptions[subscriptionID]; exists {
			existingHandler = existing.messageHandler
		}
	}
	manager.lock.RUnlock()

	manager.lock.Lock()
	if messageHandler == nil {
		if existing, exists := manager.subscriptions[subscriptionID]; exists {
			messageHandler = existing.messageHandler
		}
		if messageHandler == nil {
			messageHandler = existingHandler
		}
	}
	manager.subscriptions[subscriptionID] = trackedSubscription{
		messageHandler:    messageHandler,
		command:           cloneCommand(command),
		requestedAckTypes: requestedAckTypes,
	}
	manager.lock.Unlock()
}

// Unsubscribe executes the exported unsubscribe operation.
func (manager *DefaultSubscriptionManager) Unsubscribe(subID string) {
	if manager == nil {
		return
	}
	manager.lock.Lock()
	defer manager.lock.Unlock()

	if subID == "" || subID == "all" {
		manager.subscriptions = make(map[string]trackedSubscription)
		return
	}
	delete(manager.subscriptions, subID)
}

// Clear executes the exported clear operation.
func (manager *DefaultSubscriptionManager) Clear() {
	if manager == nil {
		return
	}
	manager.lock.Lock()
	manager.subscriptions = make(map[string]trackedSubscription)
	manager.lock.Unlock()
}

// SetFailedResubscribeHandler sets failed resubscribe handler on the receiver.
func (manager *DefaultSubscriptionManager) SetFailedResubscribeHandler(handler FailedResubscribeHandler) {
	if manager == nil {
		return
	}
	manager.lock.Lock()
	manager.failedResubscribeHandler = handler
	manager.lock.Unlock()
}

// Resubscribe executes the exported resubscribe operation.
func (manager *DefaultSubscriptionManager) Resubscribe(client *Client) error {
	if manager == nil || client == nil {
		return nil
	}

	manager.lock.RLock()
	ids := make([]string, 0, len(manager.subscriptions))
	for subID := range manager.subscriptions {
		ids = append(ids, subID)
	}
	failedHandler := manager.failedResubscribeHandler
	manager.lock.RUnlock()

	sort.Strings(ids)

	subscriptions := make([]trackedSubscription, 0, len(ids))
	for _, subID := range ids {
		manager.lock.RLock()
		if sub, ok := manager.subscriptions[subID]; ok {
			subscriptions = append(subscriptions, sub)
		}
		manager.lock.RUnlock()
	}

	state := ensureClientState(client)
	noResubscribe := map[string]struct{}{}
	if state != nil {
		state.lock.Lock()
		for subID := range state.noResubscribeRoutes {
			noResubscribe[subID] = struct{}{}
		}
		state.lock.Unlock()
	}

	for _, subscription := range subscriptions {
		command := cloneCommand(subscription.command)
		subID := trackedSubscriptionID(command)
		if _, blocked := noResubscribe[subID]; blocked {
			continue
		}

		if _, err := client.ExecuteAsync(command, subscription.messageHandler); err != nil {
			if failedHandler != nil && failedHandler.Failure(command, subscription.requestedAckTypes, err) {
				continue
			}
			return err
		}

	}

	return nil
}
