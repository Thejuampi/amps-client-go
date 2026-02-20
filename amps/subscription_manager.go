package amps

import (
	"sort"
	"sync"
)

// DefaultSubscriptionManager is the default in-memory subscription manager.
type DefaultSubscriptionManager struct {
	lock                     sync.Mutex
	subscriptions            map[string]trackedSubscription
	failedResubscribeHandler FailedResubscribeHandler
}

// NewDefaultSubscriptionManager creates a new default subscription manager.
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

// Subscribe stores a subscription command for future resubscribe attempts.
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

	manager.lock.Lock()
	if messageHandler == nil {
		if existing, exists := manager.subscriptions[subscriptionID]; exists {
			messageHandler = existing.messageHandler
		}
	}
	manager.subscriptions[subscriptionID] = trackedSubscription{
		messageHandler:    messageHandler,
		command:           cloneCommand(command),
		requestedAckTypes: requestedAckTypes,
	}
	manager.lock.Unlock()
}

// Unsubscribe removes one tracked subscription, or all when subID=="all".
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

// Clear removes all tracked subscriptions.
func (manager *DefaultSubscriptionManager) Clear() {
	if manager == nil {
		return
	}
	manager.lock.Lock()
	manager.subscriptions = make(map[string]trackedSubscription)
	manager.lock.Unlock()
}

// SetFailedResubscribeHandler sets handler for resubscribe failures.
func (manager *DefaultSubscriptionManager) SetFailedResubscribeHandler(handler FailedResubscribeHandler) {
	if manager == nil {
		return
	}
	manager.lock.Lock()
	manager.failedResubscribeHandler = handler
	manager.lock.Unlock()
}

// Resubscribe replays tracked subscriptions on a connected/logged-on client.
func (manager *DefaultSubscriptionManager) Resubscribe(client *Client) error {
	if manager == nil || client == nil {
		return nil
	}

	manager.lock.Lock()
	ids := make([]string, 0, len(manager.subscriptions))
	for subID := range manager.subscriptions {
		ids = append(ids, subID)
	}
	sort.Strings(ids)
	subscriptions := make([]trackedSubscription, 0, len(ids))
	for _, subID := range ids {
		subscriptions = append(subscriptions, manager.subscriptions[subID])
	}
	failedHandler := manager.failedResubscribeHandler
	manager.lock.Unlock()

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
