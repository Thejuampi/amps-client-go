package amps

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var defaultResubscriptionTimeout atomic.Int64

// DefaultSubscriptionManager stores exported state used by AMPS client APIs.
type DefaultSubscriptionManager struct {
	lock                     sync.RWMutex
	subscriptions            map[string]trackedSubscription
	resumed                  map[string]*trackedResumeGroup
	resumedGroups            map[*trackedResumeGroup]struct{}
	failedResubscribeHandler FailedResubscribeHandler
	resubscriptionTimeout    int
}

type trackedResumeGroup struct {
	command           *Command
	requestedAckTypes int
	members           map[string]struct{}
}

// NewDefaultSubscriptionManager returns a new DefaultSubscriptionManager.
func NewDefaultSubscriptionManager() *DefaultSubscriptionManager {
	return &DefaultSubscriptionManager{
		subscriptions:         make(map[string]trackedSubscription),
		resumed:               make(map[string]*trackedResumeGroup),
		resumedGroups:         make(map[*trackedResumeGroup]struct{}),
		resubscriptionTimeout: GetDefaultResubscriptionTimeout(),
	}
}

// SetDefaultResubscriptionTimeout sets the default timeout in milliseconds for new subscription managers.
func SetDefaultResubscriptionTimeout(timeout int) int {
	if timeout >= 0 {
		defaultResubscriptionTimeout.Store(int64(timeout))
	}
	return int(defaultResubscriptionTimeout.Load())
}

// GetDefaultResubscriptionTimeout returns the default timeout in milliseconds for new subscription managers.
func GetDefaultResubscriptionTimeout() int {
	return SetDefaultResubscriptionTimeout(-1)
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

func removeReplayOnlyOptions(options string) string {
	if options == "" {
		return ""
	}

	var parts = strings.Split(options, ",")
	var kept = make([]string, 0, len(parts))
	for _, part := range parts {
		var token = strings.TrimSpace(part)
		if token == "" || token == "replace" {
			continue
		}
		kept = append(kept, token)
	}

	return strings.Join(kept, ",")
}

func splitSubscriptionIDs(subIDs string) []string {
	if subIDs == "" {
		return nil
	}

	var parts = strings.Split(subIDs, ",")
	var ids = make([]string, 0, len(parts))
	for _, part := range parts {
		var token = strings.TrimSpace(part)
		if token == "" {
			continue
		}
		ids = append(ids, token)
	}
	return ids
}

func hasCommandOption(options string, option string) bool {
	for _, part := range strings.Split(options, ",") {
		if strings.TrimSpace(part) == option {
			return true
		}
	}
	return false
}

func prependCommandOption(options string, option string) string {
	if hasCommandOption(options, option) {
		return options
	}
	if options == "" {
		return option
	}
	return option + "," + options
}

func parseBookmarkToken(bookmark string) (uint64, uint64, bool) {
	var parts = strings.Split(bookmark, "|")
	if len(parts) < 2 {
		return 0, 0, false
	}

	publisher, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	sequence, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return publisher, sequence, true
}

func combinedResumeBookmark(client *Client, group *trackedResumeGroup) string {
	if client == nil || group == nil {
		return ""
	}
	bookmarkStore := client.BookmarkStore()
	if bookmarkStore == nil {
		return ""
	}

	var publishers = make(map[uint64]uint64)
	var memberIDs = make([]string, 0, len(group.members))
	for memberID := range group.members {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)

	for _, memberID := range memberIDs {
		var recent = bookmarkStore.GetMostRecent(memberID)
		for _, token := range splitBookmarkList(recent) {
			publisher, sequence, ok := parseBookmarkToken(token)
			if !ok {
				continue
			}
			if current, exists := publishers[publisher]; !exists || current > sequence {
				publishers[publisher] = sequence
			}
		}
	}

	if len(publishers) == 0 {
		return ""
	}

	var publisherIDs = make([]uint64, 0, len(publishers))
	for publisherID := range publishers {
		if publisherID == 0 && publishers[publisherID] == 0 {
			continue
		}
		publisherIDs = append(publisherIDs, publisherID)
	}
	sort.Slice(publisherIDs, func(left int, right int) bool {
		return publisherIDs[left] < publisherIDs[right]
	})

	var tokens = make([]string, 0, len(publisherIDs))
	for _, publisherID := range publisherIDs {
		tokens = append(tokens, fmt.Sprintf("%d|%d|", publisherID, publishers[publisherID]))
	}
	return strings.Join(tokens, ",")
}

func splitBookmarkList(bookmarks string) []string {
	if bookmarks == "" {
		return nil
	}
	var parts = strings.Split(bookmarks, ",")
	var result = make([]string, 0, len(parts))
	for _, part := range parts {
		var token = strings.TrimSpace(part)
		if token == "" {
			continue
		}
		result = append(result, token)
	}
	return result
}

func cloneCommandWithSubID(command *Command, subID string) *Command {
	var cloned = cloneCommand(command)
	cloned.SetSubID(subID)
	return cloned
}

func (manager *DefaultSubscriptionManager) removeResumedSubID(subID string) {
	group, exists := manager.resumed[subID]
	if !exists || group == nil {
		return
	}
	delete(manager.resumed, subID)
	delete(group.members, subID)
	if len(group.members) == 0 {
		delete(manager.resumedGroups, group)
	}
}

func (manager *DefaultSubscriptionManager) removeResumeGroup(group *trackedResumeGroup) {
	if group == nil {
		return
	}
	delete(manager.resumedGroups, group)
	for memberID := range group.members {
		delete(manager.resumed, memberID)
	}
}

func (manager *DefaultSubscriptionManager) removeTrackedSubscription(subID string) {
	if subID == "" {
		return
	}
	delete(manager.subscriptions, subID)
	manager.removeResumedSubID(subID)
}

func invokeFailedResubscribeHandler(handler FailedResubscribeHandler, command *Command, requestedAckTypes int, err error) (handled bool, handlerErr error) {
	if handler == nil {
		return false, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			handlerErr = NewError(RetryOperationError, fmt.Sprintf("FailedResubscribeHandler panic: %v", recovered))
		}
	}()
	handled = handler.Failure(command, requestedAckTypes, err)
	return handled, nil
}

func (manager *DefaultSubscriptionManager) subscribeResume(command *Command, requestedAckTypes int) {
	var ids = splitSubscriptionIDs(trackedSubscriptionID(command))
	if len(ids) == 0 {
		return
	}

	var group = &trackedResumeGroup{
		command:           cloneCommand(command),
		requestedAckTypes: requestedAckTypes,
		members:           make(map[string]struct{}),
	}
	var saved bool
	for _, id := range ids {
		if _, exists := manager.resumed[id]; exists {
			continue
		}
		manager.resumed[id] = group
		group.members[id] = struct{}{}
		saved = true
	}
	if saved {
		manager.resumedGroups[group] = struct{}{}
	}
}

func (manager *DefaultSubscriptionManager) subscribePause(messageHandler func(*Message) error, command *Command, requestedAckTypes int, options string) {
	var ids = splitSubscriptionIDs(trackedSubscriptionID(command))
	if len(ids) == 0 {
		return
	}
	var replace = hasCommandOption(options, "replace")
	for _, id := range ids {
		manager.removeResumedSubID(id)

		existing, exists := manager.subscriptions[id]
		if exists && !replace {
			var existingOptions, _ = existing.command.Options()
			existing.command.SetOptions(prependCommandOption(existingOptions, "pause"))
			existing.paused = true
			manager.subscriptions[id] = existing
			continue
		}

		var handler = messageHandler
		if exists && existing.messageHandler != nil {
			handler = existing.messageHandler
		}
		manager.subscriptions[id] = trackedSubscription{
			messageHandler:    handler,
			command:           cloneCommandWithSubID(command, id),
			requestedAckTypes: requestedAckTypes,
			paused:            true,
		}
	}
}

func prepareResubscribeCommand(client *Client, subscription trackedSubscription) *Command {
	var command = cloneCommand(subscription.command)

	command.SetAckType(subscription.requestedAckTypes)

	if options, hasOptions := command.Options(); hasOptions {
		command.SetOptions(removeReplayOnlyOptions(options))
	}

	if _, hasBookmark := command.Bookmark(); hasBookmark && client != nil {
		if bookmarkStore := client.BookmarkStore(); bookmarkStore != nil {
			if bookmark := bookmarkStore.GetMostRecent(trackedSubscriptionID(command)); bookmark != "" {
				command.SetBookmark(bookmark)
			}
		}
	}

	return command
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

	var options, _ = command.Options()
	if hasCommandOption(options, "resume") {
		manager.lock.Lock()
		manager.subscribeResume(command, requestedAckTypes)
		manager.lock.Unlock()
		return
	}
	if hasCommandOption(options, "pause") {
		manager.lock.Lock()
		manager.subscribePause(messageHandler, command, requestedAckTypes, options)
		manager.lock.Unlock()
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
		manager.resumed = make(map[string]*trackedResumeGroup)
		manager.resumedGroups = make(map[*trackedResumeGroup]struct{})
		return
	}
	delete(manager.subscriptions, subID)
	manager.removeResumedSubID(subID)
}

// Clear executes the exported clear operation.
func (manager *DefaultSubscriptionManager) Clear() {
	if manager == nil {
		return
	}
	manager.lock.Lock()
	manager.subscriptions = make(map[string]trackedSubscription)
	manager.resumed = make(map[string]*trackedResumeGroup)
	manager.resumedGroups = make(map[*trackedResumeGroup]struct{})
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

// SetResubscriptionTimeout sets the timeout used for replayed resubscribe commands in milliseconds.
func (manager *DefaultSubscriptionManager) SetResubscriptionTimeout(timeout int) {
	if manager == nil || timeout < 0 {
		return
	}
	manager.lock.Lock()
	manager.resubscriptionTimeout = timeout
	manager.lock.Unlock()
}

// GetResubscriptionTimeout returns the timeout used for replayed resubscribe commands in milliseconds.
func (manager *DefaultSubscriptionManager) GetResubscriptionTimeout() int {
	if manager == nil {
		return 0
	}
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	return manager.resubscriptionTimeout
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

	manager.lock.RLock()
	var resumedGroups = make([]*trackedResumeGroup, 0, len(manager.resumedGroups))
	for group := range manager.resumedGroups {
		resumedGroups = append(resumedGroups, group)
	}
	manager.lock.RUnlock()
	sort.Slice(resumedGroups, func(left int, right int) bool {
		leftID, _ := resumedGroups[left].command.SubID()
		rightID, _ := resumedGroups[right].command.SubID()
		return leftID < rightID
	})

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

	resubscriptionTimeout := manager.GetResubscriptionTimeout()
	var removedSubscriptions []string
	var removedResumeGroups []*trackedResumeGroup

	for _, subscription := range subscriptions {
		command := prepareResubscribeCommand(client, subscription)
		command.SetTimeout(resubscriptionTimeout)
		if subscription.paused {
			manager.lock.RLock()
			group := manager.resumed[trackedSubscriptionID(command)]
			manager.lock.RUnlock()
			if bookmark := combinedResumeBookmark(client, group); bookmark != "" {
				command.SetBookmark(bookmark)
			}
		}
		subID := trackedSubscriptionID(command)
		if _, blocked := noResubscribe[subID]; blocked {
			continue
		}

		if _, err := client.ExecuteAsync(command, subscription.messageHandler); err != nil {
			if failedHandler != nil {
				handled, handlerErr := invokeFailedResubscribeHandler(failedHandler, command, subscription.requestedAckTypes, err)
				if handlerErr != nil {
					return handlerErr
				}
				if handled {
					removedSubscriptions = append(removedSubscriptions, subID)
					continue
				}
			}
			return err
		}

	}

	for _, group := range resumedGroups {
		var resumeCommand = prepareResubscribeCommand(client, trackedSubscription{
			command:           group.command,
			requestedAckTypes: group.requestedAckTypes,
		})
		resumeCommand.SetTimeout(resubscriptionTimeout)
		if _, err := client.ExecuteAsync(resumeCommand, nil); err != nil {
			if failedHandler != nil {
				handled, handlerErr := invokeFailedResubscribeHandler(failedHandler, resumeCommand, group.requestedAckTypes, err)
				if handlerErr != nil {
					return handlerErr
				}
				if handled {
					removedResumeGroups = append(removedResumeGroups, group)
					continue
				}
			}
			return err
		}
	}

	if len(removedSubscriptions) > 0 || len(removedResumeGroups) > 0 {
		manager.lock.Lock()
		for _, subID := range removedSubscriptions {
			manager.removeTrackedSubscription(subID)
		}
		for _, group := range removedResumeGroups {
			manager.removeResumeGroup(group)
		}
		manager.lock.Unlock()
	}

	return nil
}
