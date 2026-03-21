package amps

import "sync"

// MessageRouter stores exported state used by AMPS client APIs.
type MessageRouter struct {
	lock   sync.Mutex
	routes map[string]MessageRoute
	key    string
}

// MessageRoute stores exported state used by AMPS client APIs.
type MessageRoute struct {
	messageHandler func(*Message) error
	systemAcks     int
	requestedAcks  int
	terminationAck int
	commandType    int
}

func isSubscribeCommandType(commandType int) bool {
	switch commandType {
	case CommandSubscribe, CommandDeltaSubscribe, CommandSOWAndSubscribe, CommandSOWAndDeltaSubscribe:
		return true
	default:
		return false
	}
}

func isSubscribeOrSOWCommandType(commandType int) bool {
	return isSubscribeCommandType(commandType) || commandType == CommandSOW
}

func terminationAckForRoute(requestedAcks int, systemAcks int, commandType int) int {
	if !isSubscribeOrSOWCommandType(commandType) {
		var terminationAck = 1
		bitCounter := (requestedAcks | systemAcks) >> 1
		for bitCounter > 0 {
			bitCounter >>= 1
			terminationAck <<= 1
		}
		return terminationAck
	}

	if commandType == CommandSOW {
		if requestedAcks >= AckTypeCompleted {
			var terminationAck = 1
			bitCounter := (requestedAcks | systemAcks) >> 1
			for bitCounter > 0 {
				bitCounter >>= 1
				terminationAck <<= 1
			}
			return terminationAck
		}
		return AckTypeCompleted
	}

	return AckTypeNone
}

func (msgRouter *MessageRouter) ensureRoutes() {
	if msgRouter != nil && msgRouter.routes == nil {
		msgRouter.routes = make(map[string]MessageRoute)
	}
}

func (msgRoute *MessageRoute) messageRoute(
	messageHandler func(message *Message) error,
	requestedAcks int,
	systemAcks int,
	commandType int,
) {
	msgRoute.messageHandler = messageHandler
	msgRoute.requestedAcks = requestedAcks
	msgRoute.systemAcks = systemAcks
	msgRoute.commandType = commandType
	msgRoute.terminationAck = terminationAckForRoute(requestedAcks, systemAcks, commandType)
}

func (msgRoute *MessageRoute) deliverAck(message *Message, ackType int) int {
	if msgRoute.requestedAcks&ackType == 0 {
		return 0
	}
	if msgRoute.messageHandler != nil {
		_ = msgRoute.messageHandler(message)
	}
	return 1
}

func (msgRoute *MessageRoute) isTerminationAck(ackType int) bool {
	return ackType == msgRoute.terminationAck
}

func (msgRoute *MessageRoute) deliverData(message *Message) int {
	if msgRoute.messageHandler != nil {
		_ = msgRoute.messageHandler(message)
	}
	return 1
}

func (msgRoute *MessageRoute) getMessageHandler() (message func(*Message) error) {
	return msgRoute.messageHandler
}

func (msgRouter *MessageRouter) deliverAckForKey(ackMessage *Message, ackType int, routeID string) int {
	if msgRouter == nil {
		return 0
	}

	msgRouter.lock.Lock()
	if msgRouter.routes == nil {
		msgRouter.lock.Unlock()
		return 0
	}
	route, exists := msgRouter.routes[routeID]
	msgRouter.lock.Unlock()
	if !exists {
		return 0
	}

	var messagesDelivered = route.deliverAck(ackMessage, ackType)
	if route.isTerminationAck(ackType) {
		msgRouter.lock.Lock()
		if current, ok := msgRouter.routes[routeID]; ok && current.isTerminationAck(ackType) {
			delete(msgRouter.routes, routeID)
			messagesDelivered++
		}
		msgRouter.lock.Unlock()
	}

	return messagesDelivered
}

func (msgRouter *MessageRouter) processAckForRemoval(ackType int, commandID string) {
	if msgRouter == nil {
		return
	}
	msgRouter.lock.Lock()
	defer msgRouter.lock.Unlock()
	if msgRouter.routes == nil {
		return
	}
	route, exists := msgRouter.routes[commandID]
	if exists && route.isTerminationAck(ackType) {
		delete(msgRouter.routes, commandID)
	}
}

// AddRoute adds route behavior on the receiver.
func (msgRouter *MessageRouter) AddRoute(
	commandID string,
	messageHandler func(*Message) error,
	requestedAcks int,
	systemAcks int,
	commandType int,
) int {
	if msgRouter == nil {
		return 0
	}

	msgRouter.lock.Lock()
	defer msgRouter.lock.Unlock()
	msgRouter.ensureRoutes()

	if existingRoute, exists := msgRouter.routes[commandID]; exists {
		if isSubscribeCommandType(commandType) && !existingRoute.isTerminationAck(0) {
			var route MessageRoute
			route.messageRoute(messageHandler, requestedAcks, systemAcks, commandType)
			msgRouter.routes[commandID] = route
			return 1
		}
		return 0
	}

	var route MessageRoute
	route.messageRoute(messageHandler, requestedAcks, systemAcks, commandType)
	msgRouter.routes[commandID] = route
	return 1
}

// RemoveRoute removes previously registered route behavior.
func (msgRouter *MessageRouter) RemoveRoute(commandID string) bool {
	if msgRouter == nil {
		return false
	}
	msgRouter.lock.Lock()
	defer msgRouter.lock.Unlock()
	if msgRouter.routes == nil {
		return false
	}
	_, exists := msgRouter.routes[commandID]
	if exists {
		delete(msgRouter.routes, commandID)
	}
	return exists
}

// FindRoute executes the exported findroute operation.
func (msgRouter *MessageRouter) FindRoute(commandID string) func(*Message) error {
	if msgRouter == nil {
		return nil
	}
	msgRouter.lock.Lock()
	defer msgRouter.lock.Unlock()
	if msgRouter.routes == nil {
		return nil
	}
	route, exists := msgRouter.routes[commandID]
	if !exists {
		return nil
	}
	if handler := route.getMessageHandler(); handler != nil {
		return handler
	}
	return nil
}

// UnsubscribeAll executes the exported unsubscribeall operation.
func (msgRouter *MessageRouter) UnsubscribeAll() {
	if msgRouter == nil {
		return
	}
	msgRouter.lock.Lock()
	defer msgRouter.lock.Unlock()
	msgRouter.ensureRoutes()
	for routeID, route := range msgRouter.routes {
		if route.isTerminationAck(0) {
			delete(msgRouter.routes, routeID)
		}
	}
}

// Clear executes the exported clear operation.
func (msgRouter *MessageRouter) Clear() {
	if msgRouter == nil {
		return
	}
	msgRouter.lock.Lock()
	msgRouter.routes = make(map[string]MessageRoute)
	msgRouter.lock.Unlock()
}

// DeliverAck executes the exported deliverack operation.
func (msgRouter *MessageRouter) DeliverAck(ackMessage *Message, ackType int) int {
	if msgRouter == nil || ackMessage == nil {
		return 0
	}
	messagesDelivered := 0
	msgCmdID, _ := ackMessage.CommandID()
	msgQueryID, _ := ackMessage.QueryID()
	msgSubID, _ := ackMessage.SubID()
	if msgCmdID == msgRouter.key {
		messagesDelivered += msgRouter.deliverAckForKey(ackMessage, ackType, msgRouter.key)
	}
	if msgQueryID == msgRouter.key {
		if messagesDelivered == 0 {
			messagesDelivered += msgRouter.deliverAckForKey(ackMessage, ackType, msgRouter.key)
		} else {
			msgRouter.processAckForRemoval(ackType, msgRouter.key)
		}
	}
	if msgSubID == msgRouter.key {
		if messagesDelivered == 0 {
			messagesDelivered += msgRouter.deliverAckForKey(ackMessage, ackType, msgRouter.key)
		} else {
			msgRouter.processAckForRemoval(ackType, msgRouter.key)
		}
	}
	return messagesDelivered
}

// DeliverData executes the exported deliverdata operation.
func (msgRouter *MessageRouter) DeliverData(dataMessage *Message) int {
	messagesDelivered := 0
	msgCommandID, _ := dataMessage.CommandID()
	msgQueryID, _ := dataMessage.QueryID()
	msgSubID, _ := dataMessage.SubID()
	if messagesDelivered == 0 && msgRouter.key == msgQueryID {
		messagesDelivered += msgRouter.DeliverDataWithID(dataMessage, msgRouter.key)
	}
	if messagesDelivered == 0 && msgRouter.key == msgCommandID {
		messagesDelivered += msgRouter.DeliverDataWithID(dataMessage, msgRouter.key)
	}
	if messagesDelivered == 0 && msgRouter.key == msgSubID {
		messagesDelivered += msgRouter.DeliverDataWithID(dataMessage, msgRouter.key)
	}
	return messagesDelivered
}

// DeliverDataWithID executes the exported deliverdatawithid operation.
func (msgRouter *MessageRouter) DeliverDataWithID(dataMessage *Message, cmdID string) int {
	if msgRouter == nil {
		return 0
	}
	msgRouter.lock.Lock()
	route, exists := msgRouter.routes[cmdID]
	msgRouter.lock.Unlock()
	if exists {
		return route.deliverData(dataMessage)
	}
	return 0
}
