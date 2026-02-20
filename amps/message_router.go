package amps

import "sync"


// MessageRouter struct
type MessageRouter struct {
	routes *sync.Map
	key string
	MessageRoute
	client *Client
}


// Internal API

// MessageRoute struct
type MessageRoute struct {
	messageHandler func(*Message) error
	systemAcks int
	requestedAcks int
	terminationAck int
}

func (msgRoute *MessageRoute) messageRoute (
	messageHandler func(message *Message) error,
	requestedAcks int,
	systemAcks int,
	isSubscribe bool,
	isReplace bool,
) (func(*Message) error) {
	msgRoute.messageHandler = messageHandler
	msgRoute.requestedAcks = requestedAcks
	msgRoute.systemAcks = systemAcks

	// auto-remove handler when termination ack comes in
	if !isSubscribe {
		bitCounter := requestedAcks | systemAcks
		for bitCounter > 0 {
			bitCounter >>= 1
			if msgRoute.terminationAck > 0 {
				msgRoute.terminationAck = 2 * msgRoute.terminationAck
			} else {
				msgRoute.terminationAck = 1
			}
		}
	}
	return msgRoute.messageHandler
}

func (msgRoute *MessageRoute) deliverAck(message *Message, ackType int) int {
	if msgRoute.requestedAcks & ackType == 0 { return 0 }
	err := msgRoute.messageHandler(message)
	if err != nil { err = NewError(MessageHandlerError, err) }
	return 1
}

func (msgRoute *MessageRoute) isTerminationAck(ackType int) bool {
	return ackType == msgRoute.terminationAck
}

func (msgRoute *MessageRoute) deliverData(message *Message) int {
	err := msgRoute.messageHandler(message)
	if err != nil { err = NewError(MessageHandlerError, err) }
	return 1
}

func (msgRoute *MessageRoute) getMessageHandler() (message func(*Message) error) {
	return msgRoute.messageHandler
}

func (msgRouter *MessageRouter) deliverAck(ackMessage *Message, ackType int) int {
	messagesDelivered := 0
	commandID, _ := ackMessage.CommandID()
	route, _ := msgRouter.routes.Load(commandID)
	if route != nil {
		messagesDelivered += msgRouter.MessageRoute.deliverAck(ackMessage, ackType)
		if msgRouter.MessageRoute.isTerminationAck(ackType) {
			msgRouter.routes.Delete(commandID)
		}
	}
	return messagesDelivered
}

func (msgRouter *MessageRouter) processAckForRemoval(ackType int, commandID string) {
	route := msgRouter.MessageRoute.messageHandler
	msgStream, _ := msgRouter.routes.Load(commandID)
	route = msgStream.(func(*Message) error)
	if route != nil && msgRouter.MessageRoute.isTerminationAck(ackType) {
		msgRouter.routes.Delete(commandID)
	}
}


// Public API

// AddRoute ...
func (msgRouter *MessageRouter) AddRoute(
	commandID string,
	messageHandler func(*Message) error,
	requestedAcks int,
	systemAcks int,
	isSubscribe bool,
	isReplace bool,
) {
	msgRouter.routes.Store(commandID, msgRouter.MessageRoute.messageRoute(
		messageHandler,
		requestedAcks,
		systemAcks,
		isSubscribe,
		isReplace,
	))
}

// RemoveRoute ...
func (msgRouter *MessageRouter) RemoveRoute(commandID string) {
	msgRouter.routes.Delete(commandID)
}

// FindRoute ... 
func (msgRouter *MessageRouter) FindRoute(commandID string) (func(*Message) error) {
	route := msgRouter.MessageRoute.messageHandler
	msgHandler, _ := msgRouter.routes.Load(commandID)
	route = msgHandler.(func(*Message) error)
	if route != nil {
		return msgRouter.MessageRoute.getMessageHandler()
	}
	return nil
}

// UnsubscribeAll ...
func (msgRouter *MessageRouter) UnsubscribeAll() {
	msgRouter.routes.Range(func(key interface{}, ms interface{}) bool {
		msgRouter.routes.Delete(key.(string))
		return true
	})
	msgRouter.routes = new(sync.Map)
}

// Clear ...
func (msgRouter *MessageRouter) Clear() {
	msgRouter.routes = new(sync.Map)
}

// DeliverAck ...
func (msgRouter *MessageRouter) DeliverAck(ackMessage *Message, ackType int) int {
	messagesDelivered := 0
	msgCmdID, _ := ackMessage.CommandID()
	msgQueryID, _ := ackMessage.QueryID()
	msgSubID, _ := ackMessage.SubID()
	if msgCmdID == msgRouter.key {
		messagesDelivered += msgRouter.deliverAck(ackMessage, ackType)
	}
	if msgQueryID == msgRouter.key {
		if messagesDelivered == 0 {
			messagesDelivered += msgRouter.deliverAck(ackMessage, ackType)
		} else {
			msgRouter.processAckForRemoval(ackType, msgRouter.key)
		}
	}
	if msgSubID == msgRouter.key {
		if messagesDelivered == 0 {
			messagesDelivered += msgRouter.deliverAck(ackMessage, ackType)
		} else {
			msgRouter.processAckForRemoval(ackType, msgRouter.key)
		}
	}
	return messagesDelivered
}

// DeliverData ...
func (msgRouter *MessageRouter) DeliverData(dataMessage *Message) int {
	messagesDelivered := 0
	msgCommandID, _ := dataMessage.CommandID()
	msgQueryID, _ := dataMessage.QueryID()
	msgSubID, _ := dataMessage.SubID()
	if messagesDelivered == 0 && msgRouter.key == msgQueryID {
		messagesDelivered += msgRouter.DeliverDataWithID(dataMessage, msgRouter.key)
	}
	if msgRouter.key == msgCommandID {
		messagesDelivered += msgRouter.DeliverDataWithID(dataMessage, msgRouter.key)
	}
	if messagesDelivered == 0 && msgRouter.key == msgSubID {
		messagesDelivered += msgRouter.DeliverDataWithID(dataMessage, msgRouter.key)
	}
	return messagesDelivered
}

// DeliverDataWithID ...
func (msgRouter *MessageRouter) DeliverDataWithID(dataMessage *Message, cmdID string) int {
	messagesDelivered := 0
	route, _ := msgRouter.routes.Load(cmdID)
	if route != nil {
		messagesDelivered += msgRouter.deliverData(dataMessage)
	}
	return messagesDelivered
}

