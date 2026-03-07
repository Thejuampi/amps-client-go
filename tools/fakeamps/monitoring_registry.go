package main

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type monitoredClient struct {
	ID         string    `json:"id"`
	RemoteAddr string    `json:"remote_addr"`
	Protocol   string    `json:"protocol"`
	Transport  string    `json:"transport"`
	UserID     string    `json:"user_id"`
	ClientName string    `json:"client_name"`
	Connected  time.Time `json:"connected"`
}

type monitoredClientEntry struct {
	descriptor monitoredClient
	conn       net.Conn
	stats      *connStats
}

type monitoredClientRegistry struct {
	mu      sync.RWMutex
	nextID  atomic.Uint64
	clients map[string]*monitoredClientEntry
}

type monitoredTransportRegistry struct {
	mu         sync.RWMutex
	transports map[string]transportDescriptor
}

var monitoringClients = &monitoredClientRegistry{
	clients: make(map[string]*monitoredClientEntry),
}

var monitoringTransports = &monitoredTransportRegistry{
	transports: make(map[string]transportDescriptor),
}

func (registry *monitoredClientRegistry) Register(conn net.Conn, remoteAddr string, protocol string, transport string, stats *connStats) string {
	if registry == nil {
		return ""
	}

	var id = "client-" + formatUint(registry.nextID.Add(1))
	registry.mu.Lock()
	registry.clients[id] = &monitoredClientEntry{
		descriptor: monitoredClient{
			ID:         id,
			RemoteAddr: remoteAddr,
			Protocol:   protocol,
			Transport:  transport,
			Connected:  time.Now().UTC(),
		},
		conn:  conn,
		stats: stats,
	}
	registry.mu.Unlock()
	return id
}

func (registry *monitoredClientRegistry) UpdateLogon(id string, userID string, clientName string) {
	if registry == nil || id == "" {
		return
	}

	registry.mu.Lock()
	if entry := registry.clients[id]; entry != nil {
		entry.descriptor.UserID = userID
		entry.descriptor.ClientName = clientName
	}
	registry.mu.Unlock()
}

func (registry *monitoredClientRegistry) Remove(id string) {
	if registry == nil || id == "" {
		return
	}

	registry.mu.Lock()
	delete(registry.clients, id)
	registry.mu.Unlock()
}

func (registry *monitoredClientRegistry) List() []monitoredClient {
	if registry == nil {
		return nil
	}

	registry.mu.RLock()
	defer registry.mu.RUnlock()

	var out = make([]monitoredClient, 0, len(registry.clients))
	for _, entry := range registry.clients {
		out = append(out, entry.descriptor)
	}
	return out
}

func (registry *monitoredClientRegistry) Disconnect(id string) bool {
	if registry == nil || id == "" {
		return false
	}

	registry.mu.Lock()
	var entry = registry.clients[id]
	delete(registry.clients, id)
	registry.mu.Unlock()
	if entry == nil || entry.conn == nil {
		return false
	}
	_ = entry.conn.Close()
	return true
}

func (registry *monitoredClientRegistry) Reset() {
	if registry == nil {
		return
	}

	registry.mu.Lock()
	registry.clients = make(map[string]*monitoredClientEntry)
	registry.nextID.Store(0)
	registry.mu.Unlock()
}

func (registry *monitoredTransportRegistry) Reset(transports []transportDescriptor) {
	if registry == nil {
		return
	}

	registry.mu.Lock()
	registry.transports = make(map[string]transportDescriptor, len(transports))
	for _, transport := range transports {
		registry.transports[transport.ID] = transport
	}
	registry.mu.Unlock()
}

func (registry *monitoredTransportRegistry) List() []transportDescriptor {
	if registry == nil {
		return nil
	}

	registry.mu.RLock()
	defer registry.mu.RUnlock()

	var out = make([]transportDescriptor, 0, len(registry.transports))
	for _, transport := range registry.transports {
		out = append(out, transport)
	}
	return out
}

func (registry *monitoredTransportRegistry) SetEnabled(id string, enabled bool) bool {
	if registry == nil {
		return false
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	var transport, ok = registry.transports[id]
	if !ok {
		for currentID, current := range registry.transports {
			if current.Name == id {
				transport = current
				id = currentID
				ok = true
				break
			}
		}
	}
	if !ok {
		return false
	}

	transport.Enabled = enabled
	registry.transports[id] = transport
	return true
}

func (registry *monitoredTransportRegistry) Enabled(id string) bool {
	if registry == nil {
		return true
	}

	registry.mu.RLock()
	defer registry.mu.RUnlock()

	if transport, ok := registry.transports[id]; ok {
		return transport.Enabled
	}
	for _, transport := range registry.transports {
		if transport.Name == id {
			return transport.Enabled
		}
	}
	return true
}

func (registry *monitoredTransportRegistry) Exists(id string) bool {
	if registry == nil {
		return false
	}

	registry.mu.RLock()
	defer registry.mu.RUnlock()

	if _, ok := registry.transports[id]; ok {
		return true
	}
	for _, transport := range registry.transports {
		if transport.Name == id {
			return true
		}
	}
	return false
}

func formatUint(value uint64) string {
	return strconv.FormatUint(value, 10)
}
