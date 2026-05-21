package hub

import (
	"Confeet/internal/event"
	"context"
	"time"
)

// EventHandler processes a specific WebSocket event type.
// Each handler is a self-contained unit with its own dependencies.
type EventHandler interface {
	// EventType returns the event string this handler processes.
	EventType() string

	// Handle processes the event. Returns an error for observability;
	// the error is NOT sent to the client (use ctx.SendError for that).
	Handle(ctx *EventContext) error
}

// EventContext carries everything a handler needs to process an event.
// It provides controlled access to Hub capabilities without exposing
// the Hub struct directly.
type EventContext struct {
	Event  event.WsEvent
	Client *Client
	hub    *Hub
}

// PublishToRoom sends an event to all online members of a conversation room.
func (ctx *EventContext) PublishToRoom(ev event.WsEvent, conversationID string) {
	ctx.hub.publishToRoom(ev, conversationID)
}

// BroadcastToAll sends an event to every online user connected to the hub.
func (ctx *EventContext) BroadcastToAll(ev event.WsEvent) {
	ctx.hub.broadcastToAll(ev)
}

// SendError sends a structured error event back to the client.
func (ctx *EventContext) SendError(code, message string) {
	ctx.hub.sendErrorToClient(ctx.Client, code, message)
}

// HubContext returns the hub's root context.
func (ctx *EventContext) HubContext() context.Context {
	return ctx.hub.ctx
}

// TimeoutContext creates a child context with timeout from the hub's root context.
func (ctx *EventContext) TimeoutContext(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx.hub.ctx, d)
}

// GetRoom retrieves a room from cache (or DB if not cached).
func (ctx *EventContext) GetRoom(conversationID string) *Room {
	return ctx.hub.GetRoom(conversationID)
}
