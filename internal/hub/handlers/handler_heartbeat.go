package handlers

import (
	"Confeet/internal/event"
	"Confeet/internal/hub"
	"Confeet/internal/model"
	"encoding/json"
	"fmt"
	"log"
)

// HeartbeatHandler processes HeartBeat events.
type HeartbeatHandler struct{}

// NewHeartbeatHandler creates a heartbeat handler (no dependencies).
func NewHeartbeatHandler() *HeartbeatHandler {
	return &HeartbeatHandler{}
}

func (h *HeartbeatHandler) EventType() string {
	return event.HeartBeat
}

func (h *HeartbeatHandler) Handle(ctx *hub.EventContext) error {
	// Client is proving it's alive - update lastSeen timestamp
	var ping model.PingIndicator
	if err := json.Unmarshal(ctx.Event.Payload, &ping); err != nil {
		log.Printf("failed to unmarshal ping indicator: %v", err)
		return fmt.Errorf("unmarshal heartbeat: %w", err)
	}
	log.Printf("Ping from: %s", ping.UserID)
	ctx.Client.UpdateLastSeen()
	// No response needed - this is a one-way heartbeat

	return nil
}
