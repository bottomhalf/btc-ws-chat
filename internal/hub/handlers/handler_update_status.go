package handlers

import (
	"Confeet/internal/event"
	"Confeet/internal/hub"
	"Confeet/internal/model"
	"Confeet/internal/repo"
	"encoding/json"
	"fmt"
	"log"
)

// UpdateStatusHandler processes EventUpdateStatus events.
type UpdateStatusHandler struct {
	userRepository repo.UserRepository
}

// NewUpdateStatusHandler creates a handler with the required user repository dependency.
func NewUpdateStatusHandler(userRepo repo.UserRepository) *UpdateStatusHandler {
	return &UpdateStatusHandler{userRepository: userRepo}
}

func (h *UpdateStatusHandler) EventType() string {
	return event.EventUpdateStatus
}

func (h *UpdateStatusHandler) Handle(ctx *hub.EventContext) error {
	var statusUpdate model.UserStatus
	if err := json.Unmarshal(ctx.Event.Payload, &statusUpdate); err != nil {
		log.Printf("failed to unmarshal user status update: %v", err)
		return fmt.Errorf("unmarshal status update: %w", err)
	}

	if statusUpdate.UserID == "" {
		return nil
	}

	// Update the client struct's status in memory
	if ctx.Client != nil {
		ctx.Client.SetStatus(statusUpdate.Status)
	}

	log.Printf("User %s changed status to %s\n", statusUpdate.UserID, statusUpdate.Status)

	// 1. Update status in DB
	if err := h.userRepository.UpdateUserStatus(statusUpdate.UserID, statusUpdate.Status); err != nil {
		log.Printf("failed to update user status in DB: %v", err)
	}

	// 2. Broadcast the status update to all online clients
	outEvent := ctx.Event
	outEvent.Event = event.EventUserStatus
	ctx.BroadcastToAll(outEvent)

	return nil
}
