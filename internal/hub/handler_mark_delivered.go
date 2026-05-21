package hub

import (
	"Confeet/internal/event"
	"Confeet/internal/model"
	"Confeet/internal/repo"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// MarkDeliveredHandler processes EventMarkDelivered events.
type MarkDeliveredHandler struct {
	conversationRepo repo.ConversationRepository
}

// NewMarkDeliveredHandler creates a handler with the required repository dependency.
func NewMarkDeliveredHandler(convRepo repo.ConversationRepository) *MarkDeliveredHandler {
	return &MarkDeliveredHandler{conversationRepo: convRepo}
}

func (h *MarkDeliveredHandler) EventType() string {
	return event.EventMarkDelivered
}

func (h *MarkDeliveredHandler) Handle(ctx *EventContext) error {
	var deliver model.MessageDelivered
	if err := json.Unmarshal(ctx.Event.Payload, &deliver); err != nil {
		log.Printf("failed to unmarshal delivery indicator: %v", err)
		return fmt.Errorf("unmarshal delivery: %w", err)
	}

	if deliver.ConversationID == "" {
		return nil
	}

	deliver.Status = model.MESSAGE_DELIVERED

	dbCtx, cancel := ctx.TimeoutContext(5 * time.Second)
	err := h.conversationRepo.UpdateMessageDelivery(dbCtx, deliver)
	cancel()

	if err != nil {
		log.Printf("failed to save message to MongoDB: %v", err)
		ctx.SendError("save_failed", "Failed to save message, please retry")
		return fmt.Errorf("update delivery: %w", err)
	}

	log.Printf("Message %s is delivered in conversation %s\n", deliver.Id, deliver.ConversationID)

	outEvent := ctx.Event
	outEvent.Event = event.EventDelivered
	ctx.PublishToRoom(outEvent, deliver.ConversationID)

	return nil
}
