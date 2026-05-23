package handlers

import (
	"Confeet/internal/event"
	"Confeet/internal/hub"
	"Confeet/internal/model"
	"encoding/json"
	"fmt"
	"log"
)

// TypingHandler processes EventTyping events.
type TypingHandler struct{}

// NewTypingHandler creates a typing indicator handler (no dependencies).
func NewTypingHandler() *TypingHandler {
	return &TypingHandler{}
}

func (h *TypingHandler) EventType() string {
	return event.EventTyping
}

func (h *TypingHandler) Handle(ctx *hub.EventContext) error {
	var typing model.TypingIndicator
	if err := json.Unmarshal(ctx.Event.Payload, &typing); err != nil {
		log.Printf("failed to unmarshal typing indicator: %v", err)
		return fmt.Errorf("unmarshal typing: %w", err)
	}

	if typing.ConversationID == "" {
		return nil
	}

	log.Printf("User %s is typing in conversation %s\n", typing.UserID, typing.ConversationID)

	outEvent := ctx.Event
	outEvent.Event = event.EventTyping
	ctx.PublishToRoom(outEvent, typing.ConversationID)

	return nil
}
