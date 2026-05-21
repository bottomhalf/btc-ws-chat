package hub

import (
	"Confeet/internal/event"
	"Confeet/internal/model"
	"Confeet/internal/repo"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// SendMessageHandler processes EventSendMessage events.
type SendMessageHandler struct {
	messageRepo      repo.MessageRepository
	conversationRepo repo.ConversationRepository
}

// NewSendMessageHandler creates a handler with the required repository dependencies.
func NewSendMessageHandler(msgRepo repo.MessageRepository, convRepo repo.ConversationRepository) *SendMessageHandler {
	return &SendMessageHandler{
		messageRepo:      msgRepo,
		conversationRepo: convRepo,
	}
}

func (h *SendMessageHandler) EventType() string {
	return event.EventSendMessage
}

func (h *SendMessageHandler) Handle(ctx *EventContext) error {
	var message model.Message
	if err := json.Unmarshal(ctx.Event.Payload, &message); err != nil {
		log.Printf("failed to unmarshal client message: %v", err)
		ctx.SendError("invalid_message", "Failed to parse message")
		return fmt.Errorf("unmarshal message: %w", err)
	}

	// Get conversation ID from the message
	conversationID := message.ConversationID.Hex()
	if conversationID == "" || conversationID == "000000000000000000000000" {
		ctx.SendError("invalid_message", "ConversationID is required")
		return fmt.Errorf("invalid conversation ID: %s", conversationID)
	}

	log.Printf("New message from %s in conversation %s: %s\n", message.SenderID, conversationID, message.Body)
	message.Status = model.MESSAGE_SENT

	// Save message to MongoDB before publishing
	dbCtx, cancel := ctx.TimeoutContext(5 * time.Second)
	insertedID, err := h.messageRepo.InsertMessage(dbCtx, &message)
	cancel()

	if err != nil {
		log.Printf("failed to save message to MongoDB: %v", err)
		ctx.SendError("save_failed", "Failed to save message, please retry")
		return fmt.Errorf("insert message: %w", err)
	}

	id, err := primitive.ObjectIDFromHex(insertedID)
	if err != nil {
		log.Printf("failed to convert inserted ID to ObjectID: %v", err)
		ctx.SendError("save_failed", "Failed to save message, please retry")
		return fmt.Errorf("parse inserted ID: %w", err)
	}

	message.ID = id
	lastMessage := &model.LastMessage{
		MessageId: insertedID,
		Content:   message.Body,
		SenderId:  message.SenderID,
		SentAt:    time.Now(),
	}

	// Update last message on the conversation
	dbCtx, cancel = ctx.TimeoutContext(5 * time.Second)
	err = h.conversationRepo.UpdateLastMessage(dbCtx, message.ConversationID.Hex(), lastMessage)
	cancel()

	if err != nil {
		log.Printf("failed to save message to MongoDB: %v", err)
		ctx.SendError("save_failed", "Failed to save message, please retry")
		return fmt.Errorf("update last message: %w", err)
	}

	log.Printf("Message saved to MongoDB with ID: %s", insertedID)

	outEvent := ctx.Event
	outEvent.Payload, _ = json.Marshal(message)
	outEvent.Event = event.EventNewMessage
	ctx.PublishToRoom(outEvent, conversationID)

	return nil
}
