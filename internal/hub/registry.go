package hub

import (
	"fmt"
	"log"
	"sync"
)

// HandlerRegistry maps event types to handlers and applies middleware.
type HandlerRegistry struct {
	handlers   map[string]EventHandler
	middleware []Middleware
	mu         sync.RWMutex
}

// NewHandlerRegistry creates an empty handler registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]EventHandler),
	}
}

// Register adds a handler for a specific event type.
// Panics on duplicate registration — this is caught at startup, not runtime.
func (r *HandlerRegistry) Register(h EventHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	eventType := h.EventType()
	if _, exists := r.handlers[eventType]; exists {
		panic(fmt.Sprintf("duplicate handler registered for event: %s", eventType))
	}
	r.handlers[eventType] = h
	log.Printf("registered handler for event: %s", eventType)
}

// Use adds middleware to the processing pipeline.
// Middleware is applied in the order added (first added = outermost wrapper).
func (r *HandlerRegistry) Use(mw ...Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.middleware = append(r.middleware, mw...)
}

// Dispatch routes an event through the middleware chain to the appropriate handler.
// Returns an error if no handler is registered or if the handler returns an error.
func (r *HandlerRegistry) Dispatch(ctx *EventContext) error {
	r.mu.RLock()
	handler, exists := r.handlers[ctx.Event.Event]
	middleware := make([]Middleware, len(r.middleware))
	copy(middleware, r.middleware)
	r.mu.RUnlock()

	if !exists {
		log.Printf("unknown event type: %s", ctx.Event.Event)
		return fmt.Errorf("unknown event: %s", ctx.Event.Event)
	}

	// Build the final handler function
	final := HandlerFunc(func(c *EventContext) error {
		return handler.Handle(c)
	})

	// Apply middleware in reverse order so the first-added middleware
	// is the outermost wrapper (executes first).
	for i := len(middleware) - 1; i >= 0; i-- {
		final = middleware[i](final)
	}

	return final(ctx)
}
