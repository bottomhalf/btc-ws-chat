package hub

import (
	"fmt"
	"log"
	"runtime/debug"
	"time"
)

// HandlerFunc is the function signature for processing events through the pipeline.
type HandlerFunc func(ctx *EventContext) error

// Middleware wraps a HandlerFunc to add cross-cutting behavior.
type Middleware func(next HandlerFunc) HandlerFunc

// RecoveryMiddleware prevents panics from killing worker goroutines.
// Any panic in a handler is caught, logged with a full stack trace,
// and converted to an error return.
func RecoveryMiddleware() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx *EventContext) (err error) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("PANIC recovered in handler for event %s (client: %s, user: %s): %v\nStack: %s",
						ctx.Event.Event, ctx.Client.ID, ctx.Client.userId, r, string(debug.Stack()))
					err = fmt.Errorf("handler panic: %v", r)
				}
			}()
			return next(ctx)
		}
	}
}

// LoggingMiddleware logs event processing duration for slow and failed events.
// Successful fast events are not logged to reduce noise — handlers retain
// their own domain-specific log lines.
func LoggingMiddleware() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx *EventContext) error {
			start := time.Now()

			err := next(ctx)

			duration := time.Since(start)
			if err != nil {
				log.Printf("[EVENT] %s | client=%s user=%s | %v | error: %v",
					ctx.Event.Event, ctx.Client.ID, ctx.Client.userId, duration, err)
			} else if duration > 100*time.Millisecond {
				// Only log slow successful events to avoid noise
				log.Printf("[EVENT] %s | client=%s user=%s | %v (slow)",
					ctx.Event.Event, ctx.Client.ID, ctx.Client.userId, duration)
			}

			return err
		}
	}
}
