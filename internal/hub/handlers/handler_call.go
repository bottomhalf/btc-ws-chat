package handlers

import (
	"Confeet/internal/event"
	"Confeet/internal/hub"
)

// ---------------------------------------------------------------------------
// Call Initiate (event.EventCallInitiate)
// ---------------------------------------------------------------------------

// CallInitiateHandler processes call initiation requests.
type CallInitiateHandler struct {
	ch *hub.CallHandler
}

func NewCallInitiateHandler(ch *hub.CallHandler) *CallInitiateHandler {
	return &CallInitiateHandler{ch: ch}
}

func (h *CallInitiateHandler) EventType() string {
	return event.EventCallInitiate
}

func (h *CallInitiateHandler) Handle(ctx *hub.EventContext) error {
	h.ch.HandleCallInitiate(ctx.Event, ctx.Client, false)
	return nil
}

// ---------------------------------------------------------------------------
// Call Started / Group Join (event.EventCallStarted)
// ---------------------------------------------------------------------------

// CallStartedHandler processes group call started events.
// Delegates to HandleCallInitiate with isJoinRequest=true.
type CallStartedHandler struct {
	ch *hub.CallHandler
}

func NewCallStartedHandler(ch *hub.CallHandler) *CallStartedHandler {
	return &CallStartedHandler{ch: ch}
}

func (h *CallStartedHandler) EventType() string {
	return event.EventCallStarted
}

func (h *CallStartedHandler) Handle(ctx *hub.EventContext) error {
	h.ch.HandleCallInitiate(ctx.Event, ctx.Client, true)
	return nil
}

// ---------------------------------------------------------------------------
// Call Accept (event.EventCallAccept)
// ---------------------------------------------------------------------------

// CallAcceptHandler processes call acceptance events.
type CallAcceptHandler struct {
	ch *hub.CallHandler
}

func NewCallAcceptHandler(ch *hub.CallHandler) *CallAcceptHandler {
	return &CallAcceptHandler{ch: ch}
}

func (h *CallAcceptHandler) EventType() string {
	return event.EventCallAccept
}

func (h *CallAcceptHandler) Handle(ctx *hub.EventContext) error {
	h.ch.HandleCallAccept(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call Reject (event.EventCallReject)
// ---------------------------------------------------------------------------

// CallRejectHandler processes call rejection events.
type CallRejectHandler struct {
	ch *hub.CallHandler
}

func NewCallRejectHandler(ch *hub.CallHandler) *CallRejectHandler {
	return &CallRejectHandler{ch: ch}
}

func (h *CallRejectHandler) EventType() string {
	return event.EventCallReject
}

func (h *CallRejectHandler) Handle(ctx *hub.EventContext) error {
	h.ch.HandleCallReject(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call Dismiss (event.EventCallDismiss)
// ---------------------------------------------------------------------------

// CallDismissHandler processes call dismiss events.
type CallDismissHandler struct {
	ch *hub.CallHandler
}

func NewCallDismissHandler(ch *hub.CallHandler) *CallDismissHandler {
	return &CallDismissHandler{ch: ch}
}

func (h *CallDismissHandler) EventType() string {
	return event.EventCallDismiss
}

func (h *CallDismissHandler) Handle(ctx *hub.EventContext) error {
	h.ch.HandleCallDismiss(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call Cancel (event.EventCallCancel)
// ---------------------------------------------------------------------------

// CallCancelHandler processes call cancellation events from the caller.
type CallCancelHandler struct {
	ch *hub.CallHandler
}

func NewCallCancelHandler(ch *hub.CallHandler) *CallCancelHandler {
	return &CallCancelHandler{ch: ch}
}

func (h *CallCancelHandler) EventType() string {
	return event.EventCallCancel
}

func (h *CallCancelHandler) Handle(ctx *hub.EventContext) error {
	h.ch.HandleCallCancel(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call Timeout (event.EventCallTimeout)
// ---------------------------------------------------------------------------

// CallTimeoutHandler processes call timeout events from the callee.
type CallTimeoutHandler struct {
	ch *hub.CallHandler
}

func NewCallTimeoutHandler(ch *hub.CallHandler) *CallTimeoutHandler {
	return &CallTimeoutHandler{ch: ch}
}

func (h *CallTimeoutHandler) EventType() string {
	return event.EventCallTimeout
}

func (h *CallTimeoutHandler) Handle(ctx *hub.EventContext) error {
	h.ch.HandleCallTimeout(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call End (event.EventCallEnd)
// ---------------------------------------------------------------------------

// CallEndHandler processes call end events (participant leaving or ending).
type CallEndHandler struct {
	ch *hub.CallHandler
}

func NewCallEndHandler(ch *hub.CallHandler) *CallEndHandler {
	return &CallEndHandler{ch: ch}
}

func (h *CallEndHandler) EventType() string {
	return event.EventCallEnd
}

func (h *CallEndHandler) Handle(ctx *hub.EventContext) error {
	h.ch.HandleCallEnd(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Joining Request (event.EventJoiningRequest)
// ---------------------------------------------------------------------------

// JoiningRequestHandler processes group call joining request events.
type JoiningRequestHandler struct {
	ch *hub.CallHandler
}

func NewJoiningRequestHandler(ch *hub.CallHandler) *JoiningRequestHandler {
	return &JoiningRequestHandler{ch: ch}
}

func (h *JoiningRequestHandler) EventType() string {
	return event.EventJoiningRequest
}

func (h *JoiningRequestHandler) Handle(ctx *hub.EventContext) error {
	h.ch.RaiseJoiningRequest(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Group Notification (event.EventSendGroupNotification)
// ---------------------------------------------------------------------------

// GroupNotificationHandler processes group call notification events.
type GroupNotificationHandler struct {
	ch *hub.CallHandler
}

func NewGroupNotificationHandler(ch *hub.CallHandler) *GroupNotificationHandler {
	return &GroupNotificationHandler{ch: ch}
}

func (h *GroupNotificationHandler) EventType() string {
	return event.EventSendGroupNotification
}

func (h *GroupNotificationHandler) Handle(ctx *hub.EventContext) error {
	h.ch.SendGroupNotification(ctx.Event, ctx.Client)
	return nil
}
