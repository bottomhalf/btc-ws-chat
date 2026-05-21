package hub

import "Confeet/internal/event"

// ---------------------------------------------------------------------------
// Call Initiate (event.EventCallInitiate)
// ---------------------------------------------------------------------------

// CallInitiateHandler processes call initiation requests.
type CallInitiateHandler struct {
	ch *CallHandler
}

func NewCallInitiateHandler(ch *CallHandler) *CallInitiateHandler {
	return &CallInitiateHandler{ch: ch}
}

func (h *CallInitiateHandler) EventType() string {
	return event.EventCallInitiate
}

func (h *CallInitiateHandler) Handle(ctx *EventContext) error {
	h.ch.handleCallInitiate(ctx.Event, ctx.Client, false)
	return nil
}

// ---------------------------------------------------------------------------
// Call Started / Group Join (event.EventCallStarted)
// ---------------------------------------------------------------------------

// CallStartedHandler processes group call started events.
// Delegates to handleCallInitiate with isJoinRequest=true.
type CallStartedHandler struct {
	ch *CallHandler
}

func NewCallStartedHandler(ch *CallHandler) *CallStartedHandler {
	return &CallStartedHandler{ch: ch}
}

func (h *CallStartedHandler) EventType() string {
	return event.EventCallStarted
}

func (h *CallStartedHandler) Handle(ctx *EventContext) error {
	h.ch.handleCallInitiate(ctx.Event, ctx.Client, true)
	return nil
}

// ---------------------------------------------------------------------------
// Call Accept (event.EventCallAccept)
// ---------------------------------------------------------------------------

// CallAcceptHandler processes call acceptance events.
type CallAcceptHandler struct {
	ch *CallHandler
}

func NewCallAcceptHandler(ch *CallHandler) *CallAcceptHandler {
	return &CallAcceptHandler{ch: ch}
}

func (h *CallAcceptHandler) EventType() string {
	return event.EventCallAccept
}

func (h *CallAcceptHandler) Handle(ctx *EventContext) error {
	h.ch.handleCallAccept(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call Reject (event.EventCallReject)
// ---------------------------------------------------------------------------

// CallRejectHandler processes call rejection events.
type CallRejectHandler struct {
	ch *CallHandler
}

func NewCallRejectHandler(ch *CallHandler) *CallRejectHandler {
	return &CallRejectHandler{ch: ch}
}

func (h *CallRejectHandler) EventType() string {
	return event.EventCallReject
}

func (h *CallRejectHandler) Handle(ctx *EventContext) error {
	h.ch.handleCallReject(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call Dismiss (event.EventCallDismiss)
// ---------------------------------------------------------------------------

// CallDismissHandler processes call dismiss events.
type CallDismissHandler struct {
	ch *CallHandler
}

func NewCallDismissHandler(ch *CallHandler) *CallDismissHandler {
	return &CallDismissHandler{ch: ch}
}

func (h *CallDismissHandler) EventType() string {
	return event.EventCallDismiss
}

func (h *CallDismissHandler) Handle(ctx *EventContext) error {
	h.ch.handleCallDismiss(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call Cancel (event.EventCallCancel)
// ---------------------------------------------------------------------------

// CallCancelHandler processes call cancellation events from the caller.
type CallCancelHandler struct {
	ch *CallHandler
}

func NewCallCancelHandler(ch *CallHandler) *CallCancelHandler {
	return &CallCancelHandler{ch: ch}
}

func (h *CallCancelHandler) EventType() string {
	return event.EventCallCancel
}

func (h *CallCancelHandler) Handle(ctx *EventContext) error {
	h.ch.handleCallCancel(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call Timeout (event.EventCallTimeout)
// ---------------------------------------------------------------------------

// CallTimeoutHandler processes call timeout events from the callee.
type CallTimeoutHandler struct {
	ch *CallHandler
}

func NewCallTimeoutHandler(ch *CallHandler) *CallTimeoutHandler {
	return &CallTimeoutHandler{ch: ch}
}

func (h *CallTimeoutHandler) EventType() string {
	return event.EventCallTimeout
}

func (h *CallTimeoutHandler) Handle(ctx *EventContext) error {
	h.ch.handleCallTimeout(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Call End (event.EventCallEnd)
// ---------------------------------------------------------------------------

// CallEndHandler processes call end events (participant leaving or ending).
type CallEndHandler struct {
	ch *CallHandler
}

func NewCallEndHandler(ch *CallHandler) *CallEndHandler {
	return &CallEndHandler{ch: ch}
}

func (h *CallEndHandler) EventType() string {
	return event.EventCallEnd
}

func (h *CallEndHandler) Handle(ctx *EventContext) error {
	h.ch.handleCallEnd(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Joining Request (event.EventJoiningRequest)
// ---------------------------------------------------------------------------

// JoiningRequestHandler processes group call joining request events.
type JoiningRequestHandler struct {
	ch *CallHandler
}

func NewJoiningRequestHandler(ch *CallHandler) *JoiningRequestHandler {
	return &JoiningRequestHandler{ch: ch}
}

func (h *JoiningRequestHandler) EventType() string {
	return event.EventJoiningRequest
}

func (h *JoiningRequestHandler) Handle(ctx *EventContext) error {
	h.ch.raiseJoiningRequest(ctx.Event, ctx.Client)
	return nil
}

// ---------------------------------------------------------------------------
// Group Notification (event.EventSendGroupNotification)
// ---------------------------------------------------------------------------

// GroupNotificationHandler processes group call notification events.
type GroupNotificationHandler struct {
	ch *CallHandler
}

func NewGroupNotificationHandler(ch *CallHandler) *GroupNotificationHandler {
	return &GroupNotificationHandler{ch: ch}
}

func (h *GroupNotificationHandler) EventType() string {
	return event.EventSendGroupNotification
}

func (h *GroupNotificationHandler) Handle(ctx *EventContext) error {
	h.ch.sendGroupNotification(ctx.Event, ctx.Client)
	return nil
}
