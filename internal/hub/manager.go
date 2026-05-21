package hub

import (
	"Confeet/internal/event"
	"Confeet/internal/model"
	"Confeet/internal/repo"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	shardCount    = 64               // tune: 16/64/128 depending on load
	roomTTL       = 30 * time.Minute // TTL for room cache eviction
	evictInterval = 5 * time.Minute  // How often to check for expired rooms
	Direct        = "direct"
	Group         = "group"
)

type inboundMessage struct {
	event  event.WsEvent
	client *Client
}

// Room represents a cached conversation room with its members (userIds)
type Room struct {
	ConversationID string
	Members        map[string]bool // userIds who are members of this room
	LastAccess     time.Time       // For TTL-based eviction
	mu             sync.RWMutex
}

type roomBucket struct {
	sync.RWMutex
	rooms map[string]*Room // conversationID -> Room
}

func generateaUUID(firstUserId string, secondUserId string) string {
	// Your input value
	var value string

	for i := 0; i < len(firstUserId); i++ {
		if firstUserId[i] < secondUserId[i] {
			value = secondUserId + firstUserId
			break
		} else {
			value = firstUserId + secondUserId
			break
		}
	}

	// Define a namespace (you can use a predefined one or create your own)
	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8") // UUID namespace for DNS (or use your own)

	// Generate deterministic UUID
	clientID := uuid.NewSHA1(namespace, []byte(value)).String()

	log.Println(clientID)
	// This will ALWAYS produce the same UUID for "1535"

	return clientID
}

type Hub struct {
	shards [shardCount]*roomBucket

	// Online users - maps userId to their Client connection
	onlineUsers   map[string]*Client
	onlineUsersMu sync.RWMutex
	callHandler   *CallHandler
	registry      *HandlerRegistry

	register   chan *Client
	unregister chan *Client
	broadcast  chan event.WsEvent
	inbound    chan inboundMessage
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc

	// Repositories
	messageRepo      repo.MessageRepository
	userRepository   repo.UserRepository
	conversationRepo repo.ConversationRepository
}

func NewHub(messageRepo repo.MessageRepository, conversationRepo repo.ConversationRepository, userRepository repo.UserRepository) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		onlineUsers:      make(map[string]*Client),
		register:         make(chan *Client, 1024),
		unregister:       make(chan *Client, 1024),
		broadcast:        make(chan event.WsEvent, 1024),
		inbound:          make(chan inboundMessage, 4096),
		messageRepo:      messageRepo,
		userRepository:   userRepository,
		conversationRepo: conversationRepo,
		ctx:              ctx,
		cancel:           cancel,
	}

	// Initialize call handler with hub reference
	h.callHandler = NewCallHandler()
	h.callHandler.SetHub(h)

	// Initialize handler registry with middleware pipeline
	h.registry = NewHandlerRegistry()
	h.registry.Use(
		RecoveryMiddleware(),
		LoggingMiddleware(),
	)

	// Register message handlers
	h.registry.Register(NewSendMessageHandler(messageRepo, conversationRepo))
	h.registry.Register(NewMarkDeliveredHandler(conversationRepo))
	h.registry.Register(NewTypingHandler())
	h.registry.Register(NewUpdateStatusHandler(userRepository))
	h.registry.Register(NewHeartbeatHandler())

	// Register call handlers
	h.registry.Register(NewCallInitiateHandler(h.callHandler))
	h.registry.Register(NewCallStartedHandler(h.callHandler))
	h.registry.Register(NewCallAcceptHandler(h.callHandler))
	h.registry.Register(NewCallRejectHandler(h.callHandler))
	h.registry.Register(NewCallDismissHandler(h.callHandler))
	h.registry.Register(NewCallCancelHandler(h.callHandler))
	h.registry.Register(NewCallTimeoutHandler(h.callHandler))
	h.registry.Register(NewCallEndHandler(h.callHandler))
	h.registry.Register(NewJoiningRequestHandler(h.callHandler))
	h.registry.Register(NewGroupNotificationHandler(h.callHandler))

	for i := 0; i < shardCount; i++ {
		h.shards[i] = &roomBucket{
			rooms: make(map[string]*Room),
		}
	}

	// run manager loop
	go h.run()

	// start worker pool for processing inbound messages
	for i := 0; i < workerPoolSize; i++ {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			for {
				select {
				case <-h.ctx.Done():
					return
				case in, ok := <-h.inbound:
					if !ok {
						return
					}
					h.handleEvent(in.event, in.client)
				}
			}
		}()
	}

	// start TTL eviction goroutine
	go h.evictExpiredRooms()

	// start heartbeat checker goroutine to remove stale clients
	go h.checkStaleClients()

	return h
}

func (h *Hub) handleEvent(ev event.WsEvent, c *Client) {
	ctx := &EventContext{
		Event:  ev,
		Client: c,
		hub:    h,
	}
	if err := h.registry.Dispatch(ctx); err != nil {
		log.Printf("event dispatch error for %s: %v", ev.Event, err)
	}
}

// publishToRoom sends message to all ONLINE members of a room
func (h *Hub) publishToRoom(ev event.WsEvent, groupConversationID string) {
	// Get the room from cache
	room := h.GetRoom(groupConversationID)
	if room == nil {
		log.Printf("room %s not found, cannot publish message", groupConversationID)
		return
	}

	// Get list of members
	room.mu.RLock()
	memberIDs := make([]string, 0, len(room.Members))
	for memberID := range room.Members {
		memberIDs = append(memberIDs, memberID)
	}
	room.mu.RUnlock()

	// Find online clients for each member and send
	h.onlineUsersMu.RLock()
	onlineClients := make([]*Client, 0)
	for _, memberID := range memberIDs {
		if client, online := h.onlineUsers[memberID]; online {
			onlineClients = append(onlineClients, client)
		}
		// TODO: For offline members, queue message to Kafka/Redis for later delivery
	}
	h.onlineUsersMu.RUnlock()

	// Deliver to online clients without holding lock
	for _, client := range onlineClients {
		// Use SafeSend to prevent panic on closed channel
		if !client.SafeSend(ev, sendTimeout) {
			if client.IsClosed() {
				log.Printf("client %s already closed, skipping", client.ID)
			} else {
				log.Printf("egress full for client %s in conversation %s", client.ID, groupConversationID)
				if kickOnFull {
					h.unregister <- client
				}
			}
		}
	}

	log.Printf("message published to %d/%d members in room %s", len(onlineClients), len(memberIDs), groupConversationID)
}

// broadcastToAll sends an event to all ONLINE users connected to the hub
func (h *Hub) broadcastToAll(ev event.WsEvent) {
	h.onlineUsersMu.RLock()
	onlineClients := make([]*Client, 0, len(h.onlineUsers))
	for _, client := range h.onlineUsers {
		onlineClients = append(onlineClients, client)
	}
	h.onlineUsersMu.RUnlock()

	// Deliver to online clients without holding lock
	for _, client := range onlineClients {
		client.SafeSend(ev, sendTimeout)
	}

	log.Printf("event %s broadcasted to all %d online users", ev.Event, len(onlineClients))
}

func (h *Hub) GetRoom(conversationID string) *Room {
	// Get the room from cache
	room := h.FindRoom(conversationID)
	if room == nil {
		log.Printf("room %s not found in cache - fetching from database", conversationID)

		// Fetch room details from database
		ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
		conversation, err := h.conversationRepo.GetRoomDetail(ctx, conversationID)
		cancel()

		if err != nil {
			log.Printf("failed to fetch room %s from database: %v", conversationID, err)
			return nil
		}

		if conversation == nil {
			log.Printf("room %s not found in database", conversationID)
			return nil
		}

		// Set room members in cache from the fetched conversation
		h.SetRoomMembers(conversationID, conversation.ParticipantIds)
		room = h.FindRoom(conversationID)
		if room == nil {
			log.Printf("failed to create room %s in cache", conversationID)
			return nil
		}
	}

	return room
}

// evictExpiredRooms periodically removes rooms that haven't been accessed recently
func (h *Hub) evictExpiredRooms() {
	ticker := time.NewTicker(evictInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			evicted := 0

			for _, bucket := range h.shards {
				bucket.Lock()
				for roomID, room := range bucket.rooms {
					room.mu.RLock()
					expired := now.Sub(room.LastAccess) > roomTTL
					room.mu.RUnlock()

					if expired {
						delete(bucket.rooms, roomID)
						evicted++
					}
				}
				bucket.Unlock()
			}

			if evicted > 0 {
				log.Printf("evicted %d expired rooms from cache", evicted)
			}
		}
	}
}

// checkStaleClients periodically checks for clients that haven't sent heartbeat
// and removes them from the online users list
func (h *Hub) checkStaleClients() {
	ticker := time.NewTicker(heartbeatCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.removeStaleClients()
		}
	}
}

// removeStaleClients finds and removes clients that haven't sent heartbeat within timeout
func (h *Hub) removeStaleClients() {
	// Collect stale clients while holding read lock
	h.onlineUsersMu.RLock()
	staleClients := make([]*Client, 0)
	for _, client := range h.onlineUsers {
		if client.IsStale(heartbeatTimeout) {
			staleClients = append(staleClients, client)
		}
	}
	h.onlineUsersMu.RUnlock()

	// Remove stale clients (this will trigger unregister flow)
	for _, client := range staleClients {
		lastSeen := client.GetLastSeen()
		log.Printf("HEARTBEAT TIMEOUT: client %s (user: %s) last seen %v ago - removing",
			client.ID, client.userId, time.Since(lastSeen).Round(time.Second))

		// Send to unregister channel to properly clean up
		select {
		case h.unregister <- client:
			// queued for removal
		case <-time.After(unregisterTimeout):
			log.Printf("failed to queue stale client %s for removal: timeout", client.ID)
			// Force close the connection
			client.Close()
		}
	}

	if len(staleClients) > 0 {
		log.Printf("HEARTBEAT CHECK: removed %d stale clients", len(staleClients))
	}
}

// FindRoom gets a room from cache, returns nil if not found
func (h *Hub) FindRoom(conversationID string) *Room {
	sh := getShard(conversationID)
	bucket := h.shards[sh]

	bucket.RLock()
	room, exists := bucket.rooms[conversationID]
	bucket.RUnlock()

	if exists {
		room.mu.Lock()
		room.LastAccess = time.Now()
		room.mu.Unlock()
		return room
	}

	return nil
}

// SetRoomMembers sets the members for a room (call this from your service after loading from DB)
func (h *Hub) SetRoomMembers(conversationID string, memberIDs []string) {
	sh := getShard(conversationID)
	bucket := h.shards[sh]

	bucket.Lock()
	defer bucket.Unlock()

	room, exists := bucket.rooms[conversationID]
	if !exists {
		room = &Room{
			ConversationID: conversationID,
			Members:        make(map[string]bool),
			LastAccess:     time.Now(),
		}
		bucket.rooms[conversationID] = room
	}

	room.mu.Lock()
	room.Members = make(map[string]bool)
	for _, memberID := range memberIDs {
		room.Members[memberID] = true
	}
	room.LastAccess = time.Now()
	room.mu.Unlock()

	log.Printf("room %s updated with %d members (shard %d)", conversationID, len(memberIDs), sh)
}

// AddMemberToRoom adds a member to room cache (call after persisting to DB)
func (h *Hub) AddMemberToRoom(conversationID string, userID string) {
	sh := getShard(conversationID)
	bucket := h.shards[sh]

	bucket.RLock()
	room, exists := bucket.rooms[conversationID]
	bucket.RUnlock()

	if exists {
		room.mu.Lock()
		room.Members[userID] = true
		room.LastAccess = time.Now()
		room.mu.Unlock()
	}
}

// RemoveMemberFromRoom removes a member from room cache (call after persisting to DB)
func (h *Hub) RemoveMemberFromRoom(conversationID string, userID string) {
	sh := getShard(conversationID)
	bucket := h.shards[sh]

	bucket.RLock()
	room, exists := bucket.rooms[conversationID]
	bucket.RUnlock()

	if exists {
		room.mu.Lock()
		delete(room.Members, userID)
		room.LastAccess = time.Now()
		room.mu.Unlock()
	}
}

// sendErrorToClient sends an error event back to the specific client
func (h *Hub) sendErrorToClient(c *Client, code string, message string) {
	errorPayload := model.ErrorPayload{
		Code:    code,
		Message: message,
	}

	payload, err := json.Marshal(errorPayload)
	if err != nil {
		log.Printf("failed to marshal error payload: %v", err)
		return
	}

	errorEvent := event.WsEvent{
		Event:   event.EventError,
		Payload: payload,
	}

	if !c.SafeSend(errorEvent, sendTimeout) {
		log.Printf("failed to send error to client %s: client closed or timeout", c.ID)
	}
}

func getShard(conversationID string) uint32 {
	if conversationID == "" {
		return 0
	}

	h := sha1.Sum([]byte(conversationID))
	return binary.BigEndian.Uint32(h[:4]) % shardCount
}

// addClient is called when a client is registered
func (h *Hub) addClient(c *Client) {
	h.onlineUsersMu.Lock()
	h.onlineUsers[c.userId] = c
	h.onlineUsersMu.Unlock()

	log.Printf("client %s (user: %s) added to online users", c.ID, c.userId)

	// Update status to online in DB
	if err := h.userRepository.UpdateUserStatus(c.userId, "online"); err != nil {
		log.Printf("failed to update user status to online in DB: %v", err)
	}

	// Broadcast online status to all other online users
	statusUpdate := model.UserStatus{
		UserID: c.userId,
		Status: "online",
	}
	payload, _ := json.Marshal(statusUpdate)
	ev := event.WsEvent{
		Event:   event.EventUserStatus,
		Payload: payload,
	}
	h.broadcastToAll(ev)

	// Send existing online users' statuses to the newly connected client
	h.onlineUsersMu.RLock()
	for _, existingClient := range h.onlineUsers {
		if existingClient.userId != c.userId {
			update := model.UserStatus{
				UserID: existingClient.userId,
				Status: existingClient.GetStatus(),
			}
			p, _ := json.Marshal(update)
			e := event.WsEvent{
				Event:   event.EventUserStatus,
				Payload: p,
			}
			c.SafeSend(e, sendTimeout)
		}
	}
	h.onlineUsersMu.RUnlock()
}

func (h *Hub) Stop() {
	h.cancel()

	// Close all online client connections
	h.onlineUsersMu.RLock()
	for _, client := range h.onlineUsers {
		client.Close()
	}
	h.onlineUsersMu.RUnlock()

	close(h.inbound)
	h.wg.Wait()
}

// removeClient removes the client from online users (does NOT remove from rooms)
func (h *Hub) removeClient(c *Client) {
	h.onlineUsersMu.Lock()
	// Only remove if it's the same client (in case of reconnects)
	if existing, ok := h.onlineUsers[c.userId]; ok && existing.ID == c.ID {
		delete(h.onlineUsers, c.userId)
		log.Printf("REMOVED: client %s (user: %s) - count now: %d", c.ID, c.userId, len(h.onlineUsers))

		// Update status to offline in DB
		if err := h.userRepository.UpdateUserStatus(c.userId, "offline"); err != nil {
			log.Printf("failed to update user status to offline in DB: %v", err)
		}

		// Broadcast offline status to all other online users
		statusUpdate := model.UserStatus{
			UserID: c.userId,
			Status: "offline",
		}
		payload, _ := json.Marshal(statusUpdate)
		ev := event.WsEvent{
			Event:   event.EventUserStatus,
			Payload: payload,
		}
		h.broadcastToAll(ev)
	} else {
		log.Printf("SKIP REMOVE: client %s (user: %s) - different client active", c.ID, c.userId)
	}
	h.onlineUsersMu.Unlock()

	// Close the client connection
	c.Close()
	log.Printf("client %s (user: %s) removed from online users", c.ID, c.userId)
}

func (h *Hub) run() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case c := <-h.register:
			h.addClient(c)
		case c := <-h.unregister:
			h.removeClient(c)
		}
	}
}

var (
	websocketUpgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     checkOrigin,
	}
)

func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")

	switch origin {
	case "http://localhost:4200":
		return true
	case "https://www.confeet.com":
		return true
	default:
		return false
	}
}

// ServeWS handles WebSocket connection requests
func (h *Hub) ServeWS(c *gin.Context, userId string) {
	conn, err := websocketUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println(err)
		return
	}

	RegisterClient(userId, conn, h)
}
