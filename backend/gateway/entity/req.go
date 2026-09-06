package entity

import "time"

// CreateQueueRequest is the wire shape: durations arrive as strings like "30s",
// and MaxRetries is a pointer so that 0 (never retry) is distinguishable from
// absent (use the default).
type CreateQueueRequest struct {
	Org                 string  `json:"-"`
	Name                string  `json:"name"`
	VisibilityTimeout   string  `json:"visibilityTimeout,omitempty"`
	MaxRetries          *uint32 `json:"maxRetries,omitempty"`
	DefaultTTL          string  `json:"defaultTtl,omitempty"`
	StarvationThreshold string  `json:"starvationThreshold,omitempty"`
	StarvationReserve   float64 `json:"starvationReserve,omitempty"`
	MaxDepth            int64   `json:"maxDepth,omitempty"`
	DeadLetterQueue     string  `json:"deadLetterQueue,omitempty"`
	Distributed         bool    `json:"distributed,omitempty"`
	PlacementWidth      int     `json:"placementWidth,omitempty"`

	// The fields above, parsed and defaulted by the controller. Everything
	// downstream reads this and never the raw strings.
	ParseQueueSettings QueueSettings `json:"-"`
}

// UpdateQueueRequest reuses creation's fields so one validator serves both.
type UpdateQueueRequest struct {
	CreateQueueRequest
	Distributed *bool `json:"distributed,omitempty"`
}

type CreateQueueResponse struct {
	Name      string `json:"name"`
	Created   bool   `json:"created"`
	OwnerNode string `json:"ownerNode,omitempty"`
}

type QueueSummary struct {
	Name           string        `json:"name"`
	Distributed    bool          `json:"distributed"`
	PlacementWidth int           `json:"placementWidth,omitempty"`
	State          string        `json:"state"`
	OwnerNode      string        `json:"ownerNode,omitempty"`
	Settings       QueueSettings `json:"settings"`
	Messages       int64         `json:"messages"`
	InFlight       int64         `json:"inFlight"`
	OldestAge      float64       `json:"oldestMessageAgeSeconds"`
}

type EnqueueRequest struct {
	Org   string `json:"-"`
	Queue string `json:"-"`
	// PayloadEncoding says how to read Payload: "text" (the default) or "base64"
	// for bytes that are not text.
	Payload         string `json:"payload"`
	PayloadEncoding string `json:"payloadEncoding,omitempty"`
	Priority        any    `json:"priority"`
	GroupID         string `json:"groupId,omitempty"`
	TTL             string `json:"ttl,omitempty"`
	DeliverAfter    string `json:"deliverAfter,omitempty"`

	Body          []byte        `json:"-"`
	PriorityValue uint8         `json:"-"`
	TTLFor        time.Duration `json:"-"`
	DeliverIn     time.Duration `json:"-"`
}

type EnqueueResponse struct {
	MessageID string `json:"messageId"`
}

type DequeueRequest struct {
	Org         string        `json:"-"`
	Queue       string        `json:"-"`
	MaxMessages int           `json:"maxMessages,omitempty"`
	WaitTime    time.Duration `json:"-"`
	WaitTimeRaw string        `json:"waitTime,omitempty"`
}

type Message struct {
	MessageID  string    `json:"messageId"`
	Payload    string    `json:"payload"`
	Priority   uint8     `json:"priority"`
	GroupID    string    `json:"groupId,omitempty"`
	Attempts   uint32    `json:"attempts"`
	EnqueuedAt time.Time `json:"enqueuedAt"`
	Receipt    string    `json:"receipt"`
}

type DequeueResponse struct {
	Messages []Message `json:"messages"`
}

type AckRequest struct {
	Org     string `json:"-"`
	Queue   string `json:"-"`
	Receipt string `json:"receipt"`
}

type NackRequest struct {
	Org     string `json:"-"`
	Queue   string `json:"-"`
	Receipt string `json:"receipt"`
	Delay   string `json:"delay,omitempty"`

	DelayFor time.Duration `json:"-"`
}

type QueueStatsResponse struct {
	Queue                   string           `json:"queue"`
	Messages                int64            `json:"messages"`
	InFlight                int64            `json:"inFlight"`
	Delayed                 int64            `json:"delayed"`
	ByPriority              map[string]int64 `json:"byPriority"`
	OldestMessageAgeSeconds float64          `json:"oldestMessageAgeSeconds"`
	DeadLettered            uint64           `json:"deadLettered"`
	Enqueued                uint64           `json:"enqueued"`
	Acked                   uint64           `json:"acked"`
	Expired                 uint64           `json:"expired"`
	Redelivered             uint64           `json:"redelivered"`
	StarvationEscapes       uint64           `json:"starvationEscapes"`
	EnqueueRate             float64          `json:"enqueueRate"`
	AckRate                 float64          `json:"ackRate"`
	OwnerNode               string           `json:"ownerNode,omitempty"`
	Distributed             bool             `json:"distributed"`
	Exact                   bool             `json:"exact"`
	UnavailableSlots        int              `json:"unavailableSlots,omitempty"`
	AsOf                    time.Time        `json:"asOf"`
}

// ClusterPlacement is one queue's footprint on one node.
type ClusterPlacement struct {
	Queue       string `json:"queue"`
	Org         string `json:"org"`
	Distributed bool   `json:"distributed"`
	Slots       int    `json:"slots"`
	TotalSlots  int    `json:"totalSlots"`
}

type ClusterNode struct {
	ID     string             `json:"id"`
	Addr   string             `json:"addr"`
	Queues []ClusterPlacement `json:"queues"`
}

// RegistryResponse is the raw etcd contents plus how many nodes the gateway
// itself is tracking, so the two can be compared.
type RegistryResponse struct {
	Entries  []RegistryEntry `json:"entries"`
	Watching int             `json:"watching"`
}

type ClusterResponse struct {
	Nodes []ClusterNode `json:"nodes"`
	// Unavailable is slots placed on machines that are no longer registered.
	// Their data is only on those machines, so they are not reassigned.
	Unavailable []ClusterPlacement `json:"unavailable,omitempty"`
}

// NodeQueueDetail is one queue's share of one node: the slots placed there and
// the messages actually sitting in them.
type NodeQueueDetail struct {
	Queue        string           `json:"queue"`
	Distributed  bool             `json:"distributed"`
	Slots        int              `json:"slots"`
	TotalSlots   int              `json:"totalSlots"`
	Ready        int64            `json:"ready"`
	ByPriority   map[string]int64 `json:"byPriority,omitempty"`
	InFlight     int64            `json:"inFlight"`
	Delayed      int64            `json:"delayed"`
	OldestAge    float64          `json:"oldestMessageAgeSeconds"`
	Enqueued     uint64           `json:"enqueued"`
	Acked        uint64           `json:"acked"`
	Expired      uint64           `json:"expired"`
	Redelivered  uint64           `json:"redelivered"`
	DeadLettered uint64           `json:"deadLettered"`
	Escapes      uint64           `json:"starvationEscapes"`
}

type NodeDetailResponse struct {
	ID        string            `json:"id"`
	Addr      string            `json:"addr"`
	Live      bool              `json:"live"`
	Slots     int               `json:"slots"`
	Ready     int64             `json:"ready"`
	InFlight  int64             `json:"inFlight"`
	Delayed   int64             `json:"delayed"`
	OldestAge float64           `json:"oldestMessageAgeSeconds"`
	Queues    []NodeQueueDetail `json:"queues"`
}
