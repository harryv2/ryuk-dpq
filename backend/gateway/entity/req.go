package entity

import "time"

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

	// Settings holds the fields above once the controller has parsed and
	// defaulted them.
	Settings QueueSettings `json:"-"`
}

type CreateQueueResponse struct {
	Name      string `json:"name"`
	Created   bool   `json:"created"`
	OwnerNode string `json:"ownerNode,omitempty"`
}

type QueueSummary struct {
	Name        string        `json:"name"`
	Distributed bool          `json:"distributed"`
	State       string        `json:"state"`
	OwnerNode   string        `json:"ownerNode,omitempty"`
	Settings    QueueSettings `json:"settings"`
	Messages    int64         `json:"messages"`
	InFlight    int64         `json:"inFlight"`
	OldestAge   float64       `json:"oldestMessageAgeSeconds"`
}

type EnqueueRequest struct {
	Org   string `json:"-"`
	Queue string `json:"-"`
	// PayloadEncoding says how to read Payload: "text" (the default) or
	// "base64" for bytes that are not text. Guessing is not safe -- "m000" is
	// both a plausible message and valid base64 -- so the caller says.
	Payload         string `json:"payload"`
	PayloadEncoding string `json:"payloadEncoding,omitempty"`
	Priority        any    `json:"priority"`
	GroupID         string `json:"groupId,omitempty"`
	TTL             string `json:"ttl,omitempty"`
	DeliverAfter    string `json:"deliverAfter,omitempty"`

	// Filled by the controller from the fields above.
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

// ClusterPlacement is one queue's footprint on one node. A distributed queue
// appears on several nodes with a different Slots on each, so a plain list of
// names would suggest one queue is many.
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

type ClusterResponse struct {
	Nodes []ClusterNode `json:"nodes"`
}
