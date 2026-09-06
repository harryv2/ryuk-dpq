// Package client is a small REST client for the gateway.
package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	base  string
	token string
	http  *http.Client
}

func New(base, token string) *Client {
	return &Client{base: base, token: token, http: &http.Client{Timeout: 30 * time.Second}}
}

// Error carries the status code so a test can assert on rejection rather than
// only on failure.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("%d: %s", e.Status, e.Message) }

func StatusOf(err error) int {
	if e, ok := err.(*Error); ok {
		return e.Status
	}
	return 0
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 400 {
		msg := string(raw)
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &e) == nil && e.Error != "" {
			msg = e.Error
		}
		return &Error{Status: res.StatusCode, Message: msg}
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

type CreateQueue struct {
	Name                string  `json:"name"`
	Distributed         bool    `json:"distributed,omitempty"`
	VisibilityTimeout   string  `json:"visibilityTimeout,omitempty"`
	MaxRetries          *uint32 `json:"maxRetries,omitempty"`
	DefaultTTL          string  `json:"defaultTtl,omitempty"`
	StarvationThreshold string  `json:"starvationThreshold,omitempty"`
	DeadLetterQueue     string  `json:"deadLetterQueue,omitempty"`
	PlacementWidth      int     `json:"placementWidth,omitempty"`
}

type QueueSummary struct {
	Name        string `json:"name"`
	Distributed bool   `json:"distributed"`
	State       string `json:"state"`
	OwnerNode   string `json:"ownerNode"`
	Messages    int64  `json:"messages"`
	InFlight    int64  `json:"inFlight"`
}

type Stats struct {
	Queue            string           `json:"queue"`
	Messages         int64            `json:"messages"`
	InFlight         int64            `json:"inFlight"`
	Delayed          int64            `json:"delayed"`
	ByPriority       map[string]int64 `json:"byPriority"`
	DeadLettered     uint64           `json:"deadLettered"`
	Enqueued         uint64           `json:"enqueued"`
	Acked            uint64           `json:"acked"`
	Redelivered      uint64           `json:"redelivered"`
	Distributed      bool             `json:"distributed"`
	Exact            bool             `json:"exact"`
	UnavailableSlots int              `json:"unavailableSlots"`
	OwnerNode        string           `json:"ownerNode"`
}

type Message struct {
	MessageID  string    `json:"messageId"`
	Payload    string    `json:"payload"`
	Priority   uint8     `json:"priority"`
	GroupID    string    `json:"groupId"`
	Attempts   uint32    `json:"attempts"`
	EnqueuedAt time.Time `json:"enqueuedAt"`
	Receipt    string    `json:"receipt"`
}

// Text decodes the payload. The gateway always answers in base64, because a
// payload may be bytes rather than text.
func (m Message) Text() string {
	if b, err := base64.StdEncoding.DecodeString(m.Payload); err == nil {
		return string(b)
	}
	return m.Payload
}

type Placement struct {
	Queue       string `json:"queue"`
	Org         string `json:"org"`
	Distributed bool   `json:"distributed"`
	Slots       int    `json:"slots"`
	TotalSlots  int    `json:"totalSlots"`
}

type Node struct {
	ID     string      `json:"id"`
	Addr   string      `json:"addr"`
	Queues []Placement `json:"queues"`
}

func (c *Client) CreateQueue(ctx context.Context, req CreateQueue) error {
	return c.do(ctx, "POST", "/v1/queues", req, nil)
}

func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	return c.do(ctx, "DELETE", "/v1/queues/"+name, nil, nil)
}

func (c *Client) ListQueues(ctx context.Context) ([]QueueSummary, error) {
	var out struct {
		Queues []QueueSummary `json:"queues"`
	}
	err := c.do(ctx, "GET", "/v1/queues", nil, &out)
	return out.Queues, err
}

func (c *Client) Stats(ctx context.Context, name string) (Stats, error) {
	var s Stats
	err := c.do(ctx, "GET", "/v1/queues/"+name+"/stats", nil, &s)
	return s, err
}

type Enqueue struct {
	Payload      string `json:"payload"`
	Encoding     string `json:"payloadEncoding,omitempty"`
	Priority     any    `json:"priority,omitempty"`
	GroupID      string `json:"groupId,omitempty"`
	TTL          string `json:"ttl,omitempty"`
	DeliverAfter string `json:"deliverAfter,omitempty"`
}

func (c *Client) Enqueue(ctx context.Context, name string, req Enqueue) (string, error) {
	var out struct {
		MessageID string `json:"messageId"`
	}
	err := c.do(ctx, "POST", "/v1/queues/"+name+"/messages", req, &out)
	return out.MessageID, err
}

func (c *Client) Dequeue(ctx context.Context, name string, max int) ([]Message, error) {
	var out struct {
		Messages []Message `json:"messages"`
	}
	body := map[string]int{"maxMessages": max}
	err := c.do(ctx, "POST", "/v1/queues/"+name+"/messages/dequeue", body, &out)
	return out.Messages, err
}

func (c *Client) Ack(ctx context.Context, name, receipt string) error {
	return c.do(ctx, "POST", "/v1/queues/"+name+"/messages/ack",
		map[string]string{"receipt": receipt}, nil)
}

func (c *Client) Nack(ctx context.Context, name, receipt string) error {
	return c.do(ctx, "POST", "/v1/queues/"+name+"/messages/nack",
		map[string]string{"receipt": receipt}, nil)
}

func (c *Client) Cluster(ctx context.Context) ([]Node, error) {
	var out struct {
		Nodes []Node `json:"nodes"`
	}
	err := c.do(ctx, "GET", "/v1/cluster", nil, &out)
	return out.Nodes, err
}

// DrainAll takes and acknowledges everything the queue will hand over, which is
// how a test collects delivery order.
func (c *Client) DrainAll(ctx context.Context, name string, limit int) ([]Message, error) {
	var all []Message
	for len(all) < limit {
		batch, err := c.Dequeue(ctx, name, 10)
		if err != nil {
			return all, err
		}
		if len(batch) == 0 {
			break
		}
		for _, m := range batch {
			if err := c.Ack(ctx, name, m.Receipt); err != nil {
				return all, err
			}
		}
		all = append(all, batch...)
	}
	return all, nil
}
