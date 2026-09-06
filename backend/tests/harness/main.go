// Command harness is the producer and consumer stub.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	addr      = flag.String("addr", "http://localhost:8090", "gateway address")
	token     = flag.String("token", "acme-token", "bearer token")
	queue     = flag.String("queue", "harness", "queue name")
	producers = flag.Int("producers", 8, "concurrent producers")
	consumers = flag.Int("consumers", 8, "concurrent consumers")
	messages  = flag.Int("messages", 500, "messages per producer")
	groups    = flag.Int("groups", 16, "group keys per producer")
	dist      = flag.Bool("distributed", false, "create the queue as distributed")
	timeout   = flag.Duration("timeout", 2*time.Minute, "give up after this long")
)

type client struct{ http *http.Client }

func (c *client) do(method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, *addr+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+*token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, b)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

type message struct {
	MessageID string `json:"messageId"`
	Payload   string `json:"payload"`
	GroupID   string `json:"groupId"`
	Priority  uint8  `json:"priority"`
	Receipt   string `json:"receipt"`
}

type tracker struct {
	mu        sync.Mutex
	acked     map[string]int
	inFlight  map[string]bool
	groupSeen map[string][]int
	problems  []string
}

func (t *tracker) deliver(m message, index int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inFlight[m.MessageID] {
		t.problems = append(t.problems, "delivered to two workers: "+m.MessageID)
	}
	t.inFlight[m.MessageID] = true
	if m.GroupID != "" && index >= 0 {
		t.groupSeen[m.GroupID] = append(t.groupSeen[m.GroupID], index)
	}
}

func (t *tracker) ack(m message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inFlight, m.MessageID)
	t.acked[m.MessageID]++
}

func main() {
	flag.Parse()
	c := &client{http: &http.Client{Timeout: 60 * time.Second}}
	total := *producers * *messages

	fmt.Printf("queue %q, %d producers x %d messages, %d consumers, %d groups, distributed=%v\n",
		*queue, *producers, *messages, *consumers, *groups, *dist)

	if err := c.do("POST", "/v1/queues", map[string]any{
		"name": *queue, "visibilityTimeout": "30s", "maxRetries": 5,
		"defaultTtl": "1h", "distributed": *dist,
	}, nil); err != nil {
		fmt.Println("create queue:", err)
		os.Exit(1)
	}

	tr := &tracker{acked: map[string]int{}, inFlight: map[string]bool{}, groupSeen: map[string][]int{}}
	var sent, acked atomic.Int64
	start := time.Now()

	var wg sync.WaitGroup
	for p := 0; p < *producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < *messages; i++ {
				// Each producer owns its own groups.
				group := fmt.Sprintf("p%d-g%d", p, i%*groups)
				// the index is carried in the payload so the consumer can check
				// that a group came out in the order it went in
				payload := base64.StdEncoding.EncodeToString(
					[]byte(fmt.Sprintf("%s|%d", group, p**messages+i)))
				err := c.do("POST", "/v1/queues/"+*queue+"/messages", map[string]any{
					"payload": payload, "priority": (i*7 + p) % 101, "groupId": group,
				}, nil)
				if err != nil {
					fmt.Println("enqueue:", err)
					return
				}
				sent.Add(1)
			}
		}(p)
	}

	done := make(chan struct{})
	var once sync.Once
	for w := 0; w < *consumers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				var resp struct {
					Messages []message `json:"messages"`
				}
				if err := c.do("POST", "/v1/queues/"+*queue+"/messages/dequeue",
					map[string]any{"maxMessages": 1}, &resp); err != nil {
					time.Sleep(20 * time.Millisecond)
					continue
				}
				if len(resp.Messages) == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				m := resp.Messages[0]
				tr.deliver(m, indexOf(m.Payload))
				if err := c.do("POST", "/v1/queues/"+*queue+"/messages/ack",
					map[string]any{"receipt": m.Receipt}, nil); err != nil {
					continue
				}
				tr.ack(m)
				if acked.Add(1) >= int64(total) {
					once.Do(func() { close(done) })
					return
				}
			}
		}()
	}

	go func() {
		<-time.After(*timeout)
		once.Do(func() { close(done) })
	}()
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\nsent %d, acked %d in %s (%.0f msg/s)\n",
		sent.Load(), acked.Load(), elapsed.Round(time.Millisecond),
		float64(acked.Load())/elapsed.Seconds())

	fail := false
	check := func(name string, ok bool, detail string) {
		mark := "PASS"
		if !ok {
			mark, fail = "FAIL", true
		}
		fmt.Printf("  %-34s %s %s\n", name, mark, detail)
	}

	check("every message acknowledged", acked.Load() == int64(total),
		fmt.Sprintf("%d/%d", acked.Load(), total))

	twice := 0
	for _, n := range tr.acked {
		if n > 1 {
			twice++
		}
	}
	check("none acknowledged twice", twice == 0, fmt.Sprintf("%d duplicates", twice))
	check("none left in flight", len(tr.inFlight) == 0, fmt.Sprintf("%d stuck", len(tr.inFlight)))

	outOfOrder := 0
	for _, seq := range tr.groupSeen {
		for i := 1; i < len(seq); i++ {
			if seq[i] < seq[i-1] {
				outOfOrder++
			}
		}
	}
	check("group order preserved", outOfOrder == 0, fmt.Sprintf("%d inversions", outOfOrder))
	check("no concurrent delivery", len(tr.problems) == 0, fmt.Sprintf("%d", len(tr.problems)))

	var stats map[string]any
	if err := c.do("GET", "/v1/queues/"+*queue+"/stats", nil, &stats); err == nil {
		fmt.Printf("\nfinal: messages=%v inFlight=%v deadLettered=%v exact=%v owner=%v\n",
			stats["messages"], stats["inFlight"], stats["deadLettered"], stats["exact"], stats["ownerNode"])
	}

	if fail {
		os.Exit(1)
	}
}

// indexOf pulls the submission index back out of the payload.
func indexOf(payload string) int {
	b, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return -1
	}
	parts := bytes.SplitN(b, []byte("|"), 2)
	if len(parts) != 2 {
		return -1
	}
	n := 0
	for _, c := range parts[1] {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int(c-'0')
	}
	return n
}
