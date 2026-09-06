// Command seed fills a running gateway with queues and messages, so the UI and
// the metrics have something real to show.
//
//	go run ./backend/tests/seed
//	go run ./backend/tests/seed -queues 40 -messages 500 -reset
//
// It builds a spread rather than a uniform pile: single-node and distributed
// queues, several placement widths, priorities across the whole 0-100 scale,
// grouped and ungrouped messages, some delayed, some in flight, some nacked
// until they dead-letter. A queue that looks the same as every other queue
// tells you nothing.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	addr        = flag.String("addr", "http://localhost:8090", "gateway address")
	orgs        = flag.String("orgs", "acme-token,globex-token", "comma-separated bearer tokens, one per tenant")
	queues      = flag.Int("queues", 12, "queues per tenant")
	messages    = flag.Int("messages", 150, "messages per queue")
	concurrency = flag.Int("concurrency", 24, "parallel requests")
	reset       = flag.Bool("reset", false, "delete every existing queue first")
	seed        = flag.Int64("seed", 1, "random seed, so a run is repeatable")
)

// Names that read like a real system rather than queue-1, queue-2.
var names = []string{
	"orders", "payments", "notifications", "emails", "webhooks", "invoices",
	"exports", "thumbnails", "search-index", "audit-log", "receipts", "refunds",
	"shipping", "fraud-checks", "reports", "sms", "push", "reconciliation",
	"imports", "cleanup",
}

type client struct {
	http  *http.Client
	token string
}

func (c *client) do(method, path string, body any) (int, []byte) {
	var buf io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, *addr+path, buf)
	if err != nil {
		return 0, nil
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

type queueSpec struct {
	name        string
	distributed bool
	width       int
	dlq         string
}

// plan decides what each queue looks like. Every third one is distributed, and
// the widths vary so the cluster page shows queues occupying different numbers
// of machines.
func plan(n int, rnd *rand.Rand) []queueSpec {
	widths := []int{2, 3, 4, 6}
	out := make([]queueSpec, 0, n)
	for i := 0; i < n; i++ {
		name := names[i%len(names)]
		if i >= len(names) {
			name = fmt.Sprintf("%s-%d", name, i/len(names)+1)
		}
		q := queueSpec{name: name}
		if i%3 == 0 {
			q.distributed = true
			q.width = widths[rnd.Intn(len(widths))]
		}
		out = append(out, q)
	}
	// A couple of queues get a dead-letter target, which has to exist first.
	if len(out) > 3 {
		out[1].dlq = out[0].name + "-dlq"
		out[3].dlq = out[0].name + "-dlq"
	}
	return out
}

func main() {
	flag.Parse()
	rnd := rand.New(rand.NewSource(*seed))
	tokens := strings.Split(*orgs, ",")

	http.DefaultClient.Timeout = 30 * time.Second
	tr := &http.Transport{MaxIdleConnsPerHost: *concurrency * 2}

	start := time.Now()
	var sent, acked, inflight, dead atomic.Int64

	for _, token := range tokens {
		token = strings.TrimSpace(token)
		c := &client{http: &http.Client{Transport: tr, Timeout: 30 * time.Second}, token: token}

		if code, _ := c.do("GET", "/v1/queues", nil); code != 200 {
			fmt.Fprintf(os.Stderr, "cannot reach the gateway at %s as %q — is it running? (make up)\n", *addr, token)
			os.Exit(1)
		}
		if *reset {
			deleteAll(c)
		}

		specs := plan(*queues, rnd)
		fmt.Printf("\n%s\n", token)

		// The dead-letter targets first, or the queues naming them are refused.
		for _, s := range specs {
			if s.dlq != "" {
				c.do("POST", "/v1/queues", map[string]any{"name": s.dlq})
			}
		}
		for _, s := range specs {
			body := map[string]any{
				"name":                s.name,
				"visibilityTimeout":   "45s",
				"maxRetries":          3,
				"defaultTtl":          "6h",
				"starvationThreshold": "5m",
				"starvationReserve":   0.2,
			}
			if s.distributed {
				body["distributed"] = true
				body["placementWidth"] = s.width
			}
			if s.dlq != "" {
				body["deadLetterQueue"] = s.dlq
			}
			if code, out := c.do("POST", "/v1/queues", body); code != 201 && code != 200 {
				fmt.Printf("  %-16s create failed (%d): %s\n", s.name, code, strings.TrimSpace(string(out)))
				continue
			}
			fill(c, s, rnd, &sent, &acked, &inflight, &dead)
		}
	}

	fmt.Printf("\nseeded in %s\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("  sent      %d\n", sent.Load())
	fmt.Printf("  acked     %d\n", acked.Load())
	fmt.Printf("  in flight %d\n", inflight.Load())
	fmt.Printf("  dead      %d\n", dead.Load())
	fmt.Printf("\n  open %s\n", *addr)
}

func deleteAll(c *client) {
	code, out := c.do("GET", "/v1/queues", nil)
	if code != 200 {
		return
	}
	var list struct {
		Queues []struct {
			Name string `json:"name"`
		} `json:"queues"`
	}
	_ = json.Unmarshal(out, &list)
	for _, q := range list.Queues {
		c.do("DELETE", "/v1/queues/"+q.Name, nil)
	}
}

// fill sends the messages, then works some of them so the queue has history:
// acknowledged ones for the throughput charts, in-flight ones for the gauge,
// and a few nacked past the retry limit so the dead-letter count is not zero.
func fill(c *client, s queueSpec, rnd *rand.Rand, sent, acked, inflight, dead *atomic.Int64) {
	work := make(chan int, *messages)
	for i := 0; i < *messages; i++ {
		work <- i
	}
	close(work)

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(rnd.Int63()))
			for i := range work {
				body := map[string]any{
					"payload":  fmt.Sprintf("%s job %d", s.name, i),
					"priority": priority(r),
				}
				// Two thirds grouped, so ordering is visible and the rest can
				// still be delivered in parallel.
				if r.Intn(3) > 0 {
					body["groupId"] = fmt.Sprintf("customer-%d", r.Intn(24))
				}
				if r.Intn(12) == 0 {
					body["deliverAfter"] = fmt.Sprintf("%dm", 1+r.Intn(30))
				}
				if code, _ := c.do("POST", "/v1/queues/"+s.name+"/messages", body); code == 201 {
					sent.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	drain(c, s.name, *messages/4, true, acked)      // acked: gives the charts a rate
	drain(c, s.name, *messages/20, false, inflight) // taken and left, for the in-flight gauge
	if s.dlq != "" {
		burn(c, s.name, 3, dead)
	}
	fmt.Printf("  %-16s %-12s %d messages\n", s.name, kind(s), *messages)
}

func kind(s queueSpec) string {
	if s.distributed {
		return fmt.Sprintf("dist w=%d", s.width)
	}
	return "single"
}

// priority spreads over the whole scale rather than only the three names, so
// the metrics bands and the custom-priority path both get exercised.
func priority(r *rand.Rand) any {
	switch r.Intn(10) {
	case 0, 1, 2:
		return "HIGH"
	case 3, 4:
		return "MEDIUM"
	case 5:
		return "LOW"
	default:
		return r.Intn(101)
	}
}

type message struct {
	MessageID string `json:"messageId"`
	Receipt   string `json:"receipt"`
}

func take(c *client, queue string, n int) []message {
	code, out := c.do("POST", "/v1/queues/"+queue+"/messages/dequeue",
		map[string]any{"maxMessages": n})
	if code != 200 {
		return nil
	}
	var resp struct {
		Messages []message `json:"messages"`
	}
	_ = json.Unmarshal(out, &resp)
	return resp.Messages
}

func drain(c *client, queue string, n int, ack bool, count *atomic.Int64) {
	for got := 0; got < n; {
		batch := take(c, queue, 10)
		if len(batch) == 0 {
			return
		}
		for _, m := range batch {
			if ack {
				c.do("POST", "/v1/queues/"+queue+"/messages/ack",
					map[string]any{"receipt": m.Receipt})
			}
			count.Add(1)
			got++
		}
	}
}

// burn nacks the same messages until they run out of retries and dead-letter.
func burn(c *client, queue string, n int, dead *atomic.Int64) {
	for i := 0; i < n; i++ {
		for attempt := 0; attempt < 4; attempt++ {
			batch := take(c, queue, 1)
			if len(batch) == 0 {
				return
			}
			c.do("POST", "/v1/queues/"+queue+"/messages/nack",
				map[string]any{"receipt": batch[0].Receipt})
		}
		dead.Add(1)
	}
}
