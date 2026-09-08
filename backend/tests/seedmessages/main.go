// Command seedmessages fills one queue with messages of mixed priority, so
// priority ordering and the metric bands have a real backlog to work on.
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
	"sync"
	"sync/atomic"
	"time"
)

// The queue and the count are fixed here rather than passed in: this command
// does one thing.
const (
	queueName = "orders_2"
	total     = 100000
)

var (
	addr        = flag.String("addr", "http://localhost:8090", "gateway address")
	token       = flag.String("token", "acme-token", "bearer token, which picks the tenant")
	concurrency = flag.Int("concurrency", 24, "parallel requests")
	seed        = flag.Int64("seed", 1, "random seed, so a run is repeatable")
	reset       = flag.Bool("reset", false, "delete the queue first")
)

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

func main() {
	flag.Parse()
	rnd := rand.New(rand.NewSource(*seed))

	tr := &http.Transport{MaxIdleConnsPerHost: *concurrency * 2}
	c := &client{http: &http.Client{Transport: tr, Timeout: 30 * time.Second}, token: *token}

	if code, _ := c.do("GET", "/v1/queues", nil); code != 200 {
		fmt.Fprintf(os.Stderr, "cannot reach the gateway at %s — is it running? (make up)\n", *addr)
		os.Exit(1)
	}
	if *reset {
		c.do("DELETE", "/v1/queues/"+queueName, nil)
	}
	code, out := c.do("POST", "/v1/queues", map[string]any{
		"name":              queueName,
		"visibilityTimeout": "45s",
		"maxRetries":        3,
		"defaultTtl":        "6h",
		"distributed":       true,
		"placementWidth":    20,
	})
	// 409 is a queue that already exists with settings of its own. Fill it as it
	// stands rather than insisting on these.
	if code != 201 && code != 200 && code != 409 {
		fmt.Fprintf(os.Stderr, "create %q failed (%d): %s\n", queueName, code, out)
		os.Exit(1)
	}

	work := make(chan int, *concurrency)
	go func() {
		for i := 0; i < total; i++ {
			work <- i
		}
		close(work)
	}()

	var sent, failed atomic.Int64
	var bands [3]atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(rnd.Int63()))
			for i := range work {
				p := r.Intn(101)
				code, _ := c.do("POST", "/v1/queues/"+queueName+"/messages", map[string]any{
					"payload":  fmt.Sprintf("message_%d_%d", p, i+1),
					"priority": p,
				})
				if code != 201 {
					failed.Add(1)
					continue
				}
				sent.Add(1)
				bands[bucket(p)].Add(1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("queue     %s\n", queueName)
	fmt.Printf("sent      %d of %d in %s (%.0f/s)\n",
		sent.Load(), total, elapsed.Round(time.Millisecond),
		float64(sent.Load())/elapsed.Seconds())
	if n := failed.Load(); n > 0 {
		fmt.Printf("refused   %d\n", n)
	}
	fmt.Printf("high      %d\n", bands[2].Load())
	fmt.Printf("medium    %d\n", bands[1].Load())
	fmt.Printf("low       %d\n", bands[0].Load())
	fmt.Printf("\nopen %s\n", *addr)
}

// bucket is the same split the queue reports its counts in.
func bucket(p int) int {
	switch {
	case p <= 33:
		return 0
	case p <= 66:
		return 1
	default:
		return 2
	}
}
