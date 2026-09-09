// Command seedmessages fills a set of queues with messages of mixed priority,
// so priority ordering and the metric bands have a real backlog to work on.
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

// The queues and the count are fixed here rather than passed in: this command
// does one thing.
const (
	queuePrefix = "orders"
	queueCount  = 2
	perQueue    = 10000
)

var (
	addr        = flag.String("addr", "http://localhost:8080", "gateway address")
	token       = flag.String("token", "acme-token", "bearer token, which picks the tenant")
	concurrency = flag.Int("concurrency", 7, "parallel requests")
	seed        = flag.Int64("seed", 1, "random seed, so a run is repeatable")
	reset       = flag.Bool("reset", false, "delete the queues first")
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

func queueNames() []string {
	out := make([]string, queueCount)
	for i := range out {
		out[i] = fmt.Sprintf("%s_%d", queuePrefix, i+1)
	}
	return out
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

	names := queueNames()
	fmt.Printf("%d queues x %d messages = %d\n\n", queueCount, perQueue, queueCount*perQueue)

	var totalSent, totalFailed atomic.Int64
	var totalBands [3]atomic.Int64
	start := time.Now()

	for _, name := range names {
		if *reset {
			c.do("DELETE", "/v1/queues/"+name, nil)
		}
		code, out := c.do("POST", "/v1/queues", map[string]any{
			"name":              name,
			"visibilityTimeout": "45s",
			"maxRetries":        3,
			"defaultTtl":        "6h",
			"distributed":       true,
			"placementWidth":    20,
		})
		// 409 is a queue that already exists with settings of its own. Fill it as
		// it stands rather than insisting on these.
		if code != 201 && code != 200 && code != 409 {
			fmt.Fprintf(os.Stderr, "create %q failed (%d): %s\n", name, code, out)
			os.Exit(1)
		}
		sent, failed, bands, took := fill(c, name, rnd)

		totalSent.Add(sent)
		totalFailed.Add(failed)
		for i, n := range bands {
			totalBands[i].Add(n)
		}
		line := fmt.Sprintf("  %-12s %6d sent  %5.0f/s  high %d  medium %d  low %d",
			name, sent, float64(sent)/took.Seconds(), bands[2], bands[1], bands[0])
		if failed > 0 {
			line += fmt.Sprintf("  refused %d", failed)
		}
		fmt.Println(line)
	}

	elapsed := time.Since(start)
	fmt.Printf("\nsent      %d of %d in %s (%.0f/s)\n",
		totalSent.Load(), queueCount*perQueue, elapsed.Round(time.Millisecond),
		float64(totalSent.Load())/elapsed.Seconds())
	if n := totalFailed.Load(); n > 0 {
		fmt.Printf("refused   %d\n", n)
	}
	fmt.Printf("high      %d\nmedium    %d\nlow       %d\n",
		totalBands[2].Load(), totalBands[1].Load(), totalBands[0].Load())
	fmt.Printf("\nopen %s\n", *addr)
}

// fill sends perQueue messages into one queue and reports what landed.
func fill(c *client, name string, rnd *rand.Rand) (sent, failed int64, bands [3]int64, took time.Duration) {
	work := make(chan int, *concurrency)
	go func() {
		for i := 0; i < perQueue; i++ {
			work <- i
		}
		close(work)
	}()

	var ok, bad atomic.Int64
	var counted [3]atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(rnd.Int63()))
			for i := range work {
				p := r.Intn(101)
				code, _ := c.do("POST", "/v1/queues/"+name+"/messages", map[string]any{
					"payload":  fmt.Sprintf("message_%d_%d", p, i+1),
					"priority": p,
				})
				if code != 201 {
					bad.Add(1)
					continue
				}
				ok.Add(1)
				counted[bucket(p)].Add(1)
			}
		}()
	}
	wg.Wait()

	for i := range counted {
		bands[i] = counted[i].Load()
	}
	return ok.Load(), bad.Load(), bands, time.Since(start)
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
