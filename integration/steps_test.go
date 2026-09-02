package integration

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cucumber/godog"

	"github.com/harryv2/ryuk-dpq/integration/client"
)

// Every scenario gets a queue name of its own, so they share one running
// cluster without sharing state.
var counter atomic.Int64

type world struct {
	queue    string
	dlq      string
	clients  map[string]*client.Client
	err      error
	sent     []string
	got      []client.Message
	held     []client.Message
	stopped  string
	restarts []string
	before   int
}

func (w *world) c(org string) *client.Client {
	if w.clients[org] == nil {
		w.clients[org] = clientFor(org)
	}
	return w.clients[org]
}

func (w *world) acme() *client.Client { return w.c("acme") }

func InitializeScenario(sc *godog.ScenarioContext) {
	w := &world{clients: map[string]*client.Client{}}

	sc.Before(func(ctx context.Context, s *godog.Scenario) (context.Context, error) {
		*w = world{clients: map[string]*client.Client{}}
		w.queue = fmt.Sprintf("it-%d", counter.Add(1))
		return ctx, nil
	})

	sc.After(func(ctx context.Context, s *godog.Scenario, err error) (context.Context, error) {
		// A stopped container has to come back or every later scenario inherits
		// a smaller cluster than it asked for.
		for _, name := range w.restarts {
			_ = stack.StartContainer(ctx, name)
		}
		if len(w.restarts) > 0 {
			_ = stack.WaitReady(ctx, *nodes, 60*time.Second)
		}
		for org := range w.clients {
			_ = w.c(org).DeleteQueue(ctx, w.queue)
		}
		if w.dlq != "" {
			_ = w.acme().DeleteQueue(ctx, w.dlq)
		}
		return ctx, nil
	})

	// ---- creating queues ----

	sc.Step(`^I (?:have )?create[d]? a queue$`, func(ctx context.Context) error {
		w.err = w.acme().CreateQueue(ctx, client.CreateQueue{Name: w.queue})
		return w.err
	})

	sc.Step(`^I (?:have )?create[d]? a distributed queue$`, func(ctx context.Context) error {
		w.err = w.acme().CreateQueue(ctx, client.CreateQueue{Name: w.queue, Distributed: true})
		return w.err
	})

	sc.Step(`^I (?:have )?create[d]? a queue as "([^"]*)"$`, func(ctx context.Context, org string) error {
		w.err = w.c(org).CreateQueue(ctx, client.CreateQueue{Name: w.queue})
		return w.err
	})

	sc.Step(`^I create a queue with the same name as "([^"]*)"$`, func(ctx context.Context, org string) error {
		w.err = w.c(org).CreateQueue(ctx, client.CreateQueue{Name: w.queue})
		return nil
	})

	sc.Step(`^I have created a queue with the same name as "([^"]*)"$`, func(ctx context.Context, org string) error {
		return w.c(org).CreateQueue(ctx, client.CreateQueue{Name: w.queue})
	})

	sc.Step(`^I create the same queue again$`, func(ctx context.Context) error {
		w.err = w.acme().CreateQueue(ctx, client.CreateQueue{Name: w.queue})
		return nil
	})

	sc.Step(`^I have created a queue with a (\d+)s visibility timeout$`, func(ctx context.Context, s int) error {
		return w.acme().CreateQueue(ctx, client.CreateQueue{
			Name: w.queue, VisibilityTimeout: fmt.Sprintf("%ds", s),
		})
	})

	sc.Step(`^I have created a queue with (\d+) retries$`, func(ctx context.Context, n int) error {
		r := uint32(n)
		return w.acme().CreateQueue(ctx, client.CreateQueue{
			Name: w.queue, MaxRetries: &r, VisibilityTimeout: "2s",
		})
	})

	sc.Step(`^I have created a queue named "([^"]*)"$`, func(ctx context.Context, suffix string) error {
		w.dlq = w.queue + "-" + suffix
		return w.acme().CreateQueue(ctx, client.CreateQueue{Name: w.dlq})
	})

	sc.Step(`^I create a queue with "([^"]*)" set to "([^"]*)"$`, func(ctx context.Context, field, value string) error {
		req := client.CreateQueue{Name: w.queue}
		switch field {
		case "name":
			req.Name = value
		case "defaultTtl":
			req.DefaultTTL = value
		case "starvationThreshold":
			req.DefaultTTL = "1h"
			req.StarvationThreshold = value
		default:
			return fmt.Errorf("no such field %q", field)
		}
		w.err = w.acme().CreateQueue(ctx, req)
		return nil
	})

	sc.Step(`^I create a queue whose dead-letter queue does not exist$`, func(ctx context.Context) error {
		w.err = w.acme().CreateQueue(ctx, client.CreateQueue{
			Name: w.queue, DeadLetterQueue: w.queue + "-nowhere",
		})
		return nil
	})

	sc.Step(`^I create a queue with "dlq" as its dead-letter queue$`, func(ctx context.Context) error {
		w.err = w.acme().CreateQueue(ctx, client.CreateQueue{
			Name: w.queue, DeadLetterQueue: w.dlq,
		})
		return nil
	})

	sc.Step(`^I delete the queue$`, func(ctx context.Context) error {
		w.err = w.acme().DeleteQueue(ctx, w.queue)
		return w.err
	})

	// ---- outcomes of a request ----

	sc.Step(`^the request succeeds$`, func() error {
		if w.err != nil {
			return fmt.Errorf("expected success, got %v", w.err)
		}
		return nil
	})

	sc.Step(`^the request is rejected with (\d+)$`, func(code int) error {
		if w.err == nil {
			return fmt.Errorf("expected %d, the request succeeded", code)
		}
		if got := client.StatusOf(w.err); got != code {
			return fmt.Errorf("expected %d, got %d (%v)", code, got, w.err)
		}
		return nil
	})

	sc.Step(`^the error mentions "([^"]*)"$`, func(want string) error {
		if w.err == nil {
			return fmt.Errorf("there was no error to check")
		}
		if !strings.Contains(w.err.Error(), want) {
			return fmt.Errorf("error %q does not mention %q", w.err.Error(), want)
		}
		return nil
	})

	sc.Step(`^the queue exists$`, func(ctx context.Context) error {
		_, err := w.acme().Stats(ctx, w.queue)
		return err
	})

	sc.Step(`^the queue is gone$`, func(ctx context.Context) error {
		_, err := w.acme().Stats(ctx, w.queue)
		if client.StatusOf(err) != 404 {
			return fmt.Errorf("expected the queue to be gone, got %v", err)
		}
		return nil
	})

	sc.Step(`^"([^"]*)" asks for that queue$`, func(ctx context.Context, org string) error {
		_, w.err = w.c(org).Stats(ctx, w.queue)
		return nil
	})

	sc.Step(`^it is a single-node queue$`, func(ctx context.Context) error {
		s, err := w.acme().Stats(ctx, w.queue)
		if err != nil {
			return err
		}
		if s.Distributed {
			return fmt.Errorf("the queue reports itself as distributed")
		}
		return nil
	})

	sc.Step(`^it has an owner node$`, func(ctx context.Context) error {
		s, err := w.acme().Stats(ctx, w.queue)
		if err != nil {
			return err
		}
		if s.OwnerNode == "" {
			return fmt.Errorf("no owner was assigned")
		}
		return nil
	})

	// ---- sending ----

	sc.Step(`^I send these messages:$`, func(ctx context.Context, table *godog.Table) error {
		head := headings(table)
		for _, row := range table.Rows[1:] {
			cell := func(name string) string {
				if i, ok := head[name]; ok {
					return row.Cells[i].Value
				}
				return ""
			}
			payload := cell("payload")
			if _, err := w.acme().Enqueue(ctx, w.queue, client.Enqueue{
				Payload: payload, Priority: cell("priority"), GroupID: cell("group"),
			}); err != nil {
				return err
			}
			w.sent = append(w.sent, payload)
		}
		return nil
	})

	sc.Step(`^I send (\d+) messages at priority "([^"]*)"$`, func(ctx context.Context, n int, p string) error {
		return w.send(ctx, "acme", n, p, func(int) string { return "" })
	})

	sc.Step(`^I send (\d+) message at priority "([^"]*)"$`, func(ctx context.Context, n int, p string) error {
		return w.send(ctx, "acme", n, p, func(int) string { return "" })
	})

	sc.Step(`^I send (\d+) messages at priority "([^"]*)" as "([^"]*)"$`,
		func(ctx context.Context, n int, p, org string) error {
			return w.send(ctx, org, n, p, func(int) string { return "" })
		})

	sc.Step(`^I send (\d+) messages? to group "([^"]*)"$`, func(ctx context.Context, n int, g string) error {
		return w.send(ctx, "acme", n, "MEDIUM", func(int) string { return g })
	})

	sc.Step(`^I send (\d+) ordered messages to group "([^"]*)"$`, func(ctx context.Context, n int, g string) error {
		return w.send(ctx, "acme", n, "MEDIUM", func(int) string { return g })
	})

	sc.Step(`^I send (\d+) messages across (\d+) groups$`, func(ctx context.Context, n, groups int) error {
		return w.send(ctx, "acme", n, "MEDIUM", func(i int) string {
			return fmt.Sprintf("g%d", i%groups)
		})
	})

	sc.Step(`^I send a message delayed by (\d+) seconds$`, func(ctx context.Context, s int) error {
		_, err := w.acme().Enqueue(ctx, w.queue, client.Enqueue{
			Payload: "later", Priority: "HIGH", DeliverAfter: fmt.Sprintf("%ds", s),
		})
		return err
	})

	// ---- taking ----

	sc.Step(`^I drain the queue$`, func(ctx context.Context) error {
		got, err := w.acme().DrainAll(ctx, w.queue, 500)
		w.got = got
		return err
	})

	sc.Step(`^I take (\d+) messages without acknowledging them$`, func(ctx context.Context, n int) error {
		got, err := w.acme().Dequeue(ctx, w.queue, n)
		w.got = got
		return err
	})

	sc.Step(`^I take and acknowledge messages one at a time$`, func(ctx context.Context) error {
		for i := 0; i < 20; i++ {
			batch, err := w.acme().Dequeue(ctx, w.queue, 1)
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				break
			}
			w.got = append(w.got, batch[0])
			if err := w.acme().Ack(ctx, w.queue, batch[0].Receipt); err != nil {
				return err
			}
		}
		return nil
	})

	sc.Step(`^I take a message and abandon it$`, func(ctx context.Context) error {
		batch, err := w.acme().Dequeue(ctx, w.queue, 1)
		if err != nil {
			return err
		}
		if len(batch) != 1 {
			return fmt.Errorf("expected a message, got %d", len(batch))
		}
		w.held = batch
		return nil
	})

	sc.Step(`^I take a message and nack it$`, func(ctx context.Context) error {
		batch, err := w.acme().Dequeue(ctx, w.queue, 1)
		if err != nil {
			return err
		}
		if len(batch) != 1 {
			return fmt.Errorf("expected a message, got %d", len(batch))
		}
		return w.acme().Nack(ctx, w.queue, batch[0].Receipt)
	})

	sc.Step(`^I take and nack the message (\d+) times$`, func(ctx context.Context, n int) error {
		for i := 0; i < n; i++ {
			batch, err := w.acme().Dequeue(ctx, w.queue, 1)
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				return fmt.Errorf("no message on attempt %d of %d", i+1, n)
			}
			if err := w.acme().Nack(ctx, w.queue, batch[0].Receipt); err != nil {
				return err
			}
		}
		return nil
	})

	sc.Step(`^I wait (\d+) seconds?$`, func(s int) error {
		time.Sleep(time.Duration(s) * time.Second)
		return nil
	})

	// ---- assertions on what came back ----

	sc.Step(`^the delivery order is "([^"]*)"$`, func(want string) error {
		var got []string
		for _, m := range w.got {
			got = append(got, m.Text())
		}
		expected := splitList(want)
		if strings.Join(got, ",") != strings.Join(expected, ",") {
			return fmt.Errorf("delivered %v, want %v", got, expected)
		}
		return nil
	})

	sc.Step(`^the messages come back in the order they were sent$`, func() error {
		if len(w.got) != len(w.sent) {
			return fmt.Errorf("sent %d, got %d back", len(w.sent), len(w.got))
		}
		for i, m := range w.got {
			if m.Text() != w.sent[i] {
				return fmt.Errorf("position %d: got %q, want %q", i, m.Text(), w.sent[i])
			}
		}
		return nil
	})

	sc.Step(`^the messages of group "([^"]*)" came back in order$`, func(group string) error {
		var seen []string
		for _, m := range w.got {
			if m.GroupID == group {
				seen = append(seen, m.Text())
			}
		}
		var want []string
		for _, p := range w.sent {
			want = append(want, p)
		}
		if len(seen) != len(want) {
			return fmt.Errorf("group %s: got %d messages, sent %d", group, len(seen), len(want))
		}
		for i := range seen {
			if seen[i] != want[i] {
				return fmt.Errorf("group %s out of order at %d: %q after %v", group, i, seen[i], want[:i])
			}
		}
		return nil
	})

	sc.Step(`^I received (\d+) messages? in order$`, func(n int) error {
		if len(w.got) != n {
			return fmt.Errorf("received %d messages, want %d", len(w.got), n)
		}
		for i, m := range w.got {
			if m.Text() != w.sent[i] {
				return fmt.Errorf("position %d: got %q, want %q", i, m.Text(), w.sent[i])
			}
		}
		return nil
	})

	sc.Step(`^I received (\d+) messages?$`, func(n int) error {
		if len(w.got) != n {
			return fmt.Errorf("received %d messages, want %d", len(w.got), n)
		}
		return nil
	})

	sc.Step(`^no message was delivered twice$`, func() error {
		seen := map[string]bool{}
		for _, m := range w.got {
			if seen[m.MessageID] {
				return fmt.Errorf("message %s was delivered twice", m.MessageID)
			}
			seen[m.MessageID] = true
		}
		return nil
	})

	sc.Step(`^the message can be taken again$`, func(ctx context.Context) error {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			batch, err := w.acme().Dequeue(ctx, w.queue, 1)
			if err != nil {
				return err
			}
			if len(batch) == 1 {
				w.got = batch
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
		return fmt.Errorf("the message never came back")
	})

	sc.Step(`^its attempt count is (\d+)$`, func(n int) error {
		if len(w.got) == 0 {
			return fmt.Errorf("no message to check")
		}
		if got := int(w.got[0].Attempts); got != n {
			return fmt.Errorf("attempt count is %d, want %d", got, n)
		}
		return nil
	})

	sc.Step(`^no message is available immediately$`, func(ctx context.Context) error {
		batch, err := w.acme().Dequeue(ctx, w.queue, 1)
		if err != nil {
			return err
		}
		if len(batch) != 0 {
			return fmt.Errorf("a delayed message was delivered early")
		}
		return nil
	})

	sc.Step(`^the message arrives within (\d+) seconds$`, func(ctx context.Context, s int) error {
		deadline := time.Now().Add(time.Duration(s) * time.Second)
		for time.Now().Before(deadline) {
			batch, err := w.acme().Dequeue(ctx, w.queue, 1)
			if err != nil {
				return err
			}
			if len(batch) == 1 {
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
		return fmt.Errorf("the delayed message never arrived")
	})

	// ---- assertions on stats ----

	sc.Step(`^the queue reports (\d+) ready messages?$`, func(ctx context.Context, n int) error {
		return eventually(20*time.Second, func() error {
			s, err := w.acme().Stats(ctx, w.queue)
			if err != nil {
				return err
			}
			if s.Messages != int64(n) {
				return fmt.Errorf("ready is %d, want %d", s.Messages, n)
			}
			return nil
		})
	})

	sc.Step(`^the queue still holds (\d+) messages$`, func(ctx context.Context, n int) error {
		return eventually(40*time.Second, func() error {
			s, err := w.acme().Stats(ctx, w.queue)
			if err != nil {
				return err
			}
			if total := s.Messages + s.InFlight; total != int64(n) {
				return fmt.Errorf("holds %d, want %d", total, n)
			}
			return nil
		})
	})

	sc.Step(`^the queue reports (\d+) dead-lettered messages?$`, func(ctx context.Context, n int) error {
		return eventually(20*time.Second, func() error {
			s, err := w.acme().Stats(ctx, w.queue)
			if err != nil {
				return err
			}
			if s.DeadLettered != uint64(n) {
				return fmt.Errorf("dead-lettered is %d, want %d", s.DeadLettered, n)
			}
			return nil
		})
	})

	sc.Step(`^the queue reports (\d+) delayed messages?$`, func(ctx context.Context, n int) error {
		s, err := w.acme().Stats(ctx, w.queue)
		if err != nil {
			return err
		}
		if s.Delayed != int64(n) {
			return fmt.Errorf("delayed is %d, want %d", s.Delayed, n)
		}
		return nil
	})

	sc.Step(`^the queue holds no ready messages$`, func(ctx context.Context) error {
		s, err := w.acme().Stats(ctx, w.queue)
		if err != nil {
			return err
		}
		if s.Messages != 0 {
			return fmt.Errorf("%d messages are still ready", s.Messages)
		}
		return nil
	})

	sc.Step(`^the counts are marked as a point-in-time sum$`, func(ctx context.Context) error {
		s, err := w.acme().Stats(ctx, w.queue)
		if err != nil {
			return err
		}
		if s.Exact {
			return fmt.Errorf("a distributed queue should not claim exact counts")
		}
		return nil
	})

	sc.Step(`^the counts are marked exact$`, func(ctx context.Context) error {
		s, err := w.acme().Stats(ctx, w.queue)
		if err != nil {
			return err
		}
		if !s.Exact {
			return fmt.Errorf("a single-node queue should report exact counts")
		}
		return nil
	})

	sc.Step(`^"([^"]*)" sees (\d+) ready messages?$`, func(ctx context.Context, org string, n int) error {
		return eventually(20*time.Second, func() error {
			s, err := w.c(org).Stats(ctx, w.queue)
			if err != nil {
				return err
			}
			if s.Messages != int64(n) {
				return fmt.Errorf("%s sees %d, want %d", org, s.Messages, n)
			}
			return nil
		})
	})

	sc.Step(`^the stats report unavailable slots$`, func(ctx context.Context) error {
		return eventually(40*time.Second, func() error {
			s, err := w.acme().Stats(ctx, w.queue)
			if err != nil {
				return err
			}
			if s.UnavailableSlots == 0 {
				return fmt.Errorf("no slots are reported unavailable")
			}
			return nil
		})
	})

	// ---- placement ----

	sc.Step(`^its slots are spread over more than one node$`, func(ctx context.Context) error {
		return eventually(20*time.Second, func() error {
			holders, _, err := w.placement(ctx)
			if err != nil {
				return err
			}
			if len(holders) < 2 {
				return fmt.Errorf("the queue is on %d node(s)", len(holders))
			}
			return nil
		})
	})

	sc.Step(`^every slot belongs to a live node$`, func(ctx context.Context) error {
		holders, total, err := w.placement(ctx)
		if err != nil {
			return err
		}
		placed := 0
		for _, n := range holders {
			placed += n
		}
		if placed != total {
			return fmt.Errorf("%d of %d slots are placed on a live node", placed, total)
		}
		return nil
	})

	sc.Step(`^I add (\d+) nodes to the cluster$`, func(ctx context.Context, extra int) error {
		holders, _, err := w.placement(ctx)
		if err != nil {
			return err
		}
		w.before = len(holders)
		return stack.Scale(ctx, *nodes+extra)
	})

	sc.Step(`^the queue's slots spread onto the new nodes$`, func(ctx context.Context) error {
		return eventually(90*time.Second, func() error {
			holders, _, err := w.placement(ctx)
			if err != nil {
				return err
			}
			if len(holders) <= w.before {
				return fmt.Errorf("still on %d nodes, was on %d", len(holders), w.before)
			}
			return nil
		})
	})

	sc.Step(`^a node holding some of its slots goes down$`, func(ctx context.Context) error {
		holders, _, err := w.placement(ctx)
		if err != nil {
			return err
		}
		members, err := w.acme().Cluster(ctx)
		if err != nil {
			return err
		}
		// Stopping a node that holds nothing of this queue would prove nothing.
		index := -1
		for i, m := range members {
			if holders[m.ID] > 0 {
				index = i
				break
			}
		}
		if index < 0 {
			return fmt.Errorf("no live node holds a slot of %s", w.queue)
		}
		name, err := stack.StopNode(ctx, index)
		if err != nil {
			return err
		}
		w.stopped = name
		w.restarts = append(w.restarts, name)
		return nil
	})

	sc.Step(`^the queue still serves messages from the surviving nodes$`, func(ctx context.Context) error {
		return eventually(60*time.Second, func() error {
			batch, err := w.acme().Dequeue(ctx, w.queue, 5)
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				return fmt.Errorf("nothing came back from the surviving nodes")
			}
			for _, m := range batch {
				_ = w.acme().Ack(ctx, w.queue, m.Receipt)
			}
			return nil
		})
	})

	sc.Step(`^the queue's owner is restarted$`, func(ctx context.Context) error {
		s, err := w.acme().Stats(ctx, w.queue)
		if err != nil {
			return err
		}
		nodesNow, err := w.acme().Cluster(ctx)
		if err != nil {
			return err
		}
		index := -1
		for i, n := range nodesNow {
			if n.ID == s.OwnerNode {
				index = i
			}
		}
		if index < 0 {
			return fmt.Errorf("owner %s is not in the member list", s.OwnerNode)
		}
		name, err := stack.StopNode(ctx, index)
		if err != nil {
			return err
		}
		if err := stack.StartContainer(ctx, name); err != nil {
			return err
		}
		return stack.WaitReady(ctx, *nodes, 60*time.Second)
	})

	sc.Step(`^the messages can still be drained$`, func(ctx context.Context) error {
		return eventually(60*time.Second, func() error {
			got, err := w.acme().DrainAll(ctx, w.queue, 500)
			if err != nil {
				return err
			}
			if len(got) != len(w.sent) {
				return fmt.Errorf("drained %d of %d", len(got), len(w.sent))
			}
			return nil
		})
	})
}

func (w *world) send(ctx context.Context, org string, n int, priority string, group func(int) string) error {
	for i := 0; i < n; i++ {
		payload := fmt.Sprintf("m%03d", i)
		if _, err := w.c(org).Enqueue(ctx, w.queue, client.Enqueue{
			Payload: payload, Priority: priority, GroupID: group(i),
		}); err != nil {
			return err
		}
		w.sent = append(w.sent, payload)
	}
	return nil
}

// placement reports how many slots of this queue each node holds, and how many
// slots the queue has in total.
func (w *world) placement(ctx context.Context) (map[string]int, int, error) {
	nodesNow, err := w.acme().Cluster(ctx)
	if err != nil {
		return nil, 0, err
	}
	holders := map[string]int{}
	total := 0
	for _, n := range nodesNow {
		for _, p := range n.Queues {
			if p.Queue == w.queue {
				holders[n.ID] += p.Slots
				total = p.TotalSlots
			}
		}
	}
	if total == 0 {
		return nil, 0, fmt.Errorf("the queue is not placed anywhere yet")
	}
	return holders, total, nil
}

// eventually retries an assertion until it passes. Placement and metric
// collection are both periodic, so a single read can be a moment early.
func eventually(within time.Duration, check func() error) error {
	deadline := time.Now().Add(within)
	var last error
	for {
		if last = check(); last == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("still failing after %s: %w", within, last)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func headings(t *godog.Table) map[string]int {
	out := map[string]int{}
	for i, c := range t.Rows[0].Cells {
		out[c.Value] = i
	}
	return out
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
