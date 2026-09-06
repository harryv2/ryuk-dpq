package logic

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// The log is not what these tests are about, so it only counts what it is told.
type fakeWAL struct {
	engine.NoopJournal
	enqueued  int
	compacted []uint16
	dead      []entity.PendingDeadLetter
	drained   []string
}

func (w *fakeWAL) AppendEnqueue(_ uint16, _ *engine.Message) error { w.enqueued++; return nil }
func (w *fakeWAL) Replay() (map[uint16][]*engine.Message, error)   { return nil, nil }
func (w *fakeWAL) Compact(slot uint16, _ []*engine.Message) error {
	w.compacted = append(w.compacted, slot)
	return nil
}
func (w *fakeWAL) Drop(uint16) error { return nil }

func (w *fakeWAL) AppendDeadLetter(slot uint16, m *engine.Message) error {
	w.dead = append(w.dead, entity.PendingDeadLetter{Slot: slot, Msg: m})
	return nil
}
func (w *fakeWAL) AppendDeadLetterDrained(id string) error {
	w.drained = append(w.drained, id)
	return nil
}
func (w *fakeWAL) ReplayDeadLetters() ([]entity.PendingDeadLetter, error) { return w.dead, nil }
func (w *fakeWAL) CompactDeadLetters(p []entity.PendingDeadLetter) error {
	w.dead = p
	return nil
}
func (w *fakeWAL) Close() error { return nil }

type fakeWALs struct{ opened map[engine.QueueKey]*fakeWAL }

func (f *fakeWALs) Open(k engine.QueueKey) (entity.WALRepo, error) {
	if f.opened == nil {
		f.opened = map[engine.QueueKey]*fakeWAL{}
	}
	if w, ok := f.opened[k]; ok {
		return w, nil
	}
	w := &fakeWAL{}
	f.opened[k] = w
	return w, nil
}
func (f *fakeWALs) Remove(engine.QueueKey) error     { return nil }
func (f *fakeWALs) List() ([]engine.QueueKey, error) { return nil, nil }

func newTestNode(t *testing.T) (*QueueLogic, *fakeWALs) {
	t.Helper()
	wals := &fakeWALs{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(Config{NodeID: "node-1", Incarnation: 1}, log, engine.SystemClock{}, wals), wals
}

func testSpec() entity.QueueSpec {
	return entity.QueueSpec{
		Org: "org1", Name: "orders",
		VisibilityTimeout: 30 * time.Second, MaxRetries: 3,
		Distributed: true, Generation: 1,
	}
}

func fill(t *testing.T, l *QueueLogic, spec entity.QueueSpec, slot uint16, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := l.Enqueue(entity.EnqueueRequest{
			Spec: spec, Slot: &slot, Payload: []byte("x"), Priority: 1,
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
}

func ready(t *testing.T, l *QueueLogic, spec entity.QueueSpec) int64 {
	t.Helper()
	st, err := l.Stats(spec)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	var total int64
	for _, n := range st.Ready {
		total += n
	}
	return total
}

// The whole point of the handoff: the old owner keeps everything until the new
// owner has it, and only then lets go. Nothing is lost, nothing is served twice.
func TestHandoffMovesSlotsWithoutLosingMessages(t *testing.T) {
	from, _ := newTestNode(t)
	to, _ := newTestNode(t)
	spec := testSpec()

	fill(t, from, spec, 3, 5)
	fill(t, from, spec, 7, 4)

	moving := []uint16{3, 7}
	held, err := from.PrepareMove(spec, moving)
	if err != nil {
		t.Fatal(err)
	}
	moved := 0
	for _, ms := range held {
		moved += len(ms)
	}
	if moved != 9 {
		t.Fatalf("prepared %d messages, want 9", moved)
	}
	// Held, not handed over: a failure here has to leave the old owner intact.
	if got := ready(t, from, spec); got != 9 {
		t.Fatalf("prepare removed messages: %d left", got)
	}

	if err := to.Absorb(entity.TransferRequest{MoveID: "m1", Spec: spec, BySlot: held}); err != nil {
		t.Fatal(err)
	}
	if got := ready(t, to, spec); got != 9 {
		t.Fatalf("new owner has %d of 9", got)
	}

	if err := from.DiscardMove(spec, moving); err != nil {
		t.Fatal(err)
	}
	if got := ready(t, from, spec); got != 0 {
		t.Fatalf("old owner still holds %d", got)
	}
}

// A retried Absorb must not double the messages it carries.
func TestAbsorbingTheSameMoveTwiceChangesNothing(t *testing.T) {
	from, _ := newTestNode(t)
	to, _ := newTestNode(t)
	spec := testSpec()

	fill(t, from, spec, 3, 6)
	held, err := from.PrepareMove(spec, []uint16{3})
	if err != nil {
		t.Fatal(err)
	}

	req := entity.TransferRequest{MoveID: "m1", Spec: spec, BySlot: held}
	if err := to.Absorb(req); err != nil {
		t.Fatal(err)
	}
	if err := to.Absorb(req); err != nil {
		t.Fatal(err)
	}
	if got := ready(t, to, spec); got != 6 {
		t.Fatalf("new owner holds %d, want 6", got)
	}
}

// When the handoff fails the old owner has to go back to serving, not sit on
// slots nobody else has.
func TestAbortPutsTheSlotsBackInService(t *testing.T) {
	from, _ := newTestNode(t)
	spec := testSpec()

	fill(t, from, spec, 3, 4)
	if _, err := from.PrepareMove(spec, []uint16{3}); err != nil {
		t.Fatal(err)
	}
	if _, err := from.Dequeue(entity.DequeueRequest{Spec: spec, MaxMessages: 10}); err == nil {
		if got := ready(t, from, spec); got != 4 {
			t.Fatalf("a frozen slot was served: %d ready", got)
		}
	}

	if err := from.AbortMove(spec, []uint16{3}); err != nil {
		t.Fatal(err)
	}
	resp, err := from.Dequeue(entity.DequeueRequest{Spec: spec, MaxMessages: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 4 {
		t.Fatalf("after abort, dequeued %d of 4", len(resp.Messages))
	}
}

// The node reports what it actually holds, which is how the gateway spots a
// handoff that stopped half way.
func TestHeldSlotsReportsWhatIsFrozen(t *testing.T) {
	l, _ := newTestNode(t)
	spec := testSpec()
	fill(t, l, spec, 3, 2)
	fill(t, l, spec, 9, 2)

	if _, err := l.PrepareMove(spec, []uint16{9}); err != nil {
		t.Fatal(err)
	}
	resp := l.HeldSlots()
	if len(resp.Queues) != 1 {
		t.Fatalf("reported %d queues, want 1", len(resp.Queues))
	}
	q := resp.Queues[0]
	if len(q.Slots) != 2 {
		t.Fatalf("holds slots %v, want two", q.Slots)
	}
	if len(q.Frozen) != 1 || q.Frozen[0] != 9 {
		t.Fatalf("frozen %v, want [9]", q.Frozen)
	}
}

// The difference between the two handoffs, side by side.
func TestPrepareLeavesTheOldOwnerHoldingTheMessages(t *testing.T) {
	spec := testSpec()
	spec.Distributed = false

	all := make([]uint16, 16)
	for i := range all {
		all[i] = uint16(i)
	}

	prepared, _ := newTestNode(t)
	frozen, _ := newTestNode(t)
	for _, n := range []*QueueLogic{prepared, frozen} {
		fill(t, n, spec, 2, 10)
		fill(t, n, spec, 9, 10)
	}

	if _, err := prepared.PrepareMove(spec, all); err != nil {
		t.Fatal(err)
	}
	if got := ready(t, prepared, spec); got != 20 {
		t.Fatalf("prepare left %d of 20 messages on the old owner; a gateway dying "+
			"here would take the rest with it", got)
	}

	if _, err := frozen.Freeze(spec, nil); err != nil {
		t.Fatal(err)
	}
	if got := ready(t, frozen, spec); got != 0 {
		t.Fatalf("freeze is supposed to drain, but left %d", got)
	}
}

// And once the move commits, the old owner does let go.
func TestDiscardAfterAWholeQueueMoveReleasesEverything(t *testing.T) {
	spec := testSpec()
	spec.Distributed = false
	all := make([]uint16, 16)
	for i := range all {
		all[i] = uint16(i)
	}

	from, _ := newTestNode(t)
	to, _ := newTestNode(t)
	fill(t, from, spec, 2, 10)
	fill(t, from, spec, 9, 10)

	held, err := from.PrepareMove(spec, all)
	if err != nil {
		t.Fatal(err)
	}
	if err := to.Absorb(entity.TransferRequest{MoveID: "m1", Spec: spec, BySlot: held}); err != nil {
		t.Fatal(err)
	}
	if got := ready(t, to, spec); got != 20 {
		t.Fatalf("new owner has %d of 20", got)
	}
	if err := from.DiscardMove(spec, all); err != nil {
		t.Fatal(err)
	}
	if got := ready(t, from, spec); got != 0 {
		t.Fatalf("old owner still holds %d after the move committed", got)
	}
}

// The gateway owns the slot decision, so a request without one is a bug rather
// than something to guess at.
func TestEnqueueWithoutASlotIsRefused(t *testing.T) {
	node, _ := newTestNode(t)
	if _, err := node.Enqueue(entity.EnqueueRequest{
		Spec: testSpec(), Payload: []byte("x"), Priority: 1,
	}); enterr.CodeOf(err) != enterr.CodeInvalid {
		t.Fatalf("want an invalid-request error, got %v", err)
	}
}
