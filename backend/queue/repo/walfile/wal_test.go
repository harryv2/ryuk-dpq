package walfile

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

func openTemp(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, Sync: SyncNever}, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, dir
}

func msg(id string, seq uint64) *engine.Message {
	return &engine.Message{
		ID:         id,
		Payload:    []byte("hello " + id),
		Priority:   engine.Medium,
		GroupID:    "g",
		Seq:        seq,
		EnqueuedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
}

func TestAppendAndReplay(t *testing.T) {
	w, _ := openTemp(t)
	for i := 0; i < 5; i++ {
		if err := w.AppendEnqueue(3, msg(string(rune('a'+i)), uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	w.AppendTerminal(3, "b", engine.TerminalAck)
	w.AppendAttempt(3, "c", 2, 7)

	got, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	msgs := got[3]
	if len(msgs) != 4 {
		t.Fatalf("replayed %d messages, want 4", len(msgs))
	}
	for _, m := range msgs {
		if m.ID == "b" {
			t.Fatal("acknowledged message came back")
		}
		if m.ID == "c" && m.Attempts != 2 {
			t.Fatalf("attempts = %d, want 2", m.Attempts)
		}
	}
}

func TestReplayPreservesOrder(t *testing.T) {
	w, _ := openTemp(t)
	ids := []string{"m1", "m2", "m3", "m4"}
	for i, id := range ids {
		if err := w.AppendEnqueue(0, msg(id, uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := w.Replay()
	for i, m := range got[0] {
		if m.ID != ids[i] {
			t.Fatalf("position %d: got %s, want %s", i, m.ID, ids[i])
		}
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	w, dir := openTemp(t)
	for i := 0; i < 3; i++ {
		if err := w.AppendEnqueue(1, msg(string(rune('a'+i)), uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()

	// simulate a crash mid-write
	p := filepath.Join(dir, "slot-0001.log")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, append(b, 0x11, 0x22, 0x33), 0o644); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(Options{Dir: dir, Sync: SyncNever}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	got, err := w2.Replay()
	if err != nil {
		t.Fatalf("replay after torn write: %v", err)
	}
	if len(got[1]) != 3 {
		t.Fatalf("replayed %d, want 3", len(got[1]))
	}

	// the torn bytes must be gone so the next append starts clean
	if err := w2.AppendEnqueue(1, msg("d", 3)); err != nil {
		t.Fatal(err)
	}
	got, err = w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got[1]) != 4 {
		t.Fatalf("after re-append: %d, want 4", len(got[1]))
	}
}

func TestCorruptPayloadStopsReplay(t *testing.T) {
	w, dir := openTemp(t)
	_ = w.AppendEnqueue(2, msg("a", 0))
	_ = w.AppendEnqueue(2, msg("b", 1))
	_ = w.Close()

	p := filepath.Join(dir, "slot-0002.log")
	b, _ := os.ReadFile(p)
	b[len(b)-1] ^= 0xff // flip a bit in the last record's payload
	_ = os.WriteFile(p, b, 0o644)

	w2, _ := Open(Options{Dir: dir, Sync: SyncNever}, 2)
	defer w2.Close()
	got, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got[2]) != 1 || got[2][0].ID != "a" {
		t.Fatalf("want only the first record, got %d", len(got[2]))
	}
}

func TestCompactDropsDeadRecords(t *testing.T) {
	w, _ := openTemp(t)
	for i := 0; i < 10; i++ {
		_ = w.AppendEnqueue(4, msg(string(rune('a'+i)), uint64(i)))
	}
	marks, err := w.Marks()
	if err != nil {
		t.Fatal(err)
	}
	live, _ := w.Replay()
	keep := live[4][:3]

	if err := w.Compact(4, keep, marks[4]); err != nil {
		t.Fatal(err)
	}
	got, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got[4]) != 3 {
		t.Fatalf("after compact: %d, want 3", len(got[4]))
	}
}

func TestDropRemovesSlot(t *testing.T) {
	w, _ := openTemp(t)
	_ = w.AppendEnqueue(5, msg("a", 0))
	if err := w.Drop(5); err != nil {
		t.Fatal(err)
	}
	got, _ := w.Replay()
	if len(got[5]) != 0 {
		t.Fatal("slot survived drop")
	}
}

// The point of writing dead letters down: a node that restarts before the
// gateway drains it must still know it owes those messages.
func TestDeadLettersSurviveARestart(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, Sync: SyncNever}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range []*engine.Message{msg("a", 1), msg("b", 2), msg("c", 3)} {
		if err := w.AppendDeadLetter(uint16(i), m); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.AppendDeadLetterDrained("b"); err != nil { // the gateway took this one
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(Options{Dir: dir, Sync: SyncNever}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	pending, err := w2.ReplayDeadLetters()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("recovered %d dead letters, want a and c", len(pending))
	}
	if pending[0].Msg.ID != "a" || pending[0].Slot != 0 {
		t.Fatalf("first is %+v, want a in slot 0", pending[0])
	}
	if pending[1].Msg.ID != "c" || pending[1].Slot != 2 {
		t.Fatalf("second is %+v, want c in slot 2", pending[1])
	}
}

// Compaction drops the drained records, which are most of the file on a busy
// queue, without losing what is still outstanding.
func TestCompactDeadLettersKeepsWhatIsOutstanding(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, Sync: SyncNever}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 50; i++ {
		if err := w.AppendDeadLetter(1, msg(fmt.Sprintf("m%d", i), uint64(i))); err != nil {
			t.Fatal(err)
		}
		if i < 48 {
			if err := w.AppendDeadLetterDrained(fmt.Sprintf("m%d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	pending, err := w.ReplayDeadLetters()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 outstanding, got %d", len(pending))
	}

	before := fileSize(t, filepath.Join(dir, deadLetterFile))
	if err := w.CompactDeadLetters(pending); err != nil {
		t.Fatal(err)
	}
	after := fileSize(t, filepath.Join(dir, deadLetterFile))
	if after >= before {
		t.Fatalf("compaction did not shrink the log: %d -> %d", before, after)
	}

	again, err := w.ReplayDeadLetters()
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 2 || again[0].Msg.ID != "m48" || again[1].Msg.ID != "m49" {
		t.Fatalf("after compaction: %+v", again)
	}
}

func fileSize(t *testing.T, p string) int64 {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// Compaction runs on a timer while producers are appending. An append that
// returned nil has been reported as durable, so it has to still be in the log
// afterwards -- and the slot has to keep working once the swap is done.
func TestCompactKeepsAppendsThatLandedDuringIt(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, Sync: SyncNever}, 1)
	if err != nil {
		t.Fatal(err)
	}

	const slot = 0
	var mu sync.Mutex
	accepted := map[string]bool{}
	var failed []error

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				select {
				case <-stop:
					return
				default:
				}
				m := msg(fmt.Sprintf("p%d-%d", p, i), uint64(i))
				err := w.AppendEnqueue(slot, m)
				mu.Lock()
				if err != nil {
					failed = append(failed, err)
				} else {
					accepted[m.ID] = true
				}
				mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}(p)
	}

	for i := 0; i < 12; i++ {
		time.Sleep(5 * time.Millisecond)
		// The mark comes before the list, the way the sweeper takes it.
		marks, err := w.Marks()
		if err != nil {
			t.Fatalf("marks: %v", err)
		}
		mu.Lock()
		live := make([]*engine.Message, 0, len(accepted))
		for id := range accepted {
			live = append(live, msg(id, 0))
		}
		mu.Unlock()
		if err := w.Compact(slot, live, marks[slot]); err != nil {
			t.Fatalf("compact: %v", err)
		}
	}
	close(stop)
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(failed) > 0 {
		t.Errorf("%d appends failed while compaction ran, first: %v", len(failed), failed[0])
	}

	w2, err := Open(Options{Dir: dir, Sync: SyncNever}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	replayed, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, m := range replayed[slot] {
		have[m.ID] = true
	}
	missing := 0
	for id := range accepted {
		if !have[id] {
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("%d of %d accepted appends are not in the log after replay",
			missing, len(accepted))
	}
}

// flushes reports how many fsyncs a slot has performed, so a test can tell a
// batched commit from one flush per append.
func flushes(t *testing.T, w *WAL, id uint16) uint64 {
	t.Helper()
	sf, err := w.slot(id)
	if err != nil {
		t.Fatal(err)
	}
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return sf.flushes
}

// An enqueue that has been acknowledged has to be on the disk, not just in the
// page cache, or a host losing power loses messages the producer was told were
// safe.
func TestAlwaysFlushesBeforeTheAppendReturns(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, Sync: SyncAlways}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 5; i++ {
		if err := w.AppendEnqueue(0, msg(fmt.Sprintf("m%d", i), uint64(i))); err != nil {
			t.Fatal(err)
		}
		sf, err := w.slot(0)
		if err != nil {
			t.Fatal(err)
		}
		sf.mu.Lock()
		synced, written := sf.synced, sf.written
		sf.mu.Unlock()
		if synced < written {
			t.Fatalf("append %d returned with %d of %d writes flushed", i, synced, written)
		}
	}
}

// The attempt and terminal records are not worth a flush each: losing one costs
// a redelivery, which at-least-once already allows.
func TestOnlyEnqueuesPayForAFlush(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, Sync: SyncAlways}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.AppendEnqueue(0, msg("a", 0)); err != nil {
		t.Fatal(err)
	}
	after := flushes(t, w, 0)
	for i := 0; i < 20; i++ {
		w.AppendAttempt(0, "a", uint32(i), uint64(i))
		w.AppendTerminal(0, "a", engine.TerminalAck)
	}
	if got := flushes(t, w, 0); got != after {
		t.Fatalf("flushes went from %d to %d; only enqueues should flush", after, got)
	}
}

// Producers arriving while a flush is running are covered by the next one, so a
// burst costs far fewer flushes than it has messages.
func TestConcurrentEnqueuesShareAFlush(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, Sync: SyncAlways}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const producers, each = 8, 50
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := w.AppendEnqueue(0, msg(fmt.Sprintf("p%d-%d", p, i), uint64(i))); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	total := uint64(producers * each)
	got := flushes(t, w, 0)
	if got > total {
		t.Fatalf("%d flushes for %d appends, want them batched", got, total)
	}
	t.Logf("%d appends cost %d flushes (%.1f per flush)", total, got, float64(total)/float64(got))
}
