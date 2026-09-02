package walfile

import (
	"os"
	"path/filepath"
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
	live, _ := w.Replay()
	keep := live[4][:3]

	if err := w.Compact(4, keep); err != nil {
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
