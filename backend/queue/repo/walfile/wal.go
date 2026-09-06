// Package walfile is the on-disk write-ahead log.
package walfile

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

var errTorn = errors.New("walfile: torn record")

type SyncMode string

const (
	SyncAlways   SyncMode = "always"
	SyncInterval SyncMode = "interval"
	SyncNever    SyncMode = "never"
)

type Options struct {
	Dir      string
	Sync     SyncMode
	Interval time.Duration
}

// WAL implements engine.Journal for one queue.
type WAL struct {
	opts Options

	mu    sync.Mutex
	slots map[uint16]*slotFile

	dlOnce sync.Once
	dl     *deadLetterLog
	dlErr  error

	incarnation uint64
	stop        chan struct{}
	stopped     sync.WaitGroup
	closeOnce   sync.Once
}

type slotFile struct {
	mu sync.Mutex
	f  *os.File
}

func Open(opts Options, incarnation uint64) (*WAL, error) {
	if opts.Sync == "" {
		opts.Sync = SyncInterval
	}
	if opts.Interval <= 0 {
		opts.Interval = 100 * time.Millisecond
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{
		opts:        opts,
		slots:       make(map[uint16]*slotFile),
		incarnation: incarnation,
		stop:        make(chan struct{}),
	}
	if opts.Sync == SyncInterval {
		w.stopped.Add(1)
		go w.syncLoop()
	}
	return w, nil
}

func (w *WAL) Close() error {
	var firstErr error
	w.closeOnce.Do(func() {
		close(w.stop)
		w.stopped.Wait()

		if w.dl != nil {
			w.dl.mu.Lock()
			_ = w.dl.f.Sync()
			if err := w.dl.f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			w.dl.mu.Unlock()
		}

		w.mu.Lock()
		defer w.mu.Unlock()
		for _, sf := range w.slots {
			sf.mu.Lock()
			_ = sf.f.Sync()
			if err := sf.f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			sf.mu.Unlock()
		}
		w.slots = map[uint16]*slotFile{}
	})
	return firstErr
}

func (w *WAL) Incarnation() uint64 { return w.incarnation }

func (w *WAL) slot(id uint16) (*slotFile, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if sf, ok := w.slots[id]; ok {
		return sf, nil
	}
	f, err := os.OpenFile(w.path(id), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	sf := &slotFile{f: f}
	w.slots[id] = sf
	return sf, nil
}

func (w *WAL) path(id uint16) string {
	return filepath.Join(w.opts.Dir, fmt.Sprintf("slot-%04d.log", id))
}

func (w *WAL) append(id uint16, kind recordKind, v any, durable bool) error {
	buf, err := encode(kind, v)
	if err != nil {
		return err
	}
	sf, err := w.slot(id)
	if err != nil {
		return err
	}
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// A single Write reaches the page cache, so the record survives the process
	// dying. Only power loss needs the fsync below.
	if _, err := sf.f.Write(buf); err != nil {
		return err
	}
	if durable && w.opts.Sync == SyncAlways {
		return sf.f.Sync()
	}
	return nil
}

func (w *WAL) AppendEnqueue(slot uint16, m *engine.Message) error {
	return w.append(slot, kindEnqueue, payloadFromMessage(m), true)
}

func (w *WAL) AppendAttempt(slot uint16, id string, attempts uint32, epoch uint64) {
	_ = w.append(slot, kindAttempt, attemptPayload{ID: id, Attempts: attempts, Epoch: epoch}, false)
}

func (w *WAL) AppendTerminal(slot uint16, id string, kind engine.TerminalKind) {
	_ = w.append(slot, kindTerminal, terminalPayload{ID: id, Kind: uint8(kind)}, false)
}

func (w *WAL) syncLoop() {
	defer w.stopped.Done()
	t := time.NewTicker(w.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-t.C:
			w.syncAll()
		}
	}
}

func (w *WAL) syncAll() {
	w.mu.Lock()
	files := make([]*slotFile, 0, len(w.slots))
	for _, sf := range w.slots {
		files = append(files, sf)
	}
	w.mu.Unlock()

	for _, sf := range files {
		sf.mu.Lock()
		_ = sf.f.Sync()
		sf.mu.Unlock()
	}
}

// Replay rebuilds what each slot held.
func (w *WAL) Replay() (map[uint16][]*engine.Message, error) {
	entries, err := os.ReadDir(w.opts.Dir)
	if err != nil {
		return nil, err
	}
	out := make(map[uint16][]*engine.Message)

	for _, e := range entries {
		id, ok := slotFromName(e.Name())
		if !ok {
			continue
		}
		msgs, err := w.replaySlot(id)
		if err != nil {
			return nil, fmt.Errorf("slot %d: %w", id, err)
		}
		if len(msgs) > 0 {
			out[id] = msgs
		}
	}
	return out, nil
}

func (w *WAL) replaySlot(id uint16) ([]*engine.Message, error) {
	f, err := os.Open(w.path(id))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	live := make(map[string]*engine.Message)
	order := make([]string, 0, 64)
	good := int64(0)

	for {
		kind, body, err := readRecord(f)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, errTorn) {
			// truncate the torn tail so the next append starts clean
			_ = os.Truncate(w.path(id), good)
			break
		}
		if err != nil {
			return nil, err
		}
		good += int64(headerLen + len(body))

		switch kind {
		case kindEnqueue:
			var p enqueuePayload
			if err := json.Unmarshal(body, &p); err != nil {
				return nil, err
			}
			if _, seen := live[p.ID]; !seen {
				order = append(order, p.ID)
			}
			live[p.ID] = messageFromPayload(p)
		case kindAttempt:
			var p attemptPayload
			if err := json.Unmarshal(body, &p); err != nil {
				return nil, err
			}
			if m, ok := live[p.ID]; ok {
				m.Attempts = p.Attempts
			}
		case kindTerminal:
			var p terminalPayload
			if err := json.Unmarshal(body, &p); err != nil {
				return nil, err
			}
			delete(live, p.ID)
		}
	}

	msgs := make([]*engine.Message, 0, len(live))
	for _, id := range order {
		if m, ok := live[id]; ok {
			msgs = append(msgs, m)
		}
	}
	return msgs, nil
}

// Compact rewrites a slot's log to hold only what is still live, which is what
// keeps the file from growing forever.
func (w *WAL) Compact(id uint16, msgs []*engine.Message) error {
	tmp := w.path(id) + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		buf, err := encode(kindEnqueue, payloadFromMessage(m))
		if err != nil {
			f.Close()
			return err
		}
		if _, err := f.Write(buf); err != nil {
			f.Close()
			return err
		}
		if m.Attempts > 0 {
			b, _ := encode(kindAttempt, attemptPayload{ID: m.ID, Attempts: m.Attempts})
			if _, err := f.Write(b); err != nil {
				f.Close()
				return err
			}
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()

	w.mu.Lock()
	if sf, ok := w.slots[id]; ok {
		sf.mu.Lock()
		sf.f.Close()
		delete(w.slots, id)
		sf.mu.Unlock()
	}
	w.mu.Unlock()

	return os.Rename(tmp, w.path(id))
}

// Drop removes a slot's log entirely, used when a slot is handed to another node.
func (w *WAL) Drop(id uint16) error {
	w.mu.Lock()
	if sf, ok := w.slots[id]; ok {
		sf.mu.Lock()
		sf.f.Close()
		delete(w.slots, id)
		sf.mu.Unlock()
	}
	w.mu.Unlock()
	err := os.Remove(w.path(id))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func slotFromName(name string) (uint16, bool) {
	if !strings.HasPrefix(name, "slot-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "slot-"), ".log"))
	if err != nil || n < 0 || n > 65535 {
		return 0, false
	}
	return uint16(n), true
}

// Factory owns the on-disk layout: one directory per queue, one file per slot.
type Factory struct {
	root string
	opts Options
	inc  uint64
}

func NewFactory(root string, sync SyncMode, interval time.Duration, incarnation uint64) *Factory {
	return &Factory{
		root: root,
		opts: Options{Sync: sync, Interval: interval},
		inc:  incarnation,
	}
}

func (f *Factory) dir(key engine.QueueKey) string {
	return filepath.Join(f.root, "queues", sanitise(key.Org), sanitise(key.Name))
}

func (f *Factory) Open(key engine.QueueKey) (*WAL, error) {
	o := f.opts
	o.Dir = f.dir(key)
	return Open(o, f.inc)
}

func (f *Factory) Remove(key engine.QueueKey) error {
	return os.RemoveAll(f.dir(key))
}

// List finds every queue this node already has data for, which is what a
// restarting node uses to work out what it holds.
func (f *Factory) List() ([]engine.QueueKey, error) {
	root := filepath.Join(f.root, "queues")
	orgs, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var keys []engine.QueueKey
	for _, org := range orgs {
		if !org.IsDir() {
			continue
		}
		names, err := os.ReadDir(filepath.Join(root, org.Name()))
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			if n.IsDir() {
				keys = append(keys, engine.QueueKey{Org: org.Name(), Name: n.Name()})
			}
		}
	}
	return keys, nil
}

// sanitise keeps a name usable as a directory without collapsing two different
// names onto one path.
func sanitise(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteString(fmt.Sprintf("~%02x", r))
		}
	}
	return b.String()
}
