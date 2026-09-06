package walfile

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

// Dead letters get their own log rather than a record in the slot's. A slot log
// holds what the queue still has; these are what it no longer has and has not
// yet handed over, which is a different lifetime. Keeping them apart also means
// slot replay and compaction are untouched by any of this.
const deadLetterFile = "dead-letters.log"

type deadLetterPayload struct {
	Slot uint16         `json:"slot"`
	Msg  enqueuePayload `json:"msg"`
}

type deadLetterLog struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

func (w *WAL) deadLetters() (*deadLetterLog, error) {
	w.dlOnce.Do(func() {
		p := filepath.Join(w.opts.Dir, deadLetterFile)
		f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			w.dlErr = err
			return
		}
		w.dl = &deadLetterLog{f: f, path: p}
	})
	return w.dl, w.dlErr
}

// AppendDeadLetter records a message the queue has given up on. Written before
// the gateway is told about it, so a restart in between does not lose it.
func (w *WAL) AppendDeadLetter(slot uint16, m *engine.Message) error {
	dl, err := w.deadLetters()
	if err != nil {
		return err
	}
	buf, err := encode(kindDeadLetter, deadLetterPayload{Slot: slot, Msg: payloadFromMessage(m)})
	if err != nil {
		return err
	}
	dl.mu.Lock()
	defer dl.mu.Unlock()
	if _, err := dl.f.Write(buf); err != nil {
		return err
	}
	return dl.f.Sync()
}

// AppendDeadLetterDrained records that the gateway has it now, so replay stops
// bringing it back.
func (w *WAL) AppendDeadLetterDrained(id string) error {
	dl, err := w.deadLetters()
	if err != nil {
		return err
	}
	buf, err := encode(kindDeadLetterDrained, terminalPayload{ID: id})
	if err != nil {
		return err
	}
	dl.mu.Lock()
	defer dl.mu.Unlock()
	_, err = dl.f.Write(buf)
	return err
}

// ReplayDeadLetters rebuilds what has been given up on but not yet handed over.
func (w *WAL) ReplayDeadLetters() ([]entity.PendingDeadLetter, error) {
	p := filepath.Join(w.opts.Dir, deadLetterFile)
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	live := map[string]entity.PendingDeadLetter{}
	order := []string{}
	good := int64(0)

	for {
		kind, body, err := readRecord(f)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, errTorn) {
			_ = os.Truncate(p, good)
			break
		}
		if err != nil {
			return nil, err
		}
		good += int64(headerLen + len(body))

		switch kind {
		case kindDeadLetter:
			var d deadLetterPayload
			if err := json.Unmarshal(body, &d); err != nil {
				return nil, err
			}
			if _, seen := live[d.Msg.ID]; !seen {
				order = append(order, d.Msg.ID)
			}
			live[d.Msg.ID] = entity.PendingDeadLetter{Slot: d.Slot, Msg: messageFromPayload(d.Msg)}
		case kindDeadLetterDrained:
			var t terminalPayload
			if err := json.Unmarshal(body, &t); err != nil {
				return nil, err
			}
			delete(live, t.ID)
		}
	}

	out := make([]entity.PendingDeadLetter, 0, len(live))
	for _, id := range order {
		if d, ok := live[id]; ok {
			out = append(out, d)
		}
	}
	return out, nil
}

// CompactDeadLetters rewrites the log to hold only what is still waiting. Every
// drained record is one that will never matter again, and without this the file
// grows for the life of the queue.
func (w *WAL) CompactDeadLetters(pending []entity.PendingDeadLetter) error {
	dl, err := w.deadLetters()
	if err != nil {
		return err
	}
	dl.mu.Lock()
	defer dl.mu.Unlock()

	tmp := dl.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, d := range pending {
		buf, err := encode(kindDeadLetter, deadLetterPayload{Slot: d.Slot, Msg: payloadFromMessage(d.Msg)})
		if err != nil {
			f.Close()
			return err
		}
		if _, err := f.Write(buf); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dl.path); err != nil {
		return err
	}

	_ = dl.f.Close()
	reopened, err := os.OpenFile(dl.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	dl.f = reopened
	return nil
}
