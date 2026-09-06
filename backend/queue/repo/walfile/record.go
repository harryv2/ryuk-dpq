package walfile

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

type recordKind uint8

const (
	kindEnqueue recordKind = iota + 1
	kindAttempt
	kindTerminal
	kindDeadLetter
	kindDeadLetterDrained
)

// header is length + crc + kind. The crc covers the payload only.
const headerLen = 4 + 4 + 1

type enqueuePayload struct {
	ID           string     `json:"id"`
	Payload      []byte     `json:"payload"`
	Priority     uint8      `json:"priority"`
	GroupID      string     `json:"group,omitempty"`
	Seq          uint64     `json:"seq"`
	EnqueuedAt   time.Time  `json:"at"`
	ExpiresAt    *time.Time `json:"exp,omitempty"`
	DeliverAfter *time.Time `json:"after,omitempty"`
}

type attemptPayload struct {
	ID       string `json:"id"`
	Attempts uint32 `json:"n"`
	Epoch    uint64 `json:"e"`
}

type terminalPayload struct {
	ID   string `json:"id"`
	Kind uint8  `json:"k"`
}

func encode(kind recordKind, v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, headerLen+len(body))
	binary.LittleEndian.PutUint32(buf[0:], uint32(len(body)))
	binary.LittleEndian.PutUint32(buf[4:], crc32.ChecksumIEEE(body))
	buf[8] = byte(kind)
	copy(buf[headerLen:], body)
	return buf, nil
}

// readRecord returns io.EOF at a clean end and errTorn at a partial or
// corrupt one, which is what a crash mid-write looks like.
func readRecord(r io.Reader) (recordKind, []byte, error) {
	var h [headerLen]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return 0, nil, errTorn
		}
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(h[0:])
	sum := binary.LittleEndian.Uint32(h[4:])
	kind := recordKind(h[8])

	if n > maxRecordBytes {
		return 0, nil, errTorn
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, errTorn
	}
	if crc32.ChecksumIEEE(body) != sum {
		return 0, nil, errTorn
	}
	return kind, body, nil
}

const maxRecordBytes = 16 << 20

func messageFromPayload(p enqueuePayload) *engine.Message {
	m := &engine.Message{
		ID:         p.ID,
		Payload:    p.Payload,
		Priority:   engine.Priority(p.Priority),
		GroupID:    p.GroupID,
		Seq:        p.Seq,
		EnqueuedAt: p.EnqueuedAt,
	}
	if p.ExpiresAt != nil {
		m.ExpiresAt = *p.ExpiresAt
	}
	if p.DeliverAfter != nil {
		m.DeliverAfter = *p.DeliverAfter
	}
	return m
}

func payloadFromMessage(m *engine.Message) enqueuePayload {
	p := enqueuePayload{
		ID:         m.ID,
		Payload:    m.Payload,
		Priority:   uint8(m.Priority),
		GroupID:    m.GroupID,
		Seq:        m.Seq,
		EnqueuedAt: m.EnqueuedAt,
	}
	if !m.ExpiresAt.IsZero() {
		e := m.ExpiresAt
		p.ExpiresAt = &e
	}
	if !m.DeliverAfter.IsZero() {
		d := m.DeliverAfter
		p.DeliverAfter = &d
	}
	return p
}

func (k recordKind) String() string {
	switch k {
	case kindEnqueue:
		return "enqueue"
	case kindAttempt:
		return "attempt"
	case kindTerminal:
		return "terminal"
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}
