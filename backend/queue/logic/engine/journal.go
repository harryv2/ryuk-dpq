package engine

type TerminalKind uint8

const (
	TerminalAck TerminalKind = iota
	TerminalExpired
	TerminalDeadLettered
)

// Journal is the write-ahead log.
type Journal interface {
	Incarnation() uint64
	AppendEnqueue(slot uint16, m *Message) error
	AppendAttempt(slot uint16, id string, attempts uint32, epoch uint64)
	AppendTerminal(slot uint16, id string, kind TerminalKind)
}

type NoopJournal struct{}

func (NoopJournal) Incarnation() uint64                          { return 0 }
func (NoopJournal) AppendEnqueue(uint16, *Message) error         { return nil }
func (NoopJournal) AppendAttempt(uint16, string, uint32, uint64) {}
func (NoopJournal) AppendTerminal(uint16, string, TerminalKind)  {}
