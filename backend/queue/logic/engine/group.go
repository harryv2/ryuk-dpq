package engine

type group struct {
	id      string
	msgs    deque[*Message]
	locked  bool
	inBand  bool
	band    Priority
	version uint64
}
