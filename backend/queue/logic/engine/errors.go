package engine

import "errors"

var (
	ErrNotInFlight  = errors.New("engine: message not in flight")
	ErrLeaseExpired = errors.New("engine: lease expired, message was redelivered")
	ErrBadPriority  = errors.New("engine: priority out of range")
	ErrQueueFull    = errors.New("engine: queue at max depth")
	ErrFrozen       = errors.New("engine: queue is frozen")
	ErrBadSlot      = errors.New("engine: slot out of range")
)
