package controller

import (
	"time"

	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

func specFrom(p *pb.QueueSpec) entity.QueueSpec {
	if p == nil {
		return entity.QueueSpec{}
	}
	return entity.QueueSpec{
		Org:                 p.Org,
		Name:                p.Name,
		VisibilityTimeout:   time.Duration(p.VisibilityTimeoutNs),
		MaxRetries:          p.MaxRetries,
		DefaultTTL:          time.Duration(p.DefaultTtlNs),
		StarvationThreshold: time.Duration(p.StarvationThresholdNs),
		StarvationReserve:   p.StarvationReserve,
		MaxDepth:            p.MaxDepth,
		Distributed:         p.Distributed,
		HasDeadLetter:       p.HasDeadLetter,
		Generation:          p.Generation,
	}
}

func specTo(s entity.QueueSpec) *pb.QueueSpec {
	return &pb.QueueSpec{
		Org:                   s.Org,
		Name:                  s.Name,
		VisibilityTimeoutNs:   int64(s.VisibilityTimeout),
		MaxRetries:            s.MaxRetries,
		DefaultTtlNs:          int64(s.DefaultTTL),
		StarvationThresholdNs: int64(s.StarvationThreshold),
		StarvationReserve:     s.StarvationReserve,
		MaxDepth:              s.MaxDepth,
		Distributed:           s.Distributed,
		HasDeadLetter:         s.HasDeadLetter,
		Generation:            s.Generation,
	}
}

func statsTo(s entity.QueueStats) *pb.QueueStats {
	return &pb.QueueStats{
		Org:          s.Org,
		Name:         s.Name,
		Ready:        s.Ready[:],
		InFlight:     s.InFlight,
		Delayed:      s.Delayed,
		OldestAgeNs:  int64(s.OldestAge),
		Enqueued:     s.Enqueued,
		Acked:        s.Acked,
		Expired:      s.Expired,
		Requeued:     s.Requeued,
		DeadLettered: s.DeadLettered,
		Escapes:      s.Escapes,
	}
}

func wireTo(m entity.WireMessage) *pb.WireMessage {
	w := &pb.WireMessage{
		Id:               m.ID,
		Payload:          m.Payload,
		Priority:         uint32(m.Priority),
		GroupId:          m.GroupID,
		Seq:              m.Seq,
		EnqueuedAtUnixNs: m.EnqueuedAt.UnixNano(),
		Attempts:         m.Attempts,
	}
	if m.ExpiresAt != nil {
		w.ExpiresAtUnixNs = m.ExpiresAt.UnixNano()
	}
	if m.DeliverAfter != nil {
		w.DeliverAfterUnixNs = m.DeliverAfter.UnixNano()
	}
	return w
}

func wireFrom(w *pb.WireMessage) entity.WireMessage {
	m := entity.WireMessage{
		ID:         w.Id,
		Payload:    w.Payload,
		Priority:   uint8(w.Priority),
		GroupID:    w.GroupId,
		Seq:        w.Seq,
		EnqueuedAt: time.Unix(0, w.EnqueuedAtUnixNs).UTC(),
		Attempts:   w.Attempts,
	}
	if w.ExpiresAtUnixNs != 0 {
		t := time.Unix(0, w.ExpiresAtUnixNs).UTC()
		m.ExpiresAt = &t
	}
	if w.DeliverAfterUnixNs != 0 {
		t := time.Unix(0, w.DeliverAfterUnixNs).UTC()
		m.DeliverAfter = &t
	}
	return m
}

var _ = engine.QueueKey{}
