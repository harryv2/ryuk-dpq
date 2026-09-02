package logic

import (
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity/enterr"
)

func (l *QueueLogic) Dequeue(req entity.DequeueRequest) (entity.DequeueResponse, error) {
	lq, err := l.queueFor(req.Spec)
	if err != nil {
		return entity.DequeueResponse{}, enterr.Internal("open queue", err)
	}

	n := req.MaxMessages
	if n <= 0 {
		n = 1
	}
	if n > 10 {
		n = 10
	}

	out := entity.DequeueResponse{}
	for i := 0; i < n; i++ {
		m, r, ok := lq.q.Dequeue()
		if !ok {
			break
		}
		out.Messages = append(out.Messages, entity.DeliveredMessage{
			MessageID:  m.ID,
			Payload:    m.Payload,
			Priority:   uint8(m.Priority),
			GroupID:    m.GroupID,
			Attempts:   m.Attempts,
			EnqueuedAt: m.EnqueuedAt,
			Receipt:    entity.EncodeReceipt(r),
		})
	}
	return out, nil
}
