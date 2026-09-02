package controller

import (
	"context"
	"time"

	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
)

func (s *Server) Enqueue(ctx context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	in := entity.EnqueueRequest{
		Spec:         specFrom(req.Spec),
		Payload:      req.Payload,
		Priority:     uint8(req.Priority),
		GroupID:      req.GroupId,
		TTL:          time.Duration(req.TtlNs),
		DeliverAfter: time.Duration(req.DeliverAfterNs),
	}
	if req.Slot != nil {
		slot := uint16(*req.Slot)
		in.Slot = &slot
	}
	out, err := s.app.Enqueue(in)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.EnqueueResponse{MessageId: out.MessageID, Slot: uint32(out.Slot), Seq: out.Seq}, nil
}
