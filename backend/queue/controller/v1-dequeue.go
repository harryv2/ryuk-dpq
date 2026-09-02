package controller

import (
	"context"

	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
)

func (s *Server) Dequeue(ctx context.Context, req *pb.DequeueRequest) (*pb.DequeueResponse, error) {
	out, err := s.app.Dequeue(entity.DequeueRequest{
		Spec:        specFrom(req.Spec),
		MaxMessages: int(req.MaxMessages),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &pb.DequeueResponse{Messages: make([]*pb.DeliveredMessage, 0, len(out.Messages))}
	for _, m := range out.Messages {
		resp.Messages = append(resp.Messages, &pb.DeliveredMessage{
			MessageId:        m.MessageID,
			Payload:          m.Payload,
			Priority:         uint32(m.Priority),
			GroupId:          m.GroupID,
			Attempts:         m.Attempts,
			EnqueuedAtUnixNs: m.EnqueuedAt.UnixNano(),
			Receipt:          m.Receipt,
		})
	}
	return resp, nil
}
