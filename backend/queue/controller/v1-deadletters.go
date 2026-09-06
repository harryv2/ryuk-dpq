package controller

import (
	"context"

	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
)

// DeadLetters hands the gateway what this node has given up on. They stay here
// until it confirms they were moved, so nothing is lost if it dies in between.
func (s *Server) DeadLetters(_ context.Context, _ *pb.Empty) (*pb.DeadLetterResponse, error) {
	held := s.app.DeadLetters()
	out := &pb.DeadLetterResponse{DeadLetters: make([]*pb.DeadLetter, 0, len(held.DeadLetters))}
	for _, d := range held.DeadLetters {
		out.DeadLetters = append(out.DeadLetters, &pb.DeadLetter{
			Org:     d.Org,
			Name:    d.Name,
			Slot:    uint32(d.Slot),
			Message: wireTo(entity.ToWire(d.Msg)),
		})
	}
	return out, nil
}

func (s *Server) AckDeadLetters(_ context.Context, req *pb.AckDeadLettersRequest) (*pb.Empty, error) {
	if err := s.app.AckDeadLetters(req.GetMessageIds()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.Empty{}, nil
}
