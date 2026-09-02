package controller

import (
	"context"
	"time"

	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
)

func (s *Server) Ack(ctx context.Context, req *pb.AckRequest) (*pb.Empty, error) {
	err := s.app.Ack(entity.AckRequest{Spec: specFrom(req.Spec), Receipt: req.Receipt})
	return &pb.Empty{}, toStatus(err)
}

func (s *Server) Nack(ctx context.Context, req *pb.NackRequest) (*pb.Empty, error) {
	err := s.app.Nack(entity.NackRequest{
		Spec: specFrom(req.Spec), Receipt: req.Receipt, Delay: time.Duration(req.DelayNs),
	})
	return &pb.Empty{}, toStatus(err)
}
