package controller

import (
	"context"

	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
)

func (s *Server) Drop(ctx context.Context, req *pb.StatsRequest) (*pb.Empty, error) {
	spec := specFrom(req.Spec)
	err := s.app.Drop(engine.QueueKey{Org: spec.Org, Name: spec.Name})
	return &pb.Empty{}, toStatus(err)
}

func (s *Server) Freeze(ctx context.Context, req *pb.FreezeRequest) (*pb.Transfer, error) {
	spec := specFrom(req.Spec)
	only := make([]uint16, 0, len(req.Slots))
	for _, sl := range req.Slots {
		only = append(only, uint16(sl))
	}
	bySlot, err := s.app.Freeze(spec, only)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &pb.Transfer{Spec: specTo(spec), BySlot: map[uint32]*pb.SlotMessages{}}
	for slot, msgs := range bySlot {
		sm := &pb.SlotMessages{Messages: make([]*pb.WireMessage, 0, len(msgs))}
		for _, m := range msgs {
			sm.Messages = append(sm.Messages, wireTo(m))
		}
		out.BySlot[uint32(slot)] = sm
	}
	return out, nil
}

func (s *Server) Absorb(ctx context.Context, req *pb.Transfer) (*pb.Empty, error) {
	bySlot := make(map[uint16][]entity.WireMessage, len(req.BySlot))
	for slot, sm := range req.BySlot {
		msgs := make([]entity.WireMessage, 0, len(sm.Messages))
		for _, m := range sm.Messages {
			msgs = append(msgs, wireFrom(m))
		}
		bySlot[uint16(slot)] = msgs
	}
	err := s.app.Absorb(entity.TransferRequest{Spec: specFrom(req.Spec), BySlot: bySlot})
	return &pb.Empty{}, toStatus(err)
}

// Subscribe pushes a notification when a queue on this node gains work. The
// gateway then issues one dequeue, so nothing is leased speculatively. When the
// gateway goes away the stream breaks and the subscription is dropped.
