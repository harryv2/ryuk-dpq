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
	return &pb.Transfer{Spec: specTo(spec), BySlot: transferTo(bySlot)}, nil
}

func transferTo(bySlot map[uint16][]entity.WireMessage) map[uint32]*pb.SlotMessages {
	out := make(map[uint32]*pb.SlotMessages, len(bySlot))
	for slot, msgs := range bySlot {
		sm := &pb.SlotMessages{Messages: make([]*pb.WireMessage, 0, len(msgs))}
		for _, m := range msgs {
			sm.Messages = append(sm.Messages, wireTo(m))
		}
		out[uint32(slot)] = sm
	}
	return out
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
	err := s.app.Absorb(entity.TransferRequest{
		Spec: specFrom(req.Spec), BySlot: bySlot, MoveID: req.MoveId,
	})
	return &pb.Empty{}, toStatus(err)
}

func (s *Server) PrepareMove(ctx context.Context, req *pb.SlotSet) (*pb.Transfer, error) {
	spec := specFrom(req.Spec)
	bySlot, err := s.app.PrepareMove(spec, slotsFrom(req.Slots))
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.Transfer{
		Spec: req.Spec, MoveId: req.MoveId, BySlot: transferTo(bySlot),
	}, nil
}

func (s *Server) DiscardMove(ctx context.Context, req *pb.SlotSet) (*pb.Empty, error) {
	if err := s.app.DiscardMove(specFrom(req.Spec), slotsFrom(req.Slots)); err != nil {
		return nil, toStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *Server) AbortMove(ctx context.Context, req *pb.SlotSet) (*pb.Empty, error) {
	if err := s.app.AbortMove(specFrom(req.Spec), slotsFrom(req.Slots)); err != nil {
		return nil, toStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *Server) Held(ctx context.Context, _ *pb.Empty) (*pb.HeldResponse, error) {
	h := s.app.HeldSlots()
	out := &pb.HeldResponse{NodeId: h.NodeID}
	for _, q := range h.Queues {
		out.Queues = append(out.Queues, &pb.HeldSlots{
			Org: q.Org, Queue: q.Name,
			Slots: slotsTo(q.Slots), Frozen: slotsTo(q.Frozen),
		})
	}
	return out, nil
}

func slotsFrom(in []uint32) []uint16 {
	out := make([]uint16, 0, len(in))
	for _, v := range in {
		out = append(out, uint16(v))
	}
	return out
}

func slotsTo(in []uint16) []uint32 {
	out := make([]uint32, 0, len(in))
	for _, v := range in {
		out = append(out, uint32(v))
	}
	return out
}
