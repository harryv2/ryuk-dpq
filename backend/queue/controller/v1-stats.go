package controller

import (
	"context"

	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
)

func (s *Server) Stats(ctx context.Context, req *pb.StatsRequest) (*pb.QueueStats, error) {
	out, err := s.app.Stats(specFrom(req.Spec))
	if err != nil {
		return nil, toStatus(err)
	}
	return statsTo(out), nil
}

// StatsAll answers for every queue on this node in one call, so collecting
// metrics costs one request per node rather than one per queue.
// StatsAll answers for every queue on this node in one call, so collecting
// metrics costs one request per node rather than one per queue.
func (s *Server) StatsAll(ctx context.Context, _ *pb.Empty) (*pb.StatsAllResponse, error) {
	out := s.app.StatsAll()
	resp := &pb.StatsAllResponse{NodeId: out.NodeID}
	for _, q := range out.Queues {
		resp.Queues = append(resp.Queues, statsTo(q))
	}
	return resp, nil
}
