package controller

import (
	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
)

// Subscribe pushes a notification when a queue on this node gains work.
func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.QueueService_SubscribeServer) error {
	id, ch := s.app.Subscribe()
	defer s.app.Unsubscribe(id)

	s.log.Debug("gateway subscribed", "gateway", req.GatewayId)
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case n, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.WorkAvailable{Org: n.Org, Name: n.Name}); err != nil {
				return err
			}
		}
	}
}
