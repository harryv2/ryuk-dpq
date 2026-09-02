package controller

import (
	"context"
	"log/slog"

	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity/enterr"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedQueueServiceServer
	app logic.QueueLogicInterface
	log *slog.Logger
}

func New(app logic.QueueLogicInterface, log *slog.Logger) *Server {
	return &Server{app: app, log: log}
}

// toStatus maps the logic layer's error codes onto gRPC codes.
// toStatus maps the logic layer's error codes onto gRPC codes.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	c := codes.Internal
	switch enterr.CodeOf(err) {
	case enterr.CodeInvalid:
		c = codes.InvalidArgument
	case enterr.CodeNotFound:
		c = codes.NotFound
	case enterr.CodeConflict:
		c = codes.FailedPrecondition
	case enterr.CodeExhausted:
		c = codes.ResourceExhausted
	case enterr.CodeMoved:
		c = codes.Aborted
	}
	return status.Error(c, err.Error())
}

func (s *Server) Health(ctx context.Context, _ *pb.Empty) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: "ok", NodeId: s.app.NodeID()}, nil
}
