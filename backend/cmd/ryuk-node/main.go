package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/harryv2/ryuk-dpq/backend/config"
	pb "github.com/harryv2/ryuk-dpq/backend/proto/queue/v1"
	"github.com/harryv2/ryuk-dpq/backend/queue/controller"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic"
	"github.com/harryv2/ryuk-dpq/backend/third_party/logger"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.LoadNode()
	log := logger.New(cfg.LogLevel, cfg.LogFormat)

	app, err := logic.InitialiseQueueLogic(cfg)
	if err != nil {
		log.Error("startup", "err", err)
		os.Exit(1)
	}
	log.Info("starting", "node", app.NodeID(), "data", cfg.DataDir)

	if err := app.Recover(); err != nil {
		log.Error("recover", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	stop := make(chan struct{})
	go app.RunSweeper(stop)

	// Registering after recovery: a node should not be routable until it can
	// answer for the data it holds.
	if len(cfg.Etcd) > 0 {
		members, cleanup, err := logic.InitialiseMembership(cfg)
		if err != nil {
			log.Error("etcd", "err", err)
			os.Exit(1)
		}
		defer cleanup()

		addr := cfg.AdvertiseAs
		if addr == "" {
			addr = advertiseAddr(cfg.Listen)
		}
		if err := members.Register(ctx, app.NodeID(), addr, int64(cfg.LeaseTTL)); err != nil {
			log.Error("register", "err", err)
			os.Exit(1)
		}
	}

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterQueueServiceServer(grpcSrv, controller.New(app, log))

	go func() {
		log.Info("listening", "addr", cfg.Listen)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Error("serve", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	close(stop)
	grpcSrv.GracefulStop()
}

// advertiseAddr is how peers reach this node. Inside a container network the
// hostname resolves, so nothing needs configuring.
func advertiseAddr(listen string) string {
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		port = "9090"
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "localhost"
	}
	return net.JoinHostPort(host, port)
}
