package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/config"
	"github.com/harryv2/ryuk-dpq/backend/gateway/controller"
	"github.com/harryv2/ryuk-dpq/backend/gateway/logic"
	"github.com/harryv2/ryuk-dpq/backend/third_party/logger"
)

func main() {
	cfg := config.LoadGateway()

	// Docker's healthcheck runs inside the container, which has no curl. The
	// binary is already there, so it answers the question itself.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		os.Exit(healthcheck(cfg.Listen))
	}

	log := logger.New(cfg.LogLevel, cfg.LogFormat)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	app, cleanup, err := logic.InitialiseGatewayLogic(ctx, cfg)
	if err != nil {
		log.Error("startup", "err", err)
		os.Exit(1)
	}
	defer cleanup()

	if err := app.WatchMembers(ctx); err != nil {
		log.Error("watch members", "err", err)
		os.Exit(1)
	}

	go app.RunCollector(ctx)
	go app.RunRebalancer(ctx)
	go app.RunSubscriber(ctx, gatewayID())

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           controller.New(app, log, loadOrgs(), cfg.StaticDir).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("listening", "addr", cfg.Listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("serve", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	_ = srv.Shutdown(shutdownCtx)
}

func healthcheck(listen string) int {
	port := listen
	if _, p, err := net.SplitHostPort(listen); err == nil {
		port = p
	}
	res, err := http.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return 1
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func gatewayID() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "gateway"
	}
	return h
}

// loadOrgs maps a bearer token to an org. Hardcoded for now; real
// authentication is a swap of this one function.
func loadOrgs() map[string]string {
	out := map[string]string{
		"acme-token":    "org_acme",
		"globex-token":  "org_globex",
		"initech-token": "org_initech",
	}
	if raw := os.Getenv("RYUK_ORG_TOKENS"); raw != "" {
		out = map[string]string{}
		for _, pair := range strings.Split(raw, ",") {
			if k, v, ok := strings.Cut(pair, ":"); ok {
				out[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
	}
	return out
}
