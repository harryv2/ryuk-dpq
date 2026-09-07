package logic

import (
	"context"
	"log/slog"

	"github.com/harryv2/ryuk-dpq/backend/config"
	"github.com/harryv2/ryuk-dpq/backend/gateway/repo/membershipetcd"
	"github.com/harryv2/ryuk-dpq/backend/gateway/repo/postgres"
	"github.com/harryv2/ryuk-dpq/backend/gateway/repo/prometheus"
	"github.com/harryv2/ryuk-dpq/backend/third_party/logger"
)

// The providers below adapt one config struct into the pieces each repo needs,
// so the wire graph stays a list of constructors.

func ProvideLogger(cfg config.Gateway) *slog.Logger {
	return logger.New(cfg.LogLevel, cfg.LogFormat)
}

func ProvidePostgresDSN(cfg config.Gateway) postgres.DSN { return postgres.DSN(cfg.Postgres) }

func ProvideEtcdEndpoints(cfg config.Gateway) membershipetcd.Endpoints {
	return membershipetcd.Endpoints(cfg.Etcd)
}

// Prometheus is optional. An empty URL yields a repo that reports itself
// unavailable, and the metrics page falls back to sampling what it can itself.
func ProvidePrometheusURL(cfg config.Gateway) prometheus.BaseURL {
	return prometheus.BaseURL(cfg.Prometheus)
}

func ProvideConfig(cfg config.Gateway) Config {
	return Config{
		CacheTTL:     cfg.CacheTTL,
		CollectEvery: cfg.CollectEvery,
		BackstopPoll: cfg.BackstopPoll,
	}
}

func ProvideContext() context.Context { return context.Background() }
