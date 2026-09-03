//go:build wireinject
// +build wireinject

//go:generate wire

package logic

import (
	"context"

	"github.com/google/wire"
	"github.com/harryv2/ryuk-dpq/backend/config"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/repo/membershipetcd"
	"github.com/harryv2/ryuk-dpq/backend/gateway/repo/nodegrpc"
	"github.com/harryv2/ryuk-dpq/backend/gateway/repo/postgres"
	"github.com/harryv2/ryuk-dpq/backend/gateway/repo/prometheus"
)

// InitialiseGatewayLogic builds the god struct and everything under it. Swapping
// an implementation is one wire.Bind line.
func InitialiseGatewayLogic(ctx context.Context, cfg config.Gateway) (*GatewayLogic, func(), error) {
	wire.Build(
		ProvideLogger,
		ProvideConfig,
		ProvidePostgresDSN,
		ProvideEtcdEndpoints,
		ProvidePrometheusURL,

		postgres.NewDBWrapper,
		postgres.NewQueuesTable,
		wire.Bind(new(entity.QueueTableRepo), new(*postgres.QueuesTable)),
		postgres.NewSlotPlacementTable,
		wire.Bind(new(entity.SlotPlacementTableRepo), new(*postgres.SlotPlacementTable)),

		nodegrpc.New,
		wire.Bind(new(entity.NodeGRPCRepo), new(*nodegrpc.Repo)),

		membershipetcd.New,
		wire.Bind(new(entity.MembershipRepo), new(*membershipetcd.Repo)),

		prometheus.New,
		wire.Bind(new(entity.TimeseriesRepo), new(*prometheus.Repo)),

		New,
	)
	return nil, nil, nil
}
