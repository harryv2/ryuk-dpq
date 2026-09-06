//go:build wireinject
// +build wireinject

//go:generate wire

package logic

import (
	"github.com/google/wire"
	"github.com/harryv2/ryuk-dpq/backend/config"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/repo/membershipetcd"
)

// InitialiseQueueLogic builds the node's god struct.
func InitialiseQueueLogic(cfg config.Node) (*QueueLogic, error) {
	wire.Build(
		ProvideLogger,
		ProvideDataDir,
		ProvideNodeIdentity,
		ProvideIncarnation,
		ProvideWALFactory,
		ProvideConfig,
		ProvideClock,
		New,
	)
	return nil, nil
}

// InitialiseMembership is separate because a node can run without etcd, which is
// what makes a single-process test possible.
func InitialiseMembership(cfg config.Node) (*membershipetcd.Repo, func(), error) {
	wire.Build(
		ProvideLogger,
		ProvideEtcdEndpoints,
		membershipetcd.New,
	)
	return nil, nil, nil
}

var _ entity.WALFactory = walFactory{}
