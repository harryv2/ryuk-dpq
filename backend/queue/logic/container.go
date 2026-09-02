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

// InitialiseQueueLogic builds the node's god struct. Identity and the log
// factory come from the data directory, so a node needs no configuration beyond
// where its data lives and how to reach etcd.
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
