package logic

import (
	"github.com/harryv2/ryuk-dpq/backend/config"
	"github.com/harryv2/ryuk-dpq/backend/queue/entity"
	"github.com/harryv2/ryuk-dpq/backend/queue/logic/engine"
	"github.com/harryv2/ryuk-dpq/backend/queue/repo/membershipetcd"
	"github.com/harryv2/ryuk-dpq/backend/queue/repo/walfile"
	"github.com/harryv2/ryuk-dpq/backend/third_party/logger"
	"log/slog"
)

// DataDir, NodeID and Incarnation are named types so the wire graph can tell
// them apart from any other string or integer.
type DataDir string
type NodeIdentity string
type Incarnation uint64

func ProvideLogger(cfg config.Node) *slog.Logger {
	return logger.New(cfg.LogLevel, cfg.LogFormat)
}

func ProvideDataDir(cfg config.Node) DataDir { return DataDir(cfg.DataDir) }

func ProvideEtcdEndpoints(cfg config.Node) membershipetcd.Endpoints {
	return membershipetcd.Endpoints(cfg.Etcd)
}

// ProvideNodeIdentity reads the id that lives beside the data. A container
// hostname changes on every recreate, and the id's job is to say who holds this
// data, so it has to come from the volume.
func ProvideNodeIdentity(dir DataDir) (NodeIdentity, error) {
	id, err := walfile.NodeID(string(dir))
	return NodeIdentity(id), err
}

// ProvideIncarnation bumps the restart counter. Lease generations restart from
// zero after a replay, so without this a receipt from before a crash could match
// a lease handed out after it.
func ProvideIncarnation(dir DataDir) (Incarnation, error) {
	n, err := walfile.NextIncarnation(string(dir))
	return Incarnation(n), err
}

func ProvideWALFactory(cfg config.Node, dir DataDir, inc Incarnation) entity.WALFactory {
	return walFactory{walfile.NewFactory(
		string(dir), walfile.SyncMode(cfg.WALSync), cfg.WALInterval, uint64(inc))}
}

func ProvideConfig(cfg config.Node, id NodeIdentity, inc Incarnation) Config {
	return Config{
		NodeID:      string(id),
		Incarnation: uint64(inc),
		SweepEvery:  cfg.SweepEvery,
	}
}

func ProvideClock() engine.Clock { return engine.SystemClock{} }

// walFactory adapts the concrete factory to the interface the logic layer holds.
type walFactory struct{ f *walfile.Factory }

func (w walFactory) Open(key engine.QueueKey) (entity.WALRepo, error) { return w.f.Open(key) }
func (w walFactory) Remove(key engine.QueueKey) error                 { return w.f.Remove(key) }
func (w walFactory) List() ([]engine.QueueKey, error)                 { return w.f.List() }
