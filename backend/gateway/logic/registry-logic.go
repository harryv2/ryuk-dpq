package logic

import (
	"context"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

// GetRegistry reads etcd as it actually is, rather than the member list the
// gateway builds from it. The two disagreeing is worth being able to see: a
// gateway that missed a watch event keeps serving a stale view, and nothing
// else in the UI would show it.
func (l *GatewayLogic) GetRegistry(ctx context.Context) (entity.RegistryResponse, error) {
	entries, err := l.membershipRepo.Entries(ctx)
	if err != nil {
		return entity.RegistryResponse{}, enterr.Internal("read registry", err)
	}
	seen := map[string]bool{}
	for _, m := range l.membershipRepo.Members() {
		seen[m.ID] = true
	}
	return entity.RegistryResponse{
		Entries:  entries,
		Watching: len(seen),
	}, nil
}
