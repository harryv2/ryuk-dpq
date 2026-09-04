package entity

import "context"

//go:generate mockgen -source=repo-membership.go -destination=../mocks/mock_membership_repo.go -package=mocks

// MembershipRepo is the live view of the cluster. The gateway only reads it;
// nodes are the ones that publish themselves.
type MembershipRepo interface {
	Watch(ctx context.Context) error
	Members() []Member
	Lookup(id string) (Member, bool)
	Changed() <-chan struct{}
	// Entries is the raw contents of the registry, for looking at what
	// membership actually looks like rather than the view built from it.
	Entries(ctx context.Context) ([]RegistryEntry, error)
}

// RegistryEntry is one key as it is stored. TTL is what remains of the lease
// holding it, which is how long the node has before it disappears.
type RegistryEntry struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	Lease      string `json:"lease,omitempty"`
	TTLSeconds int64  `json:"ttlSeconds,omitempty"`
	Version    int64  `json:"version"`
}
