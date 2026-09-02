package entity

import "context"

//go:generate mockgen -source=repo-membership.go -destination=../mocks/mock_membership_repo.go -package=mocks

// MembershipRepo is how a node publishes itself. It only writes: reading the
// cluster is the gateway's job.
type MembershipRepo interface {
	Register(ctx context.Context, id, addr string, ttlSeconds int64) error
	Close() error
}
