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
}
