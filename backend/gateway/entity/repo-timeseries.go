package entity

import (
	"context"
	"time"
)

//go:generate mockgen -source=repo-timeseries.go -destination=../mocks/mock_timeseries_repo.go -package=mocks

type Series struct {
	Name   string        `json:"name"`
	Points []SeriesPoint `json:"points"`
}

type SeriesPoint struct {
	At    time.Time `json:"at"`
	Value float64   `json:"value"`
}

// TimeseriesQuery asks for one metric over a window.
type TimeseriesQuery struct {
	Org   string
	Queue string
	Range time.Duration
	Step  time.Duration
}

type TimeseriesResponse struct {
	// Available is false when no monitoring system is configured, which is the
	// signal for the caller to fall back to what it can sample itself.
	Available  bool                `json:"available"`
	Range      string              `json:"range"`
	Step       string              `json:"step"`
	ByPriority map[string][]Series `json:"-"`
	Ready      []Series            `json:"ready"`
	InFlight   []Series            `json:"inFlight"`
	OldestAge  []Series            `json:"oldestAge"`
	Rates      []Series            `json:"rates"`
}

// TimeseriesRepo reads history back out of the monitoring system. The gateway
// writes to it only by being scraped; nothing here pushes.
type TimeseriesRepo interface {
	Query(ctx context.Context, q TimeseriesQuery) (TimeseriesResponse, error)
	Ready(ctx context.Context) bool
}
