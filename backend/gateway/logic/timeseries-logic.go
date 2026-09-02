package logic

import (
	"context"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
)

// windows the metrics page can ask for, and how finely to sample each. A longer
// window gets a coarser step so the number of points stays roughly constant
// whatever the range.
var windows = map[string]struct {
	Range time.Duration
	Step  time.Duration
}{
	"5m":  {5 * time.Minute, 5 * time.Second},
	"1h":  {time.Hour, 30 * time.Second},
	"6h":  {6 * time.Hour, 3 * time.Minute},
	"24h": {24 * time.Hour, 10 * time.Minute},
}

// Timeseries reads a queue's history back out of the monitoring system. The org
// comes from the credential, so a caller can only ever see its own queues.
func (l *GatewayLogic) Timeseries(
	ctx context.Context, org, name, window string,
) (entity.TimeseriesResponse, error) {
	// The queue has to exist and belong to this org before anything is read.
	if _, err := l.config(ctx, org, name); err != nil {
		return entity.TimeseriesResponse{}, err
	}

	w, ok := windows[window]
	if !ok {
		w = windows["5m"]
		window = "5m"
	}

	res, err := l.series.Query(ctx, entity.TimeseriesQuery{
		Org: org, Queue: name, Range: w.Range, Step: w.Step,
	})
	if err != nil {
		return entity.TimeseriesResponse{}, err
	}
	res.Range = window
	return res, nil
}
