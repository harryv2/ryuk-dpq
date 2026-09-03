package logic

import (
	"context"
	"testing"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
	"go.uber.org/mock/gomock"
)

// The org has to come from the credential. If the queue name alone decided what
// was read, one tenant could name another's queue and get its history back.
func TestTimeseriesScopesToTheCallersOrg(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "q").Return(cfgFor("org1", "q", "node-1"), nil)
	d.series.EXPECT().
		Query(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q entity.TimeseriesQuery) (entity.TimeseriesResponse, error) {
			if q.Org != "org1" {
				t.Errorf("queried org %q, want org1", q.Org)
			}
			if q.Queue != "q" {
				t.Errorf("queried queue %q, want q", q.Queue)
			}
			return entity.TimeseriesResponse{Available: true}, nil
		})

	if _, err := l.Timeseries(context.Background(), "org1", "q", "5m"); err != nil {
		t.Fatal(err)
	}
}

// A queue this org does not own must not reach the monitoring system at all.
func TestTimeseriesRefusesAQueueTheOrgDoesNotOwn(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "elsewhere").
		Return(entity.QueueConfig{}, enterr.NotFound("queue"))

	_, err := l.Timeseries(context.Background(), "org1", "elsewhere", "5m")
	if enterr.CodeOf(err) != enterr.CodeNotFound {
		t.Fatalf("want a not-found error, got %v", err)
	}
}

func TestTimeseriesWindows(t *testing.T) {
	cases := []struct {
		asked string
		want  time.Duration
		label string
	}{
		{"5m", 5 * time.Minute, "5m"},
		{"1h", time.Hour, "1h"},
		{"24h", 24 * time.Hour, "24h"},
		{"nonsense", 5 * time.Minute, "5m"}, // an unknown window falls back
		{"", 5 * time.Minute, "5m"},
	}
	for _, c := range cases {
		t.Run(c.asked, func(t *testing.T) {
			l, d := setup(t)
			d.queues.EXPECT().Get(gomock.Any(), "org1", "q").Return(cfgFor("org1", "q", "node-1"), nil)
			d.series.EXPECT().
				Query(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, q entity.TimeseriesQuery) (entity.TimeseriesResponse, error) {
					if q.Range != c.want {
						t.Errorf("range %v, want %v", q.Range, c.want)
					}
					if q.Step <= 0 || q.Step >= q.Range {
						t.Errorf("step %v makes no sense for a %v window", q.Step, q.Range)
					}
					return entity.TimeseriesResponse{Available: true}, nil
				})

			got, err := l.Timeseries(context.Background(), "org1", "q", c.asked)
			if err != nil {
				t.Fatal(err)
			}
			if got.Range != c.label {
				t.Fatalf("reported range %q, want %q", got.Range, c.label)
			}
		})
	}
}

// Prometheus is optional. Without it the endpoint still answers, saying so, and
// the page falls back to sampling what it can itself.
func TestTimeseriesWithoutAMonitoringSystem(t *testing.T) {
	l, d := setup(t)
	d.queues.EXPECT().Get(gomock.Any(), "org1", "q").Return(cfgFor("org1", "q", "node-1"), nil)
	d.series.EXPECT().Query(gomock.Any(), gomock.Any()).
		Return(entity.TimeseriesResponse{Available: false}, nil)

	got, err := l.Timeseries(context.Background(), "org1", "q", "5m")
	if err != nil {
		t.Fatalf("a missing monitoring system is not an error: %v", err)
	}
	if got.Available {
		t.Fatal("reported available with nothing configured")
	}
}
