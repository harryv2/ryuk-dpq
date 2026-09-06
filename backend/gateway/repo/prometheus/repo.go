// Package prometheus reads history back out of Prometheus.
package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/harryv2/ryuk-dpq/backend/gateway/entity"
	"github.com/harryv2/ryuk-dpq/backend/gateway/entity/enterr"
)

// BaseURL is a named type so the wire graph can tell it from any other string.
type BaseURL string

type Repo struct {
	base string
	http *http.Client
	log  *slog.Logger
}

func New(base BaseURL, log *slog.Logger) *Repo {
	return &Repo{
		base: strings.TrimRight(string(base), "/"),
		http: &http.Client{Timeout: 10 * time.Second},
		log:  log,
	}
}

// Configured reports whether there is anywhere to query. Prometheus is optional:
// without it the UI falls back to sampling the live endpoint itself.
func (r *Repo) Configured() bool { return r.base != "" }

func (r *Repo) Ready(ctx context.Context) bool {
	if !r.Configured() {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, "GET", r.base+"/-/ready", nil)
	if err != nil {
		return false
	}
	res, err := r.http.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	return res.StatusCode == http.StatusOK
}

// The queries the metrics page can ask for.
const (
	qReady     = `ryuk_queue_ready_messages{org=%q,queue=%q}`
	qInFlight  = `ryuk_queue_inflight_messages{org=%q,queue=%q}`
	qOldestAge = `ryuk_queue_oldest_message_age_seconds{org=%q,queue=%q}`
	qEnqueue   = `ryuk_queue_enqueue_rate{org=%q,queue=%q}`
	qAck       = `ryuk_queue_ack_rate{org=%q,queue=%q}`
)

func (r *Repo) Query(ctx context.Context, q entity.TimeseriesQuery) (entity.TimeseriesResponse, error) {
	out := entity.TimeseriesResponse{
		Range: q.Range.String(),
		Step:  q.Step.String(),
	}
	if !r.Configured() {
		return out, nil
	}
	out.Available = true

	// A label value with a quote or a backslash in it would break out of the
	// matcher, so it is escaped the way PromQL expects.
	org, name := escape(q.Org), escape(q.Queue)

	type ask struct {
		query string
		into  *[]entity.Series
		label string
	}
	asks := []ask{
		{fmt.Sprintf(qReady, org, name), &out.Ready, "priority"},
		{fmt.Sprintf(qInFlight, org, name), &out.InFlight, ""},
		{fmt.Sprintf(qOldestAge, org, name), &out.OldestAge, ""},
		{fmt.Sprintf(qEnqueue, org, name), &out.Rates, "enqueue"},
		{fmt.Sprintf(qAck, org, name), &out.Rates, "ack"},
	}

	for _, a := range asks {
		series, err := r.rangeQuery(ctx, a.query, q.Range, q.Step, a.label)
		if err != nil {
			return out, err
		}
		*a.into = append(*a.into, series...)
	}
	return out, nil
}

func (r *Repo) rangeQuery(
	ctx context.Context, query string, window, step time.Duration, label string,
) ([]entity.Series, error) {
	end := time.Now()
	start := end.Add(-window)

	v := url.Values{}
	v.Set("query", query)
	v.Set("start", strconv.FormatInt(start.Unix(), 10))
	v.Set("end", strconv.FormatInt(end.Unix(), 10))
	v.Set("step", strconv.FormatFloat(step.Seconds(), 'f', -1, 64))

	req, err := http.NewRequestWithContext(ctx, "GET", r.base+"/api/v1/query_range?"+v.Encode(), nil)
	if err != nil {
		return nil, enterr.Internal("build prometheus query", err)
	}
	res, err := r.http.Do(req)
	if err != nil {
		return nil, enterr.New(enterr.CodeExhausted, "monitoring system unavailable")
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, enterr.New(enterr.CodeExhausted,
			"monitoring system returned "+strconv.Itoa(res.StatusCode))
	}

	var body struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, enterr.Internal("decode prometheus response", err)
	}
	if body.Status != "success" {
		return nil, enterr.New(enterr.CodeExhausted, "monitoring query failed")
	}

	out := make([]entity.Series, 0, len(body.Data.Result))
	for _, s := range body.Data.Result {
		name := label
		if label != "" && label != "enqueue" && label != "ack" {
			name = s.Metric[label] // a per-series label, e.g. priority
		}
		points := make([]entity.SeriesPoint, 0, len(s.Values))
		for _, pair := range s.Values {
			at, ok := pair[0].(float64)
			if !ok {
				continue
			}
			raw, ok := pair[1].(string)
			if !ok {
				continue
			}
			val, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				continue // Prometheus reports NaN as a string; skip the gap
			}
			points = append(points, entity.SeriesPoint{
				At:    time.Unix(int64(at), 0).UTC(),
				Value: val,
			})
		}
		out = append(out, entity.Series{Name: name, Points: points})
	}
	return out, nil
}

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `"`, `\"`)
}
