package store

import (
	"context"
	"github.com/go-logr/stdr"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"log"
	"os"
	"testing"
	"time"
)

var (
	db  = getStore()
	ctx = context.TODO()
	ts  = time.Now().Add(24 * time.Hour)
)

var (
	metric1 = "prometheus_tsdb_head_series_created_total"
)

func TestPush(t *testing.T) {
	tmp := ts
	for i := 0; i <= 100; i++ {
		sample := promql.Sample{
			Point: promql.Point{
				T: tmp.UnixMilli(),
				V: float64(i),
			},
			Metric: labels.Labels{{
				Name:  "__name__",
				Value: metric1,
			}},
		}

		if err := db.Push(ctx, sample); err != nil {
			t.Fatalf("push error: %v", err)
		}

		t.Log("push sample", "sample", sample, "time", tmp.String())
		tmp = tmp.Add(1 * time.Minute)
	}
}

func TestQueryRange(t *testing.T) {
	e := ts.Add(3 * time.Hour)

	t.Log("range query", "start", ts.String(), "end", e.String(), "metric", metric1)
	query, err := db.RangeQuery(ctx, metric1, ts, e, 1*time.Minute)
	if err != nil {
		t.Fatalf("range query error: %v", err)
	}

	t.Logf("metric: %v, ts: %v", metric1, query.String())
}

func getStore() Store {
	logger := stdr.New(log.New(os.Stdout, "", log.Lshortfile))
	db, err := NewTSDB("./data", 500, 10*time.Second, 5*time.Minute, logger)
	if err != nil {
		panic(err)
	}

	return db
}
