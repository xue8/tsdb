package store

import (
	"context"
	"errors"
	"github.com/go-logr/logr"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	promtsdb "github.com/prometheus/prometheus/tsdb"
	"time"
)

var (
	ErrMetricNameNotFound = errors.New("metric name not found")
)

type Store interface {
	RangeQuery(ctx context.Context, qs string, start, end time.Time, interval time.Duration) (promql.Matrix, error)
	Query(ctx context.Context, qs string, ts time.Time) (promql.Vector, error)
	Push(ctx context.Context, sample promql.Sample) error
}

func NewTSDB(dir string, queryMaxSamples int, timeout, lookbackDelta time.Duration, logger logr.Logger) (Store, error) {
	db, err := promtsdb.Open(dir, nil, nil, nil, promtsdb.NewDBStats())
	if err != nil {
		return nil, err
	}

	opts := promql.EngineOpts{
		MaxSamples:    queryMaxSamples,
		Timeout:       timeout,
		LookbackDelta: lookbackDelta,
	}
	engine := promql.NewEngine(opts)

	return &tsdb{
		db:          db,
		queryEngine: engine,
		cache:       map[uint64]*memSeries{},
	}, nil
}

type tsdb struct {
	db          *promtsdb.DB
	queryEngine *promql.Engine
	cache       map[uint64]*memSeries
}

type memSeries struct {
	ref  storage.SeriesRef
	lset labels.Labels
}

func (t *tsdb) RangeQuery(ctx context.Context, qs string, start, end time.Time, interval time.Duration) (promql.Matrix, error) {
	opts := promql.QueryOpts{}
	query, err := t.queryEngine.NewRangeQuery(t.db, &opts, qs, start, end, interval)
	if err != nil {
		return nil, err
	}

	exec := query.Exec(ctx)
	return exec.Matrix()
}

func (t *tsdb) Query(ctx context.Context, qs string, ts time.Time) (promql.Vector, error) {
	opts := promql.QueryOpts{}
	query, err := t.queryEngine.NewInstantQuery(t.db, &opts, qs, ts)
	if err != nil {
		return nil, err
	}

	exec := query.Exec(ctx)
	return exec.Vector()
}

func (t *tsdb) Push(ctx context.Context, sample promql.Sample) error {
	var metric string
	for i := range sample.Metric {
		l := sample.Metric[i]
		if l.Name != "__name__" {
			continue
		}

		metric = l.Value
		break
	}

	if metric == "" {
		return ErrMetricNameNotFound
	}

	var ref storage.SeriesRef
	seriesRef := t.cache[sample.Metric.Hash()]
	if seriesRef != nil {
		ref = seriesRef.ref
	}

	appender := t.db.Head().Appender(ctx)
	ref, err := appender.Append(ref, sample.Metric, sample.T, sample.V)
	if err != nil {
		return err
	}

	if err := appender.Commit(); err != nil {
		return err
	}

	t.cache[sample.Metric.Hash()] = &memSeries{
		ref:  ref,
		lset: sample.Metric,
	}

	return nil
}
