package redis_rate //nolint:revive // upstream used this name

import (
	"bytes"
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type ConcurrencyLimit struct {
	Max int64
	// RequestMaxDuration is the time period in seconds over which the a request must complete.  If unset it defaults to 30 seconds.
	RequestMaxDuration time.Duration
}

type ConcurrencyResult struct {
	// Name of the key used for this result.
	Key string

	// Request ID used for this result.
	RequestID string

	// Limit is the limit that was used to obtain this result.
	Limit ConcurrencyLimit

	// Allowed is the number of events that may happen at time now.
	Allowed bool

	// Used is the number of events that have already happened at time now.
	Used int64

	// Remaining is the maximum number of requests that could be
	// permitted instantaneously for this key given the current
	// state. For example, if a rate limiter allows 10 requests per
	// second and has already received 6 requests for this key this
	// second, Remaining would be 4.
	Remaining int64
}

func (tk *Limiter) Take(ctx context.Context, key string, requestID string, limit ConcurrencyLimit) (ConcurrencyResult, error) {
	rv, err := tk.takeMulti(ctx, requestID, map[string]ConcurrencyLimit{key: limit}, 0)
	if err != nil {
		return ConcurrencyResult{}, err
	}
	return rv[key], nil
}

func (p *pipeline) takePipe(ctx context.Context, pipe redis.Pipeliner, rv *ConcurrencyResult) func() error {
	p.buf.Reset()
	_, _ = p.buf.WriteString(p.l.concurrentPrefix)
	_, _ = p.buf.WriteString(rv.Key)

	reqPeriod := rv.Limit.RequestMaxDuration.Round(time.Second) / time.Second
	if reqPeriod <= 0 {
		reqPeriod = 60
	}

	values := []interface{}{rv.RequestID, rv.Limit.Max, reqPeriod}

	eval := concurrencyTake.EvalSha(ctx, pipe, []string{p.buf.String()}, values...)
	return func() error {
		v, err := eval.Result()
		if err != nil {
			return err
		}
		values := v.([]interface{})

		ok := values[0].(int64) == 1
		current := values[1].(int64)
		rv.Allowed = ok
		rv.Used = current
		rv.Remaining = rv.Limit.Max - current
		return nil
	}
}

func (tk *Limiter) Release(ctx context.Context, key string, requestID string, limit ConcurrencyLimit) error {
	err := tk.releaseMulti(ctx, requestID, map[string]ConcurrencyLimit{key: limit})
	if err != nil {
		return err
	}
	return nil
}

func (tk *Limiter) releasePipe(ctx context.Context, pipe redis.Pipeliner, items []pair[string, string]) {
	buf := bytes.Buffer{}
	for _, v := range items {
		buf.Reset()
		_, _ = buf.WriteString(tk.concurrentPrefix)
		_, _ = buf.WriteString(v.A)
		pipe.HDel(ctx, buf.String(), v.B)
	}
}

func (tk *Limiter) releaseMulti(ctx context.Context, requestID string, limits map[string]ConcurrencyLimit) error {
	pl := tk.rdb.Pipeline()

	// Release any concurrency limits.
	buf := bytes.Buffer{}
	for key := range limits {
		buf.Reset()
		_, _ = buf.WriteString(tk.concurrentPrefix)
		_, _ = buf.WriteString(key)
		pl.HDel(ctx, buf.String(), requestID)
	}

	if pl.Len() == 0 {
		return nil
	}

	_, err := pl.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

type takeResult struct {
	key   string
	limit ConcurrencyLimit
	cmd   *redis.Cmd
}

func (tk *Limiter) takeMulti(ctx context.Context, requestID string, limits map[string]ConcurrencyLimit, depth int) (map[string]ConcurrencyResult, error) {
	if depth > 10 {
		return nil, ErrTooManyRetries
	}

	results := make([]*takeResult, 0, len(limits))
	buf := bytes.Buffer{}
	pl := tk.rdb.Pipeline()
	existsCmd := concurrencyTake.Exists(ctx, pl)
	for key, limit := range limits {
		reqPeriod := limit.RequestMaxDuration.Round(time.Second) / time.Second
		if reqPeriod <= 0 {
			reqPeriod = 60
		}
		values := []interface{}{requestID, limit.Max, reqPeriod}

		buf.Reset()
		_, _ = buf.WriteString(tk.concurrentPrefix)
		_, _ = buf.WriteString(key)

		results = append(results, &takeResult{
			key:   key,
			limit: limit,
			cmd: concurrencyTake.EvalSha(
				ctx,
				pl,
				[]string{buf.String()},
				values...,
			),
		})
	}
	if len(results) == 0 {
		return nil, nil
	}
	_, err := pl.Exec(ctx)
	if err != nil {
		return nil, err
	}

	exists, err := existsCmd.Result()
	if err != nil {
		return nil, err
	}
	if len(exists) != 1 {
		return nil, ErrScriptFailed
	}

	if !exists[0] {
		err = tk.LoadScripts(ctx)
		if err != nil {
			return nil, err
		}
		return tk.takeMulti(ctx, requestID, limits, depth+1)
	}

	rv := make(map[string]ConcurrencyResult, len(results))
	for _, result := range results {
		v, err := result.cmd.Result()
		if err != nil {
			return nil, err
		}
		values := v.([]interface{})

		ok := values[0].(int64) == 1
		current := values[1].(int64)
		cr := ConcurrencyResult{
			RequestID: requestID,
			Key:       result.key,
			Allowed:   ok,
			Limit:     result.limit,
			Used:      current,
			Remaining: result.limit.Max - current,
		}
		rv[result.key] = cr
	}

	return rv, nil
}
