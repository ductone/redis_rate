package redis_rate //nolint:revive // upstream used this name

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limit struct {
	Rate   int
	Burst  int
	Period time.Duration
}

func (l Limit) String() string {
	return fmt.Sprintf("%d req/%s (burst %d)", l.Rate, fmtDur(l.Period), l.Burst)
}

func (l Limit) IsZero() bool {
	return l == Limit{}
}

func fmtDur(d time.Duration) string {
	switch d {
	case time.Second:
		return "s"
	case time.Minute:
		return "m"
	case time.Hour:
		return "h"
	default:
		return d.String()
	}
}

func PerSecond(rate int) Limit {
	return Limit{
		Rate:   rate,
		Period: time.Second,
		Burst:  rate,
	}
}

func PerMinute(rate int) Limit {
	return Limit{
		Rate:   rate,
		Period: time.Minute,
		Burst:  rate,
	}
}

func PerHour(rate int) Limit {
	return Limit{
		Rate:   rate,
		Period: time.Hour,
		Burst:  rate,
	}
}

// ------------------------------------------------------------------------------

// Limiter controls how frequently events are allowed to happen.
type Limiter struct {
	rdb              RedisClientConn
	ratePrefix       string
	concurrentPrefix string
}

// Allow is a shortcut for AllowN(ctx, key, limit, 1).
func (l *Limiter) Allow(ctx context.Context, key string, limit Limit) (*Result, error) {
	return l.AllowN(ctx, key, limit, 1)
}

func (p *pipeline) allowPipe(ctx context.Context, pipe redis.Pipeliner, rv *Result) func() error {
	values := []interface{}{rv.Limit.Burst, rv.Limit.Rate, rv.Limit.Period.Seconds(), int(1)}
	p.buf.Reset()
	_, _ = p.buf.WriteString(p.l.ratePrefix)
	_, _ = p.buf.WriteString(rv.Key)

	eval := allowN.EvalSha(
		ctx,
		pipe,
		[]string{p.buf.String()},
		values...,
	)

	return func() error {
		v, err := eval.Result()
		if err != nil {
			return err
		}
		values := v.([]interface{})
		err = rv.parseScriptResult(values)
		if err != nil {
			return err
		}
		return nil
	}
}

func (rv *Result) parseScriptResult(values []interface{}) error {
	retryAfter, err := strconv.ParseFloat(values[2].(string), 64)
	if err != nil {
		return err
	}

	resetAfter, err := strconv.ParseFloat(values[3].(string), 64)
	if err != nil {
		return err
	}

	rv.Allowed = values[0].(int64)
	rv.Remaining = values[1].(int64)
	rv.Used = 0
	rv.RetryAfter = dur(retryAfter)
	rv.ResetAfter = dur(resetAfter)
	return nil
}

// AllowN reports whether n events may happen at time now.
func (l *Limiter) AllowN(
	ctx context.Context,
	key string,
	limit Limit,
	n int,
) (*Result, error) {
	values := []interface{}{limit.Burst, limit.Rate, limit.Period.Seconds(), n}
	v, err := allowN.Run(ctx, l.rdb, []string{l.ratePrefix + key}, values...).Result()
	if err != nil {
		return nil, err
	}

	values = v.([]interface{})

	rv := &Result{
		Key:   key,
		Limit: limit,
	}
	err = rv.parseScriptResult(values)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

// AllowAtMost reports whether at most n events may happen at time now.
// It returns number of allowed events that is less than or equal to n.
func (l *Limiter) AllowAtMost(
	ctx context.Context,
	key string,
	limit Limit,
	n int,
) (*Result, error) {
	values := []interface{}{limit.Burst, limit.Rate, limit.Period.Seconds(), n}
	v, err := allowAtMost.Run(ctx, l.rdb, []string{l.ratePrefix + key}, values...).Result()
	if err != nil {
		return nil, err
	}

	values = v.([]interface{})

	rv := &Result{
		Key:   key,
		Limit: limit,
	}
	err = rv.parseScriptResult(values)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

// Reset gets a key and reset all limitations and previous usages.
func (l *Limiter) Reset(ctx context.Context, key string) error {
	return l.rdb.Del(ctx, l.ratePrefix+key).Err()
}

func dur(f float64) time.Duration {
	if f == -1 {
		return -1
	}
	return time.Duration(f * float64(time.Second))
}

type Result struct {
	// Name of the key used for this result.
	Key string

	// Limit is the limit that was used to obtain this result.
	Limit Limit

	// Allowed is the number of events that may happen at time now.
	Allowed int64

	// Used is the number of events that have already happened at time now.
	Used int64

	// Remaining is the maximum number of requests that could be
	// permitted instantaneously for this key given the current
	// state. For example, if a rate limiter allows 10 requests per
	// second and has already received 6 requests for this key this
	// second, Remaining would be 4.
	Remaining int64

	// RetryAfter is the time until the next request will be permitted.
	// It should be -1 unless the rate limit has been exceeded.
	RetryAfter time.Duration

	// ResetAfter is the time until the RateLimiter returns to its
	// initial state for a given key. For example, if a rate limiter
	// manages requests per second and received one request 200ms ago,
	// Reset would return 800ms. You can also think of this as the time
	// until Limit and Remaining will be equal.
	ResetAfter time.Duration
}
