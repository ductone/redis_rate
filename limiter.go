package redis_rate //nolint:revive // upstream used this name

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	defaultConcurrencyKeyPrefix = "concurrency:"
	defaultRedisPrefix          = "rate:"
)

// WithRatePrefix sets the prefix for rate limit keys
// when using the Limiter.  If unset the default is "rate:".
func WithRatePrefix(ratePrefix string) func(*Limiter) {
	return func(s *Limiter) {
		s.ratePrefix = ratePrefix
	}
}

// WithConcurrencyPrefix sets the prefix for concurrency limit keys.  If unset the default is "concurrency:".
func WithConcurrencyPrefix(concurrentPrefix string) func(*Limiter) {
	return func(s *Limiter) {
		s.concurrentPrefix = concurrentPrefix
	}
}

// New returns a new Limiter.
func New(rdb RedisClientConn, options ...func(*Limiter)) *Limiter {
	l := &Limiter{
		rdb:              rdb,
		ratePrefix:       defaultRedisPrefix,
		concurrentPrefix: defaultConcurrencyKeyPrefix,
	}

	for _, option := range options {
		option(l)
	}
	return l
}

func (l *Limiter) LoadScripts(ctx context.Context) error {
	_, err := concurrencyTake.Load(ctx, l.rdb).Result()
	if err != nil {
		return fmt.Errorf("redis_rate: failed to load 'script_concurrency_take.lua': %w", err)
	}

	_, err = allowN.Load(ctx, l.rdb).Result()
	if err != nil {
		return fmt.Errorf("redis_rate: failed to load 'script_allow_n.lua': %w", err)
	}

	_, err = allowAtMost.Load(ctx, l.rdb).Result()
	if err != nil {
		return fmt.Errorf("redis_rate: failed to load 'script_allow_at_most.lua': %w", err)
	}

	return nil
}

type RedisClientConn interface {
	Pipeline() redis.Pipeliner
	Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)

	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd

	EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd

	// redis.Cmdable // can uncomment when testing using new interface methods
}
