package redis_rate //nolint:revive // upstream used this name

import (
	"bytes"
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

var ErrScriptFailed = errors.New("redis_rate: invalid result from SCRIPT EXISTS in pipeline")
var ErrTooManyRetries = errors.New("redis_rate: pipeline too many retries to load scripts")

type Pipeline interface {
	Allow(ctx context.Context,
		key string,
		limit Limit,
	) *Result

	Take(ctx context.Context, key string, requestID string, limit ConcurrencyLimit) *ConcurrencyResult

	Release(ctx context.Context, key string, requestID string)

	Exec(ctx context.Context) error
}

func (l *Limiter) Pipeline() Pipeline {
	return &pipeline{
		l: l,
	}
}

type pair[TA any, TB any] struct {
	A TA
	B TB
}

type pipeline struct {
	l               *Limiter
	buf             bytes.Buffer
	releaseCommands []pair[string, string]
	allowCommands   []*Result
	takeCommands    []*ConcurrencyResult
}

func (p *pipeline) Allow(ctx context.Context,
	key string,
	limit Limit) *Result {
	rv := &Result{
		Key:   key,
		Limit: limit,
	}
	p.allowCommands = append(p.allowCommands, rv)
	return rv
}

func (p *pipeline) Take(ctx context.Context,
	key string, requestID string,
	limit ConcurrencyLimit) *ConcurrencyResult {
	rv := &ConcurrencyResult{
		Key:       key,
		Limit:     limit,
		RequestID: requestID,
	}
	p.takeCommands = append(p.takeCommands, rv)
	return rv
}

func (p *pipeline) Release(ctx context.Context,
	key string, requestID string) {
	p.releaseCommands = append(p.releaseCommands, pair[string, string]{key, requestID})
}

func (p *pipeline) Exec(ctx context.Context) error {
	return p.exec(ctx, 0)
}

func (p *pipeline) exec(ctx context.Context, depth int) error {
	if depth > 10 {
		return ErrTooManyRetries
	}

	finishFuncs := make([]func() error, 0, len(p.allowCommands))
	pipe := p.l.rdb.Pipeline()

	var scriptExistChecks []*redis.BoolSliceCmd

	if len(p.allowCommands) > 0 {
		scriptExistChecks = append(scriptExistChecks, allowN.Exists(ctx, pipe))
		for _, v := range p.allowCommands {
			finishFuncs = append(finishFuncs, p.allowPipe(ctx, pipe, v))
		}
	}

	if len(p.takeCommands) > 0 {
		scriptExistChecks = append(scriptExistChecks, concurrencyTake.Exists(ctx, pipe))
		for _, v := range p.takeCommands {
			finishFuncs = append(finishFuncs, p.takePipe(ctx, pipe, v))
		}
	}

	if len(p.releaseCommands) > 0 {
		p.l.releasePipe(ctx, pipe, p.releaseCommands)
	}

	_, err := pipe.Exec(ctx)
	if err != nil && !redis.HasErrorPrefix(err, "NOSCRIPT") {
		return err
	}

	for _, se := range scriptExistChecks {
		exists, err := se.Result()
		if err != nil {
			return err
		}
		if len(exists) != 1 {
			return ErrScriptFailed
		}
		if !exists[0] {
			err = p.l.LoadScripts(ctx)
			if err != nil {
				return err
			}
			return p.exec(ctx, depth+1)
		}
	}

	for _, fn := range finishFuncs {
		err := fn()
		if err != nil {
			return err
		}
	}

	return nil
}
