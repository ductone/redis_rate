package redis_rate_test

import (
	"context"
	"testing"
	"time"

	"github.com/ductone/redis_rate/v11"
	"github.com/stretchr/testify/require"
)

func TestTake(t *testing.T) {
	l := newTestLimiter(t)
	ctx := context.Background()

	r1, err := l.Take(ctx, "test_id", "req1", redis_rate.ConcurrencyLimit{
		Max:                1,
		RequestMaxDuration: time.Second * 5,
	})
	require.NoError(t, err)
	require.Equal(t, true, r1.Allowed)
	require.Equal(t, int64(0), r1.Remaining)
	require.Equal(t, int64(1), r1.Used)
	require.Equal(t, "test_id", r1.Key)

	r2, err := l.Take(ctx, "test_id", "req2", redis_rate.ConcurrencyLimit{
		Max:                1,
		RequestMaxDuration: time.Second * 5,
	})
	require.NoError(t, err)
	require.Equal(t, false, r2.Allowed)
	require.Equal(t, int64(0), r2.Remaining)
	require.Equal(t, int64(1), r2.Used)
	require.Equal(t, "test_id", r2.Key)

	r3, err := l.Take(ctx, "test_id", "req3", redis_rate.ConcurrencyLimit{
		Max:                2,
		RequestMaxDuration: time.Second * 5,
	})
	require.NoError(t, err)
	require.Equal(t, true, r3.Allowed)
	require.Equal(t, int64(0), r3.Remaining)
	require.Equal(t, int64(2), r3.Used)
	require.Equal(t, "test_id", r2.Key)

	err = l.Release(ctx, "test_id", "req1", redis_rate.ConcurrencyLimit{
		Max:                1,
		RequestMaxDuration: time.Second * 5,
	})
	require.NoError(t, err)

	r4, err := l.Take(ctx, "test_id", "req4", redis_rate.ConcurrencyLimit{
		Max:                1,
		RequestMaxDuration: time.Second * 5,
	})
	require.NoError(t, err)
	require.Equal(t, false, r4.Allowed)
	require.Equal(t, int64(0), r4.Remaining)
	require.Equal(t, int64(1), r4.Used)
	require.Equal(t, "test_id", r4.Key)
}
