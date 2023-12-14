package lock

import (
	"context"
	"errors"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/util"
	"github.com/rs/zerolog"
	"time"
)

var overlap = time.Second

func UpdateOverlap(m time.Duration) {
	overlap = m
}

func WithRefresh(ctx context.Context, work func() error, refresh func(time.Duration) error, period time.Duration) error {
	err := refresh(period + overlap)
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			return fmt.Errorf("%w: lock not obtained during refresh", &dom.ErrRateLimitExceeded{RetryIn: util.DurationJitter(time.Second*5, time.Second*30)})
		}
		return err
	}
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		errCh <- work()
	}()
	timer := time.NewTimer(period)
	defer timer.Stop()
	i := 0
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: refresh context canceled: %v", context.Canceled, ctx.Err())
		case <-timer.C:
			i++
			err = refresh(period + overlap)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).
					Str("refresh_period", period.String()).
					Str("refresh_overlap", overlap.String()).
					Int("refresh_iteration", i).
					Msg("unable to refresh lock")
				continue
			}
			timer.Reset(period)
		case err = <-errCh:
			return err
		}
	}
}
