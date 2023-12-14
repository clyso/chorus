package rpc

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAgent(t *testing.T) {
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = AgentServe(ctx, c, "agent1", "s1")
	}()
	go func() {
		_ = AgentServe(ctx, c, "agent2", "s2")
	}()
	go func() {
		_ = AgentServe(ctx, c, "agent3", "s3")
	}()
	tst := AgentClient{c}
	time.Sleep(time.Millisecond)
	res, err := tst.Ping(ctx)
	r.NoError(err)
	res, err = tst.Ping(ctx)
	r.NoError(err)
	r.Len(res, 3)
	r.ElementsMatch(res, []AgentInfo{
		{URL: "agent1", FromStorage: "s1"},
		{URL: "agent2", FromStorage: "s2"},
		{URL: "agent3", FromStorage: "s3"},
	})
}
