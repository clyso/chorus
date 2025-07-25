package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func Test_Pipeline(t *testing.T) {
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.Background()

	userRoutingPolicyStore := NewUserRoutingPolicyStore(c)
	routingBlockStore := NewRoutingBlockStore(c)

	exec := userRoutingPolicyStore.GroupExecutor()

	err := userRoutingPolicyStore.WithExecutor(exec).Set(ctx, "a", "b")
	fmt.Println(err)
	affected, err := routingBlockStore.WithExecutor(exec).Add(ctx, "d", "e", "f")
	fmt.Println(affected, err)
	err = exec.Exec(ctx)
	fmt.Println("p", affected, err)
}
