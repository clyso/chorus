package rpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/testutil"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

var _ Proxy = &mockProxy{}

type mockProxy struct{}

func (m mockProxy) GetCredentials(_ context.Context) (*pb.GetProxyCredentialsResponse, error) {
	return &pb.GetProxyCredentialsResponse{Address: "address", Credentials: []*pb.Credential{{
		Alias:     "admin",
		AccessKey: "access",
		SecretKey: "secret",
	}}}, nil
}

func TestProxyClient_GetCredentials(t *testing.T) {
	c := testutil.SetupRedis(t)
	r := require.New(t)
	ctx := t.Context()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = ProxyServe(ctx, c, &mockProxy{})
	}()
	t.Cleanup(func() {
		wg.Wait()
	})

	// Wait until the proxy's request subscription is active. A fixed sleep can
	// still race with slow test environments and Redis startup.
	r.Eventually(func() bool {
		subscribers, err := c.PubSubNumSub(ctx, proxyCreds).Result()
		return err == nil && subscribers[proxyCreds] == 1
	}, time.Second, 10*time.Millisecond)

	tst := ProxyClient{c}
	res, err := tst.GetCredentials(ctx)
	r.NoError(err)
	r.EqualValues("address", res.Address)
	r.Len(res.Credentials, 1)
	r.EqualValues("admin", res.Credentials[0].Alias)
	r.EqualValues("access", res.Credentials[0].AccessKey)
	r.EqualValues("secret", res.Credentials[0].SecretKey)
}
