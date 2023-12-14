package rpc

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"testing"
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
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = ProxyServe(ctx, c, &mockProxy{})
	}()
	tst := ProxyClient{c}
	res, err := tst.GetCredentials(ctx)
	r.NoError(err)
	r.EqualValues("address", res.Address)
	r.Len(res.Credentials, 1)
	r.EqualValues("admin", res.Credentials[0].Alias)
	r.EqualValues("access", res.Credentials[0].AccessKey)
	r.EqualValues("secret", res.Credentials[0].SecretKey)
}
