package test

import (
	"testing"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
	"github.com/stretchr/testify/require"
)

func TestApi_Routing(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	ctx := t.Context()
	r := require.New(t)

	var (
		user = "test"

		bucket  = "routing-test"
		bucket2 = "routing-test2"
		bucket3 = "routing-test3"
	)

	testRes, err := e.PolicyClient.TestProxy(ctx, &pb.TestProxyRequest{
		User:   user,
		Bucket: bucket,
	})
	r.NoError(err)
	r.EqualValues("main", testRes.RouteToStorage)
	r.Empty(testRes.Replications)

	_, err = e.PolicyClient.AddRouting(ctx, &pb.AddRoutingRequest{
		User:      user,
		ToStorage: "f1",
	})
	r.NoError(err, "add user routing policy")

	_, err = e.PolicyClient.AddRouting(ctx, &pb.AddRoutingRequest{
		User:      user,
		Bucket:    &bucket2,
		ToStorage: "f2",
	})
	r.NoError(err, "add bucket routing policy")

	_, err = e.PolicyClient.AddRouting(ctx, &pb.AddRoutingRequest{
		User:      user,
		Bucket:    &bucket3,
		ToStorage: "f1",
	})
	r.NoError(err, "add bucket routing policy")

	_, err = e.PolicyClient.BlockRouting(ctx, &pb.RoutingID{
		User:   user,
		Bucket: &bucket3,
	})
	r.NoError(err, "block bucket routing")

	_, err = e.PolicyClient.BlockRouting(ctx, &pb.RoutingID{
		User:   user,
		Bucket: &bucket,
	})
	r.NoError(err, "block bucket routing")

	routings, err := e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{})
	r.NoError(err, "list routings")
	r.EqualValues("main", routings.Main)
	r.Len(routings.UserRoutings, 1)
	ur := routings.UserRoutings[0]
	r.EqualValues(user, ur.User)
	r.EqualValues("f1", ur.ToStorage)
	r.False(ur.IsBlocked)

	r.Len(routings.BucketRoutings, 3)
	for _, br := range routings.BucketRoutings {
		r.EqualValues(user, br.User)
		switch br.Bucket {
		case bucket:
			r.EqualValues("", br.ToStorage)
			r.True(br.IsBlocked)
		case bucket2:
			r.EqualValues("f2", br.ToStorage)
			r.False(br.IsBlocked)
		case bucket3:
			r.EqualValues("f1", br.ToStorage)
			r.True(br.IsBlocked)
		default:
			t.Fatalf("unexpected bucket routing: %+v", br)
		}
	}

	testRes, err = e.PolicyClient.TestProxy(ctx, &pb.TestProxyRequest{
		User:   user,
		Bucket: bucket,
	})
	r.NoError(err)
	r.True(testRes.IsBlocked)

	testRes, err = e.PolicyClient.TestProxy(ctx, &pb.TestProxyRequest{
		User:   user,
		Bucket: bucket2,
	})
	r.NoError(err)
	r.EqualValues("f2", testRes.RouteToStorage)

	testRes, err = e.PolicyClient.TestProxy(ctx, &pb.TestProxyRequest{
		User:   user,
		Bucket: bucket3,
	})
	r.NoError(err)
	r.True(testRes.IsBlocked)

	testRes, err = e.PolicyClient.TestProxy(ctx, &pb.TestProxyRequest{
		User:   user,
		Bucket: "some-other-bucket",
	})
	r.NoError(err)
	r.EqualValues("f1", testRes.RouteToStorage, "fallback to user routing")

	// unblock bucket3 routing
	_, err = e.PolicyClient.UnblockRouting(ctx, &pb.RoutingID{
		User:   user,
		Bucket: &bucket3,
	})
	r.NoError(err, "unblock bucket routing")

	testRes, err = e.PolicyClient.TestProxy(ctx, &pb.TestProxyRequest{
		User:   user,
		Bucket: bucket3,
	})
	r.NoError(err)
	r.EqualValues("f1", testRes.RouteToStorage)

	routings, err = e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{})
	r.NoError(err, "list routings")
	r.EqualValues("main", routings.Main)
	r.Len(routings.UserRoutings, 1)
	ur = routings.UserRoutings[0]
	r.EqualValues(user, ur.User)
	r.EqualValues("f1", ur.ToStorage)
	r.False(ur.IsBlocked)

	r.Len(routings.BucketRoutings, 3)
	for _, br := range routings.BucketRoutings {
		r.EqualValues(user, br.User)
		switch br.Bucket {
		case bucket:
			r.EqualValues("", br.ToStorage)
			r.True(br.IsBlocked)
		case bucket2:
			r.EqualValues("f2", br.ToStorage)
			r.False(br.IsBlocked)
		case bucket3:
			r.EqualValues("f1", br.ToStorage)
			r.False(br.IsBlocked)
		default:
			t.Fatalf("unexpected bucket routing: %+v", br)
		}
	}

	// test filters
	// filter blocked
	routings, err = e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{
		Filter: &pb.RoutingsRequest_Filter{
			IsBlocked: ptrBool(true),
		},
	})
	r.NoError(err, "list routings with filter IsBlocked=true")
	r.Len(routings.UserRoutings, 0)
	r.Len(routings.BucketRoutings, 1)
	br := routings.BucketRoutings[0]
	r.EqualValues(user, br.User)
	r.EqualValues(bucket, br.Bucket)
	r.EqualValues("", br.ToStorage)
	r.True(br.IsBlocked)
	// filter unblocked
	routings, err = e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{
		Filter: &pb.RoutingsRequest_Filter{
			IsBlocked: ptrBool(false),
		},
	})
	r.NoError(err, "list routings with filter IsBlocked=false")
	r.Len(routings.UserRoutings, 1)
	ur = routings.UserRoutings[0]
	r.EqualValues(user, ur.User)
	r.EqualValues("f1", ur.ToStorage)
	r.False(ur.IsBlocked)
	r.Len(routings.BucketRoutings, 2)
	for _, br := range routings.BucketRoutings {
		r.EqualValues(user, br.User)
		switch br.Bucket {
		case bucket2:
			r.EqualValues("f2", br.ToStorage)
			r.False(br.IsBlocked)
		case bucket3:
			r.EqualValues("f1", br.ToStorage)
			r.False(br.IsBlocked)
		default:
			t.Fatalf("unexpected bucket routing: %+v", br)
		}
	}

	// filter by bucket
	routings, err = e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{
		Filter: &pb.RoutingsRequest_Filter{
			Bucket: &bucket2,
		},
	})
	r.NoError(err, "list routings with filter Bucket=bucket2")
	r.Len(routings.UserRoutings, 1)
	r.Len(routings.BucketRoutings, 1)
	br = routings.BucketRoutings[0]
	r.EqualValues(user, br.User)
	r.EqualValues(bucket2, br.Bucket)
	r.EqualValues("f2", br.ToStorage)
	r.False(br.IsBlocked)

	// filter by user
	routings, err = e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{
		Filter: &pb.RoutingsRequest_Filter{
			User: &user,
		},
	})
	r.NoError(err, "list routings with filter User=user")
	r.Len(routings.UserRoutings, 1)
	ur = routings.UserRoutings[0]
	r.EqualValues(user, ur.User)
	r.EqualValues("f1", ur.ToStorage)
	r.False(ur.IsBlocked)
	r.Len(routings.BucketRoutings, 3)
	//invalid user
	invalidUser := "invalid-user"
	routings, err = e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{
		Filter: &pb.RoutingsRequest_Filter{
			User: &invalidUser,
		},
	})
	r.NoError(err, "list routings with filter User=invalid-user")
	r.Len(routings.UserRoutings, 0)
	r.Len(routings.BucketRoutings, 0)

	//filter by ToStorage
	routings, err = e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{
		Filter: &pb.RoutingsRequest_Filter{
			ToStorage: ptrString("f1"),
		},
	})
	r.NoError(err, "list routings with filter ToStorage=f1")
	r.Len(routings.UserRoutings, 1)
	ur = routings.UserRoutings[0]
	r.EqualValues(user, ur.User)
	r.EqualValues("f1", ur.ToStorage)
	r.False(ur.IsBlocked)
	r.Len(routings.BucketRoutings, 2)
	for _, br := range routings.BucketRoutings {
		r.EqualValues(user, br.User)
		switch br.Bucket {
		case bucket:
			r.EqualValues("", br.ToStorage)
			r.True(br.IsBlocked)
		case bucket3:
			r.EqualValues("f1", br.ToStorage)
			r.False(br.IsBlocked)
		default:
			t.Fatalf("unexpected bucket routing: %+v", br)
		}
	}

	// delete bucket policy and keep block
	_, err = e.PolicyClient.DeleteRouting(ctx, &pb.RoutingID{
		User:   user,
		Bucket: &bucket,
	})
	r.NoError(err, "delete bucket routing")
	routings, err = e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{})
	r.NoError(err, "list routings")
	r.Len(routings.UserRoutings, 1)
	r.Len(routings.BucketRoutings, 3)
	for _, br := range routings.BucketRoutings {
		r.EqualValues(user, br.User)
		switch br.Bucket {
		case bucket:
			r.EqualValues("", br.ToStorage)
			r.True(br.IsBlocked)
		case bucket2:
			r.EqualValues("f2", br.ToStorage)
			r.False(br.IsBlocked)
		case bucket3:
			r.EqualValues("f1", br.ToStorage)
			r.False(br.IsBlocked)
		default:
			t.Fatalf("unexpected bucket routing: %+v", br)
		}
	}

	// delete user routing
	_, err = e.PolicyClient.DeleteRouting(ctx, &pb.RoutingID{
		User: user,
	})
	r.NoError(err, "delete user routing")
	// delete bucket block
	_, err = e.PolicyClient.UnblockRouting(ctx, &pb.RoutingID{
		User:   user,
		Bucket: &bucket,
	})
	r.NoError(err, "delete bucket block")

	routings, err = e.PolicyClient.ListRoutings(ctx, &pb.RoutingsRequest{})
	r.NoError(err, "list routings")
	r.Len(routings.UserRoutings, 0)
	r.Len(routings.BucketRoutings, 2)
	for _, br := range routings.BucketRoutings {
		r.EqualValues(user, br.User)
		switch br.Bucket {
		case bucket2:
			r.EqualValues("f2", br.ToStorage)
			r.False(br.IsBlocked)
		case bucket3:
			r.EqualValues("f1", br.ToStorage)
			r.False(br.IsBlocked)
		default:
			t.Fatalf("unexpected bucket routing: %+v", br)
		}
	}
}

func ptrBool(b bool) *bool {
	return &b
}

func ptrString(s string) *string {
	return &s
}
