/*
 * Copyright © 2023 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package standalone

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestStandaloneDefaultConfig(t *testing.T) {
	r := require.New(t)
	conf, err := GetConfig()
	r.NoError(err)
	r.EqualValues(5*time.Second, conf.ShutdownTimeout)
	conf.ShutdownTimeout = 500 * time.Millisecond

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	done := false
	go func() {
		_ = Start(ctx, dom.AppInfo{}, conf)
		done = true
	}()

	grpcConn, err := grpc.DialContext(ctx, fmt.Sprintf("localhost:%d", conf.Api.GrpcPort),
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithBlock(),
	)
	r.NoError(err)
	apiClient := pb.NewChorusClient(grpcConn)

	r.Eventually(func() bool {
		res, err := apiClient.GetStorages(ctx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		return len(res.Storages) == 2
	}, 2*time.Second, 10*time.Millisecond, "standalone is not up")

	cancel()
	r.Eventually(func() bool {
		return done
	}, 2*time.Second, 10*time.Millisecond, "unable to shutdown standalone")
}
