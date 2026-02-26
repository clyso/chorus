// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

var (
	routeBlockFlagUser   string
	routeBlockFlagBucket string
)

func init() {
	routeCmd.AddCommand(routeBlockCmd)
	routeBlockCmd.Flags().StringVarP(&routeBlockFlagUser, "user", "u", "", "routing policy user")
	routeBlockCmd.Flags().StringVarP(&routeBlockFlagBucket, "bucket", "b", "", "routing policy users bucket")
	if err := routeBlockCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
}

var routeBlockCmd = &cobra.Command{
	Use:   "block",
	Short: "Block routing policy",
	Long: `Block routing policy.

Block user-level routing policy:
  chorctl route block --user my-user

Block bucket-level routing policy:
  chorctl route block --user my-user --bucket my-bucket`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		req := &pb.RoutingID{
			User: routeBlockFlagUser,
		}
		if routeBlockFlagBucket != "" {
			req.Bucket = &routeBlockFlagBucket
		}
		_, err := client.BlockRouting(ctx, req)
		if err != nil {
			api.PrintGrpcError(err)
		}
	},
}
