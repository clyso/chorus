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
	routeAddFlagUser    string
	routeAddFlagStorage string
	routeAddFlagBucket  string
)

func init() {
	routeCmd.AddCommand(routeAddCmd)
	routeAddCmd.Flags().StringVarP(&routeAddFlagUser, "user", "u", "", "routing policy user")
	routeAddCmd.Flags().StringVarP(&routeAddFlagStorage, "storage", "s", "", "destination storage")
	routeAddCmd.Flags().StringVarP(&routeAddFlagBucket, "bucket", "b", "", "routing policy users bucket")
	if err := routeAddCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := routeAddCmd.MarkFlagRequired("storage"); err != nil {
		logrus.WithError(err).Fatal()
	}
}

var routeAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add routing policy",
	Long: `Add routing policy.

Add user-level routing (all existing and future buckets for a user):
  chorctl route add --user my-user --storage my-storage-a

Add bucket-level routing (single bucket):
  chorctl route add --user my-user --bucket my-bucket --storage my-storage-a`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		req := &pb.AddRoutingRequest{
			User:      routeAddFlagUser,
			ToStorage: routeAddFlagStorage,
		}
		if routeAddFlagBucket != "" {
			req.Bucket = &routeAddFlagBucket
		}
		_, err := client.AddRouting(ctx, req)
		if err != nil {
			api.PrintGrpcError(err)
		}
	},
}
