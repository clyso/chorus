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
	routeDelFlagUser   string
	routeDelFlagBucket string
)

func init() {
	routeCmd.AddCommand(routeDelCmd)
	routeDelCmd.Flags().StringVarP(&routeDelFlagUser, "user", "u", "", "routing policy user")
	routeDelCmd.Flags().StringVarP(&routeDelFlagBucket, "bucket", "b", "", "routing policy users bucket")
	if err := routeDelCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
}

var routeDelCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete routing policy",
	Long: `Delete routing policy.

Delete user-level routing policy:
  chorctl route delete --user my-user

Delete bucket-level routing policy:
  chorctl route delete --user my-user --bucket my-bucket`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		req := &pb.RoutingID{
			User: routeDelFlagUser,
		}
		if routeDelFlagBucket != "" {
			req.Bucket = &routeDelFlagBucket
		}
		_, err := client.DeleteRouting(ctx, req)
		if err != nil {
			api.PrintGrpcError(err)
		}
	},
}
