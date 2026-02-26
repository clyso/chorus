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
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

var (
	testProxyFlagUser   string
	testProxyFlagBucket string
)

func init() {
	rootCmd.AddCommand(testProxyCmd)
	testProxyCmd.Flags().StringVarP(&testProxyFlagUser, "user", "u", "", "filter by routing policy user")
	testProxyCmd.Flags().StringVarP(&testProxyFlagBucket, "bucket", "b", "", "filter by routing policy bucket")
	if err := testProxyCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := testProxyCmd.MarkFlagRequired("bucket"); err != nil {
		logrus.WithError(err).Fatal()
	}
}

var testProxyCmd = &cobra.Command{
	Use:   "test-proxy",
	Short: "Emulates storage request to chorus proxy to get applied routing/replication policies",
	Long: `Examples:

Emulate request to chorus proxy:
  chorctl test-proxy --user my-user --bucket my-bucket`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		res, err := client.TestProxy(ctx, &pb.TestProxyRequest{
			User:   testProxyFlagUser,
			Bucket: testProxyFlagBucket,
		})
		if err != nil {
			api.PrintGrpcError(err)
		}
		if res.IsBlocked {
			fmt.Println("Routing is blocked for this user/bucket")
			return
		}
		fmt.Printf("Routed to: %s\n", res.RouteToStorage)
		if len(res.Replications) == 0 {
			fmt.Println("Replications: -")
			return
		}
		fmt.Println("Replications:")
		for _, r := range res.Replications {
			fmt.Printf("  - %s\n", api.ReplIDToString(r))
		}
	},
}
