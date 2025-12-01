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

package cmd

import (
	"context"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/sirupsen/logrus"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"

	"github.com/spf13/cobra"
)

// Filters for `chorctl repl`.
var (
	replType       string
	replUserFilter string
	replFromFilter string
	replToFilter   string
	replFromBucket string
	replToBucket   string
	replHasSwitch  bool
	replAgentOnly  bool
)

// replCmd represents the repl command
var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "list and manage replication policies",
	Long: `List replication policies.

Examples:
  # list all policies
  chorctl repl

  # list only bucket-level policies
  chorctl repl --type=bucket

  # filter by user and from/to storages
  chorctl repl --user admin --from main --to follower`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		req := &pb.ListReplicationsRequest{}
		filter := &pb.ListReplicationsRequest_Filter{}

		// type filter: user/bucket/all
		switch replType {
		case "bucket":
			req.HideUserReplications = true
		case "user":
			req.HideBucketReplications = true
		case "agent":
			v := true
			filter.IsAgent = &v
		case "all", "":
			// default: no hiding
		default:
			logrus.Fatalf("invalid --type %q (must be one of: all, bucket, user)", replType)
		}

		anyFilter := false
		if replUserFilter != "" {
			filter.User = &replUserFilter
			anyFilter = true
		}
		if replFromFilter != "" {
			filter.FromStorage = &replFromFilter
			anyFilter = true
		}
		if replToFilter != "" {
			filter.ToStorage = &replToFilter
			anyFilter = true
		}
		if replFromBucket != "" {
			filter.FromBucket = &replFromBucket
			anyFilter = true
		}
		if replToBucket != "" {
			filter.ToBucket = &replToBucket
			anyFilter = true
		}
		if replHasSwitch {
			v := true
			filter.HasSwitch = &v
			anyFilter = true
		}
		if replAgentOnly {
			v := true
			filter.IsAgent = &v
			anyFilter = true
		}
		if anyFilter {
			req.Filter = filter
		}

		res, err := client.ListReplications(ctx, req)
		if err != nil {
			api.PrintGrpcError(err)
		}
		sort.Slice(res.Replications, func(i, j int) bool {
			return res.Replications[i].CreatedAt.AsTime().After(res.Replications[j].CreatedAt.AsTime())
		})

		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, api.ReplHeader())
		for _, m := range res.Replications {
			fmt.Fprintln(w, api.ReplRow(m))
		}
		w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(replCmd)

	replCmd.Flags().StringVar(&replType, "type", "all", "replication type filter: bucket, user, agent")
	replCmd.Flags().StringVarP(&replUserFilter, "user", "u", "", "filter by replication user")
	replCmd.Flags().StringVarP(&replFromFilter, "from", "f", "", "filter by source storage")
	replCmd.Flags().StringVarP(&replToFilter, "to", "t", "", "filter by destination storage")
	replCmd.Flags().StringVarP(&replFromBucket, "from-bucket", "b", "", "filter by source bucket of bucket-level replication")
	replCmd.Flags().StringVar(&replToBucket, "to-bucket", "", "filter by destination bucket of bucket-level replication")
}
