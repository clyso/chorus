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

var (
	rbFrom string
	rbTo   string
	rbUser string
)

// bucketsCmd represents the buckets command
var bucketsCmd = &cobra.Command{
	Use:   "buckets",
	Short: "list buckets available for replication",
	Long: `List buckets available for replication for a given user and from/to storages.

Example:
  chorctl repl buckets --from main --to follower --user admin
  chorctl repl buckets --from main --to follower --user admin --show-replicated`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		res, err := client.AvailableBuckets(ctx, &pb.AvailableBucketsRequest{
			User:        rbUser,
			FromStorage: rbFrom,
			ToStorage:   rbTo,
		})
		if err != nil {
			api.PrintGrpcError(err)
		}
		sort.Strings(res.Buckets)

		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		for _, m := range res.Buckets {
			fmt.Fprintln(w, m)
		}
		w.Flush()
	},
}

func init() {
	replCmd.AddCommand(bucketsCmd)
	bucketsCmd.Flags().StringVarP(&rbFrom, "from", "f", "", "source storage name")
	bucketsCmd.Flags().StringVarP(&rbTo, "to", "t", "", "destination storage name")
	bucketsCmd.Flags().StringVarP(&rbUser, "user", "u", "", "replication user")
	err := bucketsCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = bucketsCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = bucketsCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// bucketsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// bucketsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
