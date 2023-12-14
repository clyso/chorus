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
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"github.com/sirupsen/logrus"
	"os"
	"sort"
	"text/tabwriter"

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
	Short: "Lists buckets without replication",
	Long: `Example:
chorctl repl buckets -f main -t follower -u admin`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)
		res, err := client.ListBucketsForReplication(ctx, &pb.ListBucketsForReplicationRequest{
			User:           rbUser,
			From:           rbFrom,
			To:             rbTo,
			ShowReplicated: false,
		})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get buckets for replication")
		}
		sort.Strings(res.Buckets)

		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, "BUCKET")
		for _, m := range res.Buckets {
			fmt.Fprintln(w, m)
		}
		w.Flush()
	},
}

func init() {
	replCmd.AddCommand(bucketsCmd)
	bucketsCmd.Flags().StringVarP(&rbFrom, "from", "f", "", "from storage")
	bucketsCmd.Flags().StringVarP(&rbTo, "to", "t", "", "to storage")
	bucketsCmd.Flags().StringVarP(&rbUser, "user", "u", "", "storage user")
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
