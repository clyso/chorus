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
	"os"
	"text/tabwriter"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

var (
	rgFrom     string
	rgTo       string
	rgUser     string
	rgBucket   string
	rgToBucket string
)

var getReplicationStatusCmd = &cobra.Command{
	Use:   "get",
	Short: "Get replication status",
	Long: `Example:
chorctl repl get -f main -t follower -u admin -b bucket1`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewPolicyClient(conn)

		req := &pb.ReplicationID{
			User:        rgUser,
			FromBucket:  &rgBucket,
			FromStorage: rgUser,
			ToStorage:   rgTo,
			ToBucket:    &rgToBucket,
		}
		if rgToBucket == "" {
			req.ToBucket = &rgBucket
		}

		res, err := client.GetReplication(ctx, req)
		if err != nil {
			logrus.WithError(err).Fatal("unable to add replication")
		}

		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, api.ReplHeader())
		fmt.Fprintln(w, api.ReplRow(res))
		w.Flush()
	},
}

func formatTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return "-"
	}
	return ts.AsTime().Format(time.RFC3339)
}

func derefString(s *string) string {
	if s == nil {
		return "-"
	}
	return *s
}

func init() {
	replCmd.AddCommand(getReplicationStatusCmd)
	getReplicationStatusCmd.Flags().StringVarP(&rgFrom, "from", "f", "", "from storage")
	getReplicationStatusCmd.Flags().StringVarP(&rgTo, "to", "t", "", "to storage")
	getReplicationStatusCmd.Flags().StringVarP(&rgUser, "user", "u", "", "storage user")
	getReplicationStatusCmd.Flags().StringVarP(&rgBucket, "bucket", "b", "", "bucket name")
	getReplicationStatusCmd.Flags().StringVar(&rgToBucket, "to-bucket", "", "to bueckt name")
	err := getReplicationStatusCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = getReplicationStatusCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = getReplicationStatusCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = getReplicationStatusCmd.MarkFlagRequired("bucket")
	if err != nil {
		logrus.WithError(err).Fatal()
	}

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// addCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
