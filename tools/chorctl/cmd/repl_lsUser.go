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
	"google.golang.org/protobuf/types/known/emptypb"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// lsUserCmd represents the lsUser command
var lsUserCmd = &cobra.Command{
	Use:   "ls-user",
	Short: "Lists user-level replications",
	Long: `Example:
chorctl repl ls-user`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		res, err := client.ListUserReplications(ctx, &emptypb.Empty{})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get buckets for replication")
		}

		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, "FROM\tTO\tUSER")
		for _, m := range res.Replications {
			fmt.Fprintln(w, fmt.Sprintf("%s\t%s\t%s", m.From, m.To, m.User))
		}
		w.Flush()
	},
}

func init() {
	replCmd.AddCommand(lsUserCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// lsUserCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// lsUserCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
