/*
 * Copyright © 2025 Clyso GmbH
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
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"

	"github.com/spf13/cobra"
)

var consistencyCmd = &cobra.Command{
	Use:   "consistency",
	Short: "list consistency checks",
	Long: `Get a list of consistency checks run by chorus instance.
	
Example:
chorctl consistency`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()

		client := pb.NewChorusClient(conn)
		res, err := client.ListConsistencyChecks(ctx, &emptypb.Empty{})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get consistency checks")
		}

		sort.Slice(res.Checks, func(i, j int) bool {
			return res.Checks[i].Id < res.Checks[j].Id
		})

		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, api.ConsistencyCheckHeader())
		for _, check := range res.Checks {
			fmt.Fprintln(w, api.ConsistencyCheckRow(check))
		}
		w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(consistencyCmd)
}
