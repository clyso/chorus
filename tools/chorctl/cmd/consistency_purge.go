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

	"github.com/sirupsen/logrus"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"

	"github.com/spf13/cobra"
)

var consistencyPurgeCmd = &cobra.Command{
	Use:   "consistency purge",
	Short: "deletes result of consistency check",
	Long: `Example:
chorctl consistency purge 87009fd62551da208508`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()

		client := pb.NewChorusClient(conn)

		for _, id := range args {
			_, err := client.DeleteConsistencyCheckReport(ctx, &pb.DeleteConsistencyCheckReportRequest{Id: id})
			if err != nil {
				logrus.WithError(err).WithField("address", address).Fatal("unable to delete consistency check", id)
			} else {
				fmt.Println("Consistency check", id, "scheduled for deletion.")
				fmt.Println("Deletion will be completed asynchronously.")
			}
		}
	},
}

func init() {
	consistencyCmd.AddCommand(consistencyPurgeCmd)
}
