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
	"strings"
	"text/tabwriter"

	"github.com/sirupsen/logrus"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"

	"github.com/spf13/cobra"
)

var consistencyReportCmd = &cobra.Command{
	Use:   "report",
	Short: "display consistency check report",
	Long: `Get a detailed report on consistency check done between 2 or more storages.
	
Example:
chorctl consistency report oldstorage:bucket newstorage:altbucket`,
	Args: cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		locations := make([]*pb.MigrateLocation, 0, len(args))
		for _, arg := range args {
			storage, bucket, found := strings.Cut(arg, ":")
			if !found {
				logrus.WithField("arg", arg).Fatal("unable to get storage and bucket parts")
			}
			locations = append(locations, &pb.MigrateLocation{
				Storage: storage,
				Bucket:  bucket,
				User:    consistencyCheckUser,
			})
		}

		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()

		client := pb.NewChorusClient(conn)
		res, err := client.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get consistency check report")
		}

		storages := make([]string, 0, len(res.Check.Locations))
		for _, location := range res.Check.Locations {
			storages = append(storages, location.Storage)
		}

		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, api.ConsistencyCheckReportBrief(res.Check))
		fmt.Fprintln(w)
		w.Flush()

		if res.Check.Consistent {
			return
		}

		fmt.Fprintln(w, api.ConsistencyCheckReportHeader(storages))

		pageSize := int64(10)
		cursor := uint64(0)

		for {
			res, err := client.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				Cursor:    cursor,
				PageSize:  pageSize,
			})
			if err != nil {
				logrus.WithError(err).WithField("address", address).Fatal("unable to get consistency check report entries")
			}

			for _, entry := range res.Entries {
				fmt.Fprintln(w, api.ConsistencyCheckReportRow(storages, entry))
			}

			if res.Cursor == 0 {
				break
			}

			cursor = res.Cursor
		}

		w.Flush()
	},
}

func init() {
	consistencyCmd.AddCommand(consistencyReportCmd)
}
