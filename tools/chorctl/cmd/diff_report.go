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

var (
	diffBriefReport bool
)

var diffReportCmd = &cobra.Command{
	Use:   "report",
	Short: "display diff check report",
	Long: `Get a detailed report on diff check done between 2 or more storages.
	
Example:
chorctl diff report oldstorage:bucket newstorage:altbucket`,
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
			})
		}

		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()

		client := pb.NewDiffClient(conn)
		res, err := client.GetReport(ctx, &pb.DiffCheckRequest{Locations: locations})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get diff check report")
		}

		storages := make([]string, 0, len(res.Check.Locations))
		for _, location := range res.Check.Locations {
			storages = append(storages, location.Storage)
		}

		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 1, ' ', 0)
		fmt.Fprintln(w, api.DiffReportBrief(res.Check))
		w.Flush()

		if res.Check.Consistent || !res.Check.Ready || diffBriefReport {
			return
		}

		fmt.Fprintln(w)
		w.Flush()

		withSizes := !res.Check.IgnoreSizes
		withEtags := !res.Check.IgnoreEtags && !res.Check.IgnoreSizes

		w = tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, api.DiffReportHeader(storages, res.Check.Versioned, withSizes, withEtags))

		pageSize := uint64(10)
		cursor := uint64(0)

		for {
			page, err := client.GetReportEntries(ctx, &pb.GetDiffCheckReportEntriesRequest{
				Locations: locations,
				Cursor:    cursor,
				PageSize:  pageSize,
			})
			if err != nil {
				logrus.WithError(err).WithField("address", address).Fatal("unable to get diff report entries")
			}

			for _, entry := range page.Entries {
				fmt.Fprintln(w, api.DiffReportRow(storages, entry, res.Check.Versioned, withSizes, withEtags))
			}

			if page.Cursor == 0 {
				break
			}

			cursor = page.Cursor
		}

		w.Flush()
	},
}

func init() {
	diffCmd.AddCommand(diffReportCmd)
	diffReportCmd.Flags().BoolVarP(&diffBriefReport, "brief", "b", false, "display only status without printing entries")
}
