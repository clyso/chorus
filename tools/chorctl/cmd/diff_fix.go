/*
 * Copyright © 2026 Clyso GmbH
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
	"strings"

	"github.com/sirupsen/logrus"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"

	"github.com/spf13/cobra"
)

var (
	sourceStorageBucket string
)

var diffFixCmd = &cobra.Command{
	Use:   "fix",
	Short: "start diff fix",
	Long: `Start diff fix to resolve inconsistencies.

Example:
# Fix bucket contents, while using storage1 storage as a source of truth
chorctl diff fix --source storage1:bucket1 storage2:bucket2 `,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		locations := make([]*pb.MigrateLocation, 0, len(args)+1)

		storage, bucket, found := strings.Cut(sourceStorageBucket, ":")
		if !found {
			logrus.WithField("source", sourceStorageBucket).Fatal("unable to get storage and bucket parts")
		}

		locations = append(locations, &pb.MigrateLocation{
			Storage: storage,
			Bucket:  bucket,
		})

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

		request := &pb.StartDiffFixRequest{
			Locations:   locations,
			SourceIndex: 0,
		}

		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()

		client := pb.NewDiffClient(conn)
		if _, err = client.Fix(ctx, request); err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to start diff fix")
		}
		fmt.Println("Diff fix has been created.")
	},
}

func init() {
	diffCmd.AddCommand(diffFixCmd)
	diffFixCmd.Flags().StringVarP(&sourceStorageBucket, "source", "s", "", "source storage and bucket")
	if err := diffFixCmd.MarkFlagRequired("source"); err != nil {
		logrus.WithError(err).Fatal()
	}
}
