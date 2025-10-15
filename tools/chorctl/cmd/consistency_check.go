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
	"strings"

	"github.com/sirupsen/logrus"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"

	"github.com/spf13/cobra"
)

var (
	consistencyCheckUser  string
	ignoreEtags           bool
	ignoreSizes           bool
	checkOnlyLastVersions bool
)

var consistencyCheckCmd = &cobra.Command{
	Use:   "check",
	Short: "start consistency check",
	Long: `Start consistency check against 2 or more buckets.

Example:
# Check bucket contents by matching etag
chorctl consistency check storage1:bucket1 storage2:bucket2 --user username
# Check bucket contents by matching sizes
chorctl consistency check storage1:bucket1 storage2:bucket2 --user username --ignore-etags
# Check bucket contents by matching object list
chorctl consistency check storage1:bucket1 storage2:bucket2 --user username --ignore-sizes
# Check bucket contents by matching last versions of object
chorctl consistency check storage1:bucket1 storage2:bucket2 --user username --last-versions-only`,
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

		if ignoreSizes {
			ignoreEtags = true
		}

		request := &pb.StartConsistencyCheckRequest{
			Locations:             locations,
			User:                  consistencyCheckUser,
			IgnoreEtags:           ignoreEtags,
			IgnoreSizes:           ignoreSizes,
			CheckOnlyLastVersions: checkOnlyLastVersions,
		}

		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()

		client := pb.NewChorusClient(conn)
		if _, err = client.StartConsistencyCheck(ctx, request); err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get replications")
		}
		fmt.Println("Consistency check has been created.")
	},
}

func init() {
	consistencyCmd.AddCommand(consistencyCheckCmd)
	consistencyCheckCmd.Flags().StringVarP(&consistencyCheckUser, "user", "u", "", "storage user")
	consistencyCheckCmd.Flags().BoolVarP(&ignoreEtags, "ignore-etags", "e", false, "do not perform etag check")
	consistencyCheckCmd.Flags().BoolVarP(&ignoreSizes, "ignore-sizes", "s", false, "do not perform etag and size check")
	consistencyCheckCmd.Flags().BoolVarP(&checkOnlyLastVersions, "last-versions-only", "l", false, "check only last versions")
	err := consistencyCheckCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
}
