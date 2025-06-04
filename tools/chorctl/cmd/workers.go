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

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

var workersCmd = &cobra.Command{
	Use:     "worker",
	Aliases: []string{"workers"},
	Short:   "Prints information about worker",
	Long:    ``,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)
		appVersion, err := client.GetAppVersion(ctx, &emptypb.Empty{})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get app version")
		}

		fmt.Printf("%q (Built on %q from Git SHA %q)", appVersion.Version, appVersion.Date, appVersion.Commit)
	},
}

func init() {
	rootCmd.AddCommand(workersCmd)
}
