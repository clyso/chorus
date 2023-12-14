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
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	raFrom     string
	raTo       string
	raUser     string
	raAgentURL string
	raBuckets  []string
)

// addCmd represents the add command
var addCmd = &cobra.Command{
	Use:   "add",
	Short: "adds new bucket replication rule(-s)",
	Long: `Example:
chorctl repl add -f main -t follower -u admin -b bucket1 -b bucket2
	- will create 2 replication rules for 2 buckets`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		req := &pb.AddReplicationRequest{
			User:            raUser,
			From:            raFrom,
			To:              raTo,
			Buckets:         raBuckets,
			IsForAllBuckets: false,
		}
		if raAgentURL != "" {
			req.AgentUrl = &raAgentURL
		}

		_, err = client.AddReplication(ctx, req)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to add replication")
		}
	},
}

func init() {
	replCmd.AddCommand(addCmd)
	addCmd.Flags().StringVarP(&raFrom, "from", "f", "", "from storage")
	addCmd.Flags().StringVarP(&raTo, "to", "t", "", "to storage")
	addCmd.Flags().StringVarP(&raUser, "user", "u", "", "storage user")
	addCmd.Flags().StringVar(&raAgentURL, "agent-url", "", "notifications agent url")
	addCmd.Flags().StringArrayVarP(&raBuckets, "bucket", "b", nil, "bucket: multiple values supported: -b bucket1 -b bucket2")
	err := addCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = addCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = addCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = addCmd.MarkFlagRequired("bucket")
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
