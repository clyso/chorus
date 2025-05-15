/*
 * Copyright © 2023 Clyso GmbH
 * Copyright © 2025 Strato GmbH
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

	"github.com/sirupsen/logrus"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"

	"github.com/spf13/cobra"
)

var (
	ruaFrom string
	ruaTo   string
	ruaUser string
)

// addUserCmd represents the addUser command
var addUserCmd = &cobra.Command{
	Use:   "add-user",
	Short: "add user-level replication rule",
	Long: `Example:
chorctl repl add-user -f main -t follower -u admin 
	- will create user replication rule for all existing and future buckets`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tlsOptions, err := getTLSOptions()
		if err != nil {
			logrus.WithError(err).Fatal("unable to get tls options")
		}

		conn, err := api.Connect(ctx, address, tlsOptions)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		_, err = client.AddReplication(ctx, &pb.AddReplicationRequest{
			User:            ruaUser,
			From:            ruaFrom,
			To:              ruaTo,
			Buckets:         nil,
			IsForAllBuckets: true,
		})
		if err != nil {
			logrus.WithError(err).Fatal("unable to add replication")
		}
	},
}

func init() {
	replCmd.AddCommand(addUserCmd)
	addUserCmd.Flags().StringVarP(&ruaFrom, "from", "f", "", "from storage")
	addUserCmd.Flags().StringVarP(&ruaTo, "to", "t", "", "to storage")
	addUserCmd.Flags().StringVarP(&ruaUser, "user", "u", "", "storage user")
	err := addUserCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = addUserCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = addUserCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addUserCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// addUserCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
