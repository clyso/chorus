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
	rrFrom   string
	rrTo     string
	rrUser   string
	rrBucket string
)

// resumeCmd represents the pause command
var resumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "resumes bucket replication rule",
	Long: `Example:
chorctl repl resume -f main -t follower -u admin -b bucket1`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		_, err = client.ResumeReplication(ctx, &pb.ReplicationRequest{
			User:   rrUser,
			Bucket: rrBucket,
			From:   rrFrom,
			To:     rrTo,
		})
		if err != nil {
			logrus.WithError(err).Fatal("unable to add replication")
		}
	},
}

func init() {
	replCmd.AddCommand(resumeCmd)
	resumeCmd.Flags().StringVarP(&rrFrom, "from", "f", "", "from storage")
	resumeCmd.Flags().StringVarP(&rrTo, "to", "t", "", "to storage")
	resumeCmd.Flags().StringVarP(&rrUser, "user", "u", "", "storage user")
	resumeCmd.Flags().StringVarP(&rrBucket, "bucket", "b", "", "bucket name")
	err := resumeCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = resumeCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = resumeCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = resumeCmd.MarkFlagRequired("bucket")
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
