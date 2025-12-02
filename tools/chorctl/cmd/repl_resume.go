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
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

var (
	rrFrom       string
	rrTo         string
	rrUser       string
	rrFromBucket string
	rrToBucket   string
)

// resumeCmd represents the resume command
var resumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "resume a replication policy",
	Long: `Resume a replication policy.

User-level policy:
  chorctl repl resume --from main --to follower --user admin

Bucket-level policy:
  chorctl repl resume --from main --to follower --user admin --from-bucket bucket1`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if rrToBucket != "" && rrFromBucket == "" {
			return fmt.Errorf("--to-bucket must be set when --from-bucket is set")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		id := buildReplicationID(rrUser, rrFrom, rrTo, rrFromBucket, rrToBucket)
		_, err := client.ResumeReplication(ctx, id)
		if err != nil {
			api.PrintGrpcError(err)
		}
	},
}

func init() {
	replCmd.AddCommand(resumeCmd)
	resumeCmd.Flags().StringVarP(&rrFrom, "from", "f", "", "source storage name")
	resumeCmd.Flags().StringVarP(&rrTo, "to", "t", "", "destination storage name")
	resumeCmd.Flags().StringVarP(&rrUser, "user", "u", "", "replication user")
	resumeCmd.Flags().StringVarP(&rrFromBucket, "from-bucket", "b", "", "source bucket name; omit for user-level policies")
	resumeCmd.Flags().StringVar(&rrToBucket, "to-bucket", "", "destination bucket name; defaults to from-bucket for bucket-level policies")

	if err := resumeCmd.MarkFlagRequired("from"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := resumeCmd.MarkFlagRequired("to"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := resumeCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
}
