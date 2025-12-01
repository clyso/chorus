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
	rdFrom       string
	rdTo         string
	rdUser       string
	rdFromBucket string
	rdToBucket   string
)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete a replication policy",
	Long: `Delete a replication policy.

User-level policy:
  chorctl repl delete --from main --to follower --user admin

Bucket-level policy:
  chorctl repl delete --from main --to follower --user admin --from-bucket bucket1`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if rdFromBucket != "" && rdToBucket == "" {
			return fmt.Errorf("--to-bucket must be set when --from-bucket is set")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		id := buildReplicationID(rdUser, rdFrom, rdTo, rdFromBucket, rdToBucket)
		_, err := client.DeleteReplication(ctx, id)
		if err != nil {
			api.PrintGrpcError(err)
		}
	},
}

func init() {
	replCmd.AddCommand(deleteCmd)
	deleteCmd.Flags().StringVarP(&rdFrom, "from", "f", "", "source storage name")
	deleteCmd.Flags().StringVarP(&rdTo, "to", "t", "", "destination storage name")
	deleteCmd.Flags().StringVarP(&rdUser, "user", "u", "", "replication user")
	deleteCmd.Flags().StringVarP(&rdFromBucket, "from-bucket", "b", "", "source bucket name; omit for user-level policies")
	deleteCmd.Flags().StringVar(&rdToBucket, "to-bucket", "", "destination bucket name; defaults to from-bucket for bucket-level policies")

	if err := deleteCmd.MarkFlagRequired("from"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := deleteCmd.MarkFlagRequired("to"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := deleteCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
}
