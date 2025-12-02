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
	rpFrom       string
	rpTo         string
	rpUser       string
	rpFromBucket string
	rpToBucket   string
)

// pauseCmd represents the pause command
var pauseCmd = &cobra.Command{
	Use:   "pause",
	Short: "pause a replication policy",
	Long: `Pause a replication policy.

User-level policy:
  chorctl repl pause --from main --to follower --user admin

Bucket-level policy:
  chorctl repl pause --from main --to follower --user admin --from-bucket bucket1`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if rpToBucket != "" && rpFromBucket == "" {
			return fmt.Errorf("--to-bucket must be set when --from-bucket is set")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		id := buildReplicationID(rpUser, rpFrom, rpTo, rpFromBucket, rpToBucket)
		_, err := client.PauseReplication(ctx, id)
		if err != nil {
			api.PrintGrpcError(err)
		}
	},
}

func init() {
	replCmd.AddCommand(pauseCmd)
	pauseCmd.Flags().StringVarP(&rpFrom, "from", "f", "", "source storage name")
	pauseCmd.Flags().StringVarP(&rpTo, "to", "t", "", "destination storage name")
	pauseCmd.Flags().StringVarP(&rpUser, "user", "u", "", "replication user")
	pauseCmd.Flags().StringVarP(&rpFromBucket, "from-bucket", "b", "", "source bucket name; omit for user-level policies")
	pauseCmd.Flags().StringVar(&rpToBucket, "to-bucket", "", "destination bucket name; defaults to from-bucket for bucket-level policies")

	if err := pauseCmd.MarkFlagRequired("from"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := pauseCmd.MarkFlagRequired("to"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := pauseCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
}
