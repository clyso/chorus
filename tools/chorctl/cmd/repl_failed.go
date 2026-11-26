/*
 * Copyright © 2024 Clyso GmbH
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
	"github.com/spf13/cobra"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

var (
	rfFrom       string
	rfTo         string
	rfUser       string
	rfFromBucket string
	rfToBucket   string
	rfLimit      int32
)

// failedCmd lists the objects a replication could not copy.
var failedCmd = &cobra.Command{
	Use:   "failed",
	Short: "list the objects a replication failed to copy, with the last error",
	Long: `List the replication tasks that failed at least once: the objects still
being retried (with the time of the next attempt) and the ones given up on
(archived: retries exhausted, or a permanent error such as the destination
rejecting the object). Archived tasks keep the replication below 100 %.

  chorctl repl failed --from main --to follower --user admin --from-bucket bucket1`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if rfToBucket != "" && rfFromBucket == "" {
			return fmt.Errorf("--to-bucket must be set when --from-bucket is set")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		id := buildReplicationID(rfUser, rfFrom, rfTo, rfFromBucket, rfToBucket)
		res, err := client.ListReplicationFailures(ctx, &pb.ListReplicationFailuresRequest{Id: id, Limit: rfLimit})
		if err != nil {
			api.PrintGrpcError(err)
			return
		}
		if len(res.Failures) == 0 {
			fmt.Println("no failed tasks")
			return
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', 0)
		fmt.Fprintln(w, "QUEUE\tSTATE\tOBJECT\tRETRIED\tLAST_FAILED\tNEXT_RETRY\tERROR")
		for _, f := range res.Failures {
			state := "retrying"
			next := "-"
			if f.Archived {
				state = "given up"
			} else if f.NextRetryAt != nil {
				next = api.DurationToStr(f.NextRetryAt.AsTime().Sub(f.LastFailedAt.AsTime())) + " after last"
			}
			obj := f.Object
			if obj == "" {
				obj = "(" + f.TaskType + ")"
			} else if f.VersionId != "" {
				obj += "@" + f.VersionId
			}
			errText := strings.ReplaceAll(f.LastError, "\n", " ")
			if i := strings.Index(errText, ": skip retry"); i > 0 {
				errText = errText[:i] // asynq's SkipRetry marker, not part of the storage's answer
			}
			if len(errText) > 120 {
				errText = errText[:117] + "..."
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%d/%d\t%s\t%s\t%s\n",
				f.Queue, state, obj, f.Retried, f.MaxRetry, api.DateToAge(f.LastFailedAt)+" ago", next, errText)
		}
		w.Flush()
	},
}

func init() {
	replCmd.AddCommand(failedCmd)
	failedCmd.Flags().StringVarP(&rfFrom, "from", "f", "", "source storage name")
	failedCmd.Flags().StringVarP(&rfTo, "to", "t", "", "destination storage name")
	failedCmd.Flags().StringVarP(&rfUser, "user", "u", "", "replication user")
	failedCmd.Flags().StringVarP(&rfFromBucket, "from-bucket", "b", "", "source bucket name; omit for user-level policies")
	failedCmd.Flags().StringVar(&rfToBucket, "to-bucket", "", "destination bucket name; defaults to from-bucket for bucket-level policies")
	failedCmd.Flags().Int32Var(&rfLimit, "limit", 100, "maximum entries per queue and state")

	for _, flag := range []string{"from", "to", "user"} {
		if err := failedCmd.MarkFlagRequired(flag); err != nil {
			logrus.WithError(err).Fatal()
		}
	}
}
