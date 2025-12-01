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
	"os"
	"text/tabwriter"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

func init() {
	switchCmdInit()
	switchZeroDowntimeCmdInit()
	switchScheduledCmdInit()
	switchDeleteCmdInit()
	switchStatusCmdInit()
}

// get switch command:

var (
	switchCmdFromBucket string
	switchCmdToBucket   string
	switchCmdFrom       string
	switchCmdTo         string
	switchCmdUser       string
	switchCmdWide       bool
)

func switchCmdInit() {
	replCmd.AddCommand(switchCmd)
	switchCmd.Flags().BoolVarP(&switchCmdWide, "wide", "w", false, "wide output: shows switch status history")
	switchCmd.Flags().StringVarP(&switchCmdFromBucket, "from-bucket", "b", "", "filter by source bucket")
	switchCmd.Flags().StringVar(&switchCmdToBucket, "to-bucket", "", "filter by destination bucket")
	switchCmd.Flags().StringVarP(&switchCmdFrom, "from", "f", "", "filter by source storage")
	switchCmd.Flags().StringVarP(&switchCmdTo, "to", "t", "", "filter by destination storage")
	switchCmd.Flags().StringVarP(&switchCmdUser, "user", "u", "", "filter by replication user")
}

var switchCmd = &cobra.Command{
	Use:   "switch",
	Short: "List all bucket replication switches",
	Long: `Example:
chorctl repl switch`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).Fatal("unable to connect to api")
		}
		defer conn.Close()

		client := pb.NewPolicyClient(conn)

		True := true
		filter := &pb.ListReplicationsRequest_Filter{
			HasSwitch: &True,
		}
		if switchCmdUser != "" {
			filter.User = &switchCmdUser
		}
		if switchCmdFrom != "" {
			filter.FromStorage = &switchCmdFrom
		}
		if switchCmdTo != "" {
			filter.ToStorage = &switchCmdTo
		}
		if switchCmdFromBucket != "" {
			filter.FromBucket = &switchCmdFromBucket
		}
		if switchCmdToBucket != "" {
			filter.ToBucket = &switchCmdToBucket
		}
		resp, err := client.ListReplications(ctx, &pb.ListReplicationsRequest{
			Filter: filter,
		})
		api.PrintGrpcError(err)

		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, api.SwitchHeader())
		for _, repl := range resp.Replications {
			api.PrintSwitchRow(w, repl.SwitchInfo, switchCmdWide)
		}
		w.Flush()
	},
}

// create zero-downtime switch command:
var (
	switchZeroDowntimeCmdFromBucket   string
	switchZeroDowntimeCmdToBucket     string
	switchZeroDowntimeCmdFrom         string
	switchZeroDowntimeCmdTo           string
	switchZeroDowntimeCmdUser         string
	switchZeroDowntimeCmdMultipartTTL time.Duration
)

func switchZeroDowntimeCmdInit() {
	switchCmd.AddCommand(switchZeroDowntimeCmd)
	switchZeroDowntimeCmd.Flags().StringVarP(&switchZeroDowntimeCmdFromBucket, "from-bucket", "b", "", "source bucket name of existing bucket-level replication")
	switchZeroDowntimeCmd.Flags().StringVar(&switchZeroDowntimeCmdToBucket, "to-bucket", "", "destination bucket name of existing bucket-level replication")
	switchZeroDowntimeCmd.Flags().StringVarP(&switchZeroDowntimeCmdFrom, "from", "f", "", "From storage of existing replication")
	switchZeroDowntimeCmd.Flags().StringVarP(&switchZeroDowntimeCmdTo, "to", "t", "", "To storage of existing replication")
	switchZeroDowntimeCmd.Flags().StringVarP(&switchZeroDowntimeCmdUser, "user", "u", "", "User of existing replication")
	switchZeroDowntimeCmd.Flags().DurationVar(&switchZeroDowntimeCmdMultipartTTL, "multipart-ttl", time.Hour, "Amount of time to wait for not finished multipart uploads during switch")
	if err := switchZeroDowntimeCmd.MarkFlagRequired("from"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := switchZeroDowntimeCmd.MarkFlagRequired("to"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := switchZeroDowntimeCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
}

var switchZeroDowntimeCmd = &cobra.Command{
	Use:   "zero-downtime",
	Short: "Switch bucket replication with zero downtime",
	Long: `Example:

User-level zero-downtime replication switch:
  chorctl repl switch zero-downtime -f main -t follower -u admin

Bucket-level zero-downtime replication switch:
  chorctl repl switch zero-downtime -f main -t follower -u admin -b bucket1`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		id := buildReplicationID(switchZeroDowntimeCmdUser, switchZeroDowntimeCmdFrom, switchZeroDowntimeCmdTo, switchZeroDowntimeCmdFromBucket, switchZeroDowntimeCmdToBucket)
		req := &pb.SwitchZeroDowntimeRequest{
			ReplicationId: id,
			MultipartTtl:  durationpb.New(switchZeroDowntimeCmdMultipartTTL),
		}

		_, err := client.SwitchWithZeroDowntime(ctx, req)
		api.PrintGrpcError(err)
	},
}

// create scheduled switch command:
var (
	switchScheduledCmdFromBucket string
	switchScheduledCmdToBucket   string
	switchScheduledCmdFrom       string
	switchScheduledCmdTo         string
	switchScheduledCmdUser       string

	switchScheduledCmdStartOnInitDone     bool
	switchScheduledCmdCron                string
	switchScheduledCmdStartAtStr          string
	switchScheduledCmdMaxDuration         time.Duration
	switchScheduledCmdMaxEventLag         uint32
	switchScheduledCmdSkipBucketCheck     bool
	switchScheduledCmdContinueReplication bool
)

func switchScheduledCmdInit() {
	switchCmd.AddCommand(switchScheduledCmd)
	switchScheduledCmd.Flags().StringVarP(&switchScheduledCmdFromBucket, "from-bucket", "b", "", "source bucket name of existing bucket-level replication")
	switchScheduledCmd.Flags().StringVar(&switchScheduledCmdToBucket, "to-bucket", "", "destination bucket name of existing bucket-level replication")
	switchScheduledCmd.Flags().StringVarP(&switchScheduledCmdFrom, "from", "f", "", "From storage of existing replication")
	switchScheduledCmd.Flags().StringVarP(&switchScheduledCmdTo, "to", "t", "", "To storage of existing replication")
	switchScheduledCmd.Flags().StringVarP(&switchScheduledCmdUser, "user", "u", "", "User of existing replication")

	switchScheduledCmd.Flags().BoolVar(&switchScheduledCmdStartOnInitDone, "start-on-init-done", false, "Start switch when initialization is done")
	switchScheduledCmd.Flags().StringVar(&switchScheduledCmdCron, "cron", "", "Cron expression for switch start")
	switchScheduledCmd.Flags().StringVar(&switchScheduledCmdStartAtStr, "start-at", "", "Time to start switch in RFC3339: 2006-01-02T15:04:05Z07:00")
	switchScheduledCmd.Flags().DurationVar(&switchScheduledCmdMaxDuration, "max-duration", time.Hour, "Maximum duration of switch")
	switchScheduledCmd.Flags().Uint32Var(&switchScheduledCmdMaxEventLag, "max-event-lag", 0, "Maximum allowed replication event lag before starting switch")
	switchScheduledCmd.Flags().BoolVar(&switchScheduledCmdSkipBucketCheck, "skip-bucket-check", false, "Skip bucket check before completing switch")
	switchScheduledCmd.Flags().BoolVar(&switchScheduledCmdContinueReplication, "continue-replication", false, "Continue replication backwards after switch")

	if err := switchScheduledCmd.MarkFlagRequired("from"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := switchScheduledCmd.MarkFlagRequired("to"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := switchScheduledCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
}

var switchScheduledCmd = &cobra.Command{
	Use:   "downtime",
	Short: "Create or update a scheduled switch with downtime window for bucket replication",
	Long: `Example-1:
chorctl repl switch scheduled -f main -t follower -u admin -b bucket1 \ 
	--start-on-init-done

  - will schedule downtime switch for replication bucket "bucket1" from storage "main"
    to storage "follower" as soon as initial replication is done

Example-2:
chorctl repl switch scheduled -f main -t follower -u admin -b bucket1 \
	--start-at="2021-01-01T03:00:00Z" --max-duration=2h --max-event-lag=100

  - will schedule downtime switch for replication bucket "bucket1" from storage "main"
    to storage "follower". Downtime will be attempted once at 3:00 AM on 1st January 2021,
    with maximum duration of 2 hours. And will start only if replication event lag
    will be less than 100

Example-3:
chorctl repl switch scheduled -f main -t follower -u admin -b bucket1 \
	--cron="0 3 * * *" --max-duration=2h --max-event-lag=100

  - will schedule downtime switch for replication bucket "bucket1" from storage "main"
    to storage "follower". Downtime will be attempted every day at 3:00 AM,
    with maximum duration of 2 hours. And will start only if replication event lag
    will be less than 100`,

	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		id := buildReplicationID(switchScheduledCmdUser, switchScheduledCmdFrom, switchScheduledCmdTo, switchScheduledCmdFromBucket, switchScheduledCmdToBucket)
		req := &pb.SwitchDowntimeRequest{
			ReplicationId: id,
			DowntimeOpts: &pb.SwitchDowntimeOpts{
				StartOnInitDone:     switchScheduledCmdStartOnInitDone,
				SkipBucketCheck:     switchScheduledCmdSkipBucketCheck,
				ContinueReplication: switchScheduledCmdContinueReplication,
			},
		}
		if cmd.Flags().Lookup("cron").Changed {
			req.DowntimeOpts.Cron = &switchScheduledCmdCron
		}
		if cmd.Flags().Lookup("start-at").Changed {
			startAt, err := time.Parse(time.RFC3339, switchScheduledCmdStartAtStr)
			if err != nil {
				logrus.WithError(err).Fatal("failed to parse start-at time")
			}
			req.DowntimeOpts.StartAt = timestamppb.New(startAt)
		}
		if cmd.Flags().Lookup("max-duration").Changed {
			req.DowntimeOpts.MaxDuration = durationpb.New(switchScheduledCmdMaxDuration)
		}
		if cmd.Flags().Lookup("max-event-lag").Changed {
			req.DowntimeOpts.MaxEventLag = &switchScheduledCmdMaxEventLag
		}

		_, err := client.SwitchWithDowntime(ctx, req)
		api.PrintGrpcError(err)
	},
}

// delete scheduled switch command:
var (
	switchDeleteCmdFromBucket string
	switchDeleteCmdToBucket   string
	switchDeleteCmdFrom       string
	switchDeleteCmdTo         string
	switchDeleteCmdUser       string
)

func switchDeleteCmdInit() {
	switchCmd.AddCommand(switchDeleteCmd)
	switchDeleteCmd.Flags().StringVarP(&switchDeleteCmdFromBucket, "from-bucket", "b", "", "source bucket name of existing bucket-level replication")
	switchDeleteCmd.Flags().StringVar(&switchDeleteCmdToBucket, "to-bucket", "", "destination bucket name of existing bucket-level replication")
	switchDeleteCmd.Flags().StringVarP(&switchDeleteCmdFrom, "from", "f", "", "From storage of existing replication")
	switchDeleteCmd.Flags().StringVarP(&switchDeleteCmdTo, "to", "t", "", "To storage of existing replication")
	switchDeleteCmd.Flags().StringVarP(&switchDeleteCmdUser, "user", "u", "", "User of existing replication")

	if err := switchDeleteCmd.MarkFlagRequired("from"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := switchDeleteCmd.MarkFlagRequired("to"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := switchDeleteCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
}

var switchDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a bucket replication switch",
	Long: `Example:
chorctl repl switch delete -f main -t follower -u admin --from-bucket bucket1
  - will delete switch for replication bucket "bucket1" from storage "main" to storage "follower"`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		id := buildReplicationID(switchDeleteCmdUser, switchDeleteCmdFrom, switchDeleteCmdTo, switchDeleteCmdFromBucket, switchDeleteCmdToBucket)

		_, err := client.DeleteSwitch(ctx, id)
		api.PrintGrpcError(err)
	},
}

// switch status command:
var (
	switchStatusFromBucket string
	switchStatusToBucket   string
	switchStatusFrom       string
	switchStatusTo         string
	switchStatusUser       string
)

// switchStatusCmdInit wires the `status` subcommand that calls GetSwitchStatus.
func switchStatusCmdInit() {
	switchCmd.AddCommand(switchStatusCmd)
	switchStatusCmd.Flags().StringVarP(&switchStatusFromBucket, "from-bucket", "b", "", "source bucket name of existing bucket-level replication")
	switchStatusCmd.Flags().StringVar(&switchStatusToBucket, "to-bucket", "", "destination bucket name of existing bucket-level replication")
	switchStatusCmd.Flags().StringVarP(&switchStatusFrom, "from", "f", "", "From storage of existing replication")
	switchStatusCmd.Flags().StringVarP(&switchStatusTo, "to", "t", "", "To storage of existing replication")
	switchStatusCmd.Flags().StringVarP(&switchStatusUser, "user", "u", "", "User of existing replication")

	if err := switchStatusCmd.MarkFlagRequired("from"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := switchStatusCmd.MarkFlagRequired("to"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := switchStatusCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
}

var switchStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Get status of a bucket replication switch",
	Long: `Example:
chorctl repl switch status -f main -t follower -u admin --from-bucket bucket1
  - will print status for the switch configured for bucket "bucket1" from storage "main" to storage "follower"`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		id := buildReplicationID(switchStatusUser, switchStatusFrom, switchStatusTo, switchStatusFromBucket, switchStatusToBucket)
		res, err := client.GetSwitchStatus(ctx, id)
		if err != nil {
			api.PrintGrpcError(err)
		}

		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, api.SwitchHeader())
		api.PrintSwitchRow(w, res, true)
		w.Flush()
	},
}
