package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func init() {
	switchCmdInit()
	switchZeroDowntimeCmdInit()
	switchScheduledCmdInit()
	switchDeleteCmdInit()
}

// get switch command:

var (
	switchCmdBucket string
	switchCmdFrom   string
	switchCmdTo     string
	switchCmdUser   string
	switchCmdWide   bool
)

func switchCmdInit() {
	replCmd.AddCommand(switchCmd)
	switchCmd.Flags().BoolVarP(&switchCmdWide, "wide", "w", false, "Wide output: shows switch status history")
	switchCmd.Flags().StringVarP(&switchCmdBucket, "bucket", "b", "", "Filter by [B]ucket")
	switchCmd.Flags().StringVarP(&switchCmdFrom, "from", "f", "", "Filter by [F]rom storage")
	switchCmd.Flags().StringVarP(&switchCmdTo, "to", "t", "", "Filter by [T]o storage")
	switchCmd.Flags().StringVarP(&switchCmdUser, "user", "u", "", "Filter by [U]ser")
}

var switchCmd = &cobra.Command{
	Use:   "switch",
	Short: "List all bucket replication switches",
	Long: `Example:
chorctl repl switch`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tlsOptions, err := getTLSOptions()
		if err != nil {
			logrus.WithError(err).Fatal("unable to get tls options")
		}
		conn, err := api.Connect(ctx, address, tlsOptions)
		if err != nil {
			logrus.WithError(err).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		resp, err := client.ListReplicationSwitches(ctx, &emptypb.Empty{})
		api.PrintGrpcError(err)

		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		fmt.Fprintln(w, api.SwitchHeader())
		for _, switchStatus := range resp.Switches {
			if !shouldPrint(switchStatus.ReplicationId) {
				continue
			}
			api.PrintSwitchRow(w, switchStatus, switchCmdWide)
		}
		w.Flush()
	},
}

func shouldPrint(in *pb.ReplicationRequest) bool {
	if switchCmdBucket != "" && in.Bucket != switchCmdBucket {
		return false
	}
	if switchCmdFrom != "" && in.From != switchCmdFrom {
		return false
	}
	if switchCmdTo != "" && in.To != switchCmdTo {
		return false
	}
	if switchCmdUser != "" && in.User != switchCmdUser {
		return false
	}
	return true
}

// create zero-downtime switch command:
var (
	switchZeroDowntimeCmdBucket       string
	switchZeroDowntimeCmdFrom         string
	switchZeroDowntimeCmdTo           string
	switchZeroDowntimeCmdUser         string
	switchZeroDowntimeCmdMultipartTTL time.Duration
)

func switchZeroDowntimeCmdInit() {
	switchCmd.AddCommand(switchZeroDowntimeCmd)
	switchZeroDowntimeCmd.Flags().StringVarP(&switchZeroDowntimeCmdBucket, "bucket", "b", "", "Bucket name of existing replication")
	switchZeroDowntimeCmd.Flags().StringVarP(&switchZeroDowntimeCmdFrom, "from", "f", "", "From storage of existing replication")
	switchZeroDowntimeCmd.Flags().StringVarP(&switchZeroDowntimeCmdTo, "to", "t", "", "To storage of existing replication")
	switchZeroDowntimeCmd.Flags().StringVarP(&switchZeroDowntimeCmdUser, "user", "u", "", "User of existing replication")
	switchZeroDowntimeCmd.Flags().DurationVar(&switchZeroDowntimeCmdMultipartTTL, "multipart-ttl", time.Hour, "Amount of time to wait for not finished multipart uploads during switch")
	if err := switchZeroDowntimeCmd.MarkFlagRequired("bucket"); err != nil {
		logrus.WithError(err).Fatal()
	}
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
chorctl repl switch zero-downtime -f main -t follower -u admin -b bucket1
  - will start zero-downtime switch for replication bucket "bucket1" from storage "main" to storage "follower"`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tlsOptions, err := getTLSOptions()
		if err != nil {
			logrus.WithError(err).Fatal("unable to get tls options")
		}
		conn, err := api.Connect(ctx, address, tlsOptions)
		if err != nil {
			logrus.WithError(err).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		req := &pb.SwitchBucketZeroDowntimeRequest{
			ReplicationId: &pb.ReplicationRequest{
				User:   switchZeroDowntimeCmdUser,
				Bucket: switchZeroDowntimeCmdBucket,
				From:   switchZeroDowntimeCmdFrom,
				To:     switchZeroDowntimeCmdTo,
			},
			MultipartTtl: durationpb.New(switchZeroDowntimeCmdMultipartTTL),
		}

		_, err = client.SwitchBucketZeroDowntime(ctx, req)
		api.PrintGrpcError(err)
	},
}

// create scheduled switch command:
var (
	switchScheduledCmdBucket string
	switchScheduledCmdFrom   string
	switchScheduledCmdTo     string
	switchScheduledCmdUser   string

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
	switchScheduledCmd.Flags().StringVarP(&switchScheduledCmdBucket, "bucket", "b", "", "Bucket name of existing replication")
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

	if err := switchScheduledCmd.MarkFlagRequired("bucket"); err != nil {
		logrus.WithError(err).Fatal()
	}
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

		tlsOptions, err := getTLSOptions()
		if err != nil {
			logrus.WithError(err).Fatal("unable to get tls options")
		}
		conn, err := api.Connect(ctx, address, tlsOptions)
		if err != nil {
			logrus.WithError(err).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		req := &pb.SwitchBucketRequest{
			ReplicationId: &pb.ReplicationRequest{
				User:   switchScheduledCmdUser,
				Bucket: switchScheduledCmdBucket,
				From:   switchScheduledCmdFrom,
				To:     switchScheduledCmdTo,
			},
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

		_, err = client.SwitchBucket(ctx, req)
		api.PrintGrpcError(err)
	},
}

// delete scheduled switch command:
var (
	switchDeleteCmdBucket string
	switchDeleteCmdFrom   string
	switchDeleteCmdTo     string
	switchDeleteCmdUser   string
)

func switchDeleteCmdInit() {
	switchCmd.AddCommand(switchDeleteCmd)
	switchDeleteCmd.Flags().StringVarP(&switchDeleteCmdBucket, "bucket", "b", "", "Bucket name of existing replication")
	switchDeleteCmd.Flags().StringVarP(&switchDeleteCmdFrom, "from", "f", "", "From storage of existing replication")
	switchDeleteCmd.Flags().StringVarP(&switchDeleteCmdTo, "to", "", "t", "To storage of existing replication")
	switchDeleteCmd.Flags().StringVarP(&switchDeleteCmdUser, "user", "u", "", "User of existing replication")

	if err := switchDeleteCmd.MarkFlagRequired("bucket"); err != nil {
		logrus.WithError(err).Fatal()
	}
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
chorctl repl switch delete -f main -t follower -u admin -b bucket1
  - will delete switch for replication bucket "bucket1" from storage "main" to storage "follower"`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tlsOptions, err := getTLSOptions()
		if err != nil {
			logrus.WithError(err).Fatal("unable to get tls options")
		}
		conn, err := api.Connect(ctx, address, tlsOptions)
		if err != nil {
			logrus.WithError(err).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		req := &pb.ReplicationRequest{
			User:   switchDeleteCmdUser,
			Bucket: switchDeleteCmdBucket,
			From:   switchDeleteCmdFrom,
			To:     switchDeleteCmdTo,
		}

		_, err = client.DeleteBucketSwitch(ctx, req)
		api.PrintGrpcError(err)
	},
}
