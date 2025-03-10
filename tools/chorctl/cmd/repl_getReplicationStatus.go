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
	"google.golang.org/protobuf/types/known/timestamppb"
)

var getReplicationStatusCmd = &cobra.Command{
	Use:   "get",
	Short: "Get replication status",
	Long: `Example:
chorctl repl get -f main -t follower -u admin -b bucket1`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		req := &pb.ReplicationRequest{
			User:   rdUser,
			Bucket: rdBucket,
			From:   rdFrom,
			To:     rdTo,
		}
		if rpToBucket != "" {
			req.ToBucket = &rpToBucket
		}
		res, err := client.FetchReplicationStatus(ctx, req)
		if err != nil {
			logrus.WithError(err).Fatal("unable to add replication")
		}

		// io.Writer, minwidth, tabwidth, padding int, padchar byte, flags uint
		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)

		fmt.Fprintln(w, "USER\tBUCKET\tFROM\tTO\tCREATED AT\tPAUSED\tINIT DONE\tEVENTS\tLAST EMITTED AT\tLAST PROCESSED AT\tAGENT URL\tSWITCH STATUS\tTO BUCKET\tINIT DONE AT")

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%t\t%t\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			res.User,
			res.Bucket,
			res.From,
			res.To,
			formatTimestamp(res.CreatedAt),
			res.IsPaused,
			res.IsInitDone,
			res.Events,
			formatTimestamp(res.LastEmittedAt),
			formatTimestamp(res.LastProcessedAt),
			derefString(res.AgentUrl),
			res.SwitchStatus.String(),
			derefString(res.ToBucket),
			formatTimestamp(res.InitDoneAt),
		)
		w.Flush()
	},
}

func formatTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return "-"
	}
	return ts.AsTime().Format(time.RFC3339)
}

func derefString(s *string) string {
	if s == nil {
		return "-"
	}
	return *s
}

func init() {
	replCmd.AddCommand(getReplicationStatusCmd)
	getReplicationStatusCmd.Flags().StringVarP(&rpFrom, "from", "f", "", "from storage")
	getReplicationStatusCmd.Flags().StringVarP(&rpTo, "to", "t", "", "to storage")
	getReplicationStatusCmd.Flags().StringVarP(&rpUser, "user", "u", "", "storage user")
	getReplicationStatusCmd.Flags().StringVarP(&rpBucket, "bucket", "b", "", "bucket name")
	err := getReplicationStatusCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = getReplicationStatusCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = getReplicationStatusCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = getReplicationStatusCmd.MarkFlagRequired("bucket")
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
