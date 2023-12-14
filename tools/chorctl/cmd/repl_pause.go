package cmd

import (
	"context"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var (
	rpFrom   string
	rpTo     string
	rpUser   string
	rpBucket string
)

// pauseCmd represents the pause command
var pauseCmd = &cobra.Command{
	Use:   "pause",
	Short: "pauses bucket replication rule",
	Long: `Example:
chorctl repl pause -f main -t follower -u admin -b bucket1`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		_, err = client.PauseReplication(ctx, &pb.ReplicationRequest{
			User:   rpUser,
			Bucket: rpBucket,
			From:   rpFrom,
			To:     rpTo,
		})
		if err != nil {
			logrus.WithError(err).Fatal("unable to add replication")
		}
	},
}

func init() {
	replCmd.AddCommand(pauseCmd)
	pauseCmd.Flags().StringVarP(&rpFrom, "from", "f", "", "from storage")
	pauseCmd.Flags().StringVarP(&rpTo, "to", "t", "", "to storage")
	pauseCmd.Flags().StringVarP(&rpUser, "user", "u", "", "storage user")
	pauseCmd.Flags().StringVarP(&rpBucket, "bucket", "b", "", "bucket name")
	err := pauseCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = pauseCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = pauseCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = pauseCmd.MarkFlagRequired("bucket")
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
