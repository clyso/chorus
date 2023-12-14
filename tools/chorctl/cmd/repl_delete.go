package cmd

import (
	"context"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var (
	rdFrom   string
	rdTo     string
	rdUser   string
	rdBucket string
)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "deletes bucket replication rule",
	Long: `Example:
chorctl repl delete -f main -t follower -u admin -b bucket1`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		_, err = client.DeleteReplication(ctx, &pb.ReplicationRequest{
			User:   rdUser,
			Bucket: rdBucket,
			From:   rdFrom,
			To:     rdTo,
		})
		if err != nil {
			logrus.WithError(err).Fatal("unable to add replication")
		}
	},
}

func init() {
	replCmd.AddCommand(deleteCmd)
	deleteCmd.Flags().StringVarP(&rdFrom, "from", "f", "", "from storage")
	deleteCmd.Flags().StringVarP(&rdTo, "to", "t", "", "to storage")
	deleteCmd.Flags().StringVarP(&rdUser, "user", "u", "", "storage user")
	deleteCmd.Flags().StringVarP(&rdBucket, "bucket", "b", "", "bucket name")
	err := deleteCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = deleteCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = deleteCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = deleteCmd.MarkFlagRequired("bucket")
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
