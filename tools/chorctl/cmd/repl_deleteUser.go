package cmd

import (
	"context"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var (
	rduFrom string
	rduTo   string
	rduUser string
)

// deleteUserCmd represents the deleteUser command
var deleteUserCmd = &cobra.Command{
	Use:   "delete-user",
	Short: "deletes user replication rule",
	Long: `Example:
chorctl repl delete -f main -t follower -u admin`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		_, err = client.DeleteUserReplication(ctx, &pb.DeleteUserReplicationRequest{
			User: rduUser,
			From: rduFrom,
			To:   rduTo,
		})
		if err != nil {
			logrus.WithError(err).Fatal("unable to delete replication")
		}
	},
}

func init() {
	replCmd.AddCommand(deleteUserCmd)
	deleteUserCmd.Flags().StringVarP(&rduFrom, "from", "f", "", "from storage")
	deleteUserCmd.Flags().StringVarP(&rduTo, "to", "t", "", "to storage")
	deleteUserCmd.Flags().StringVarP(&rduUser, "user", "u", "", "storage user")
	err := deleteUserCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = deleteUserCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = deleteUserCmd.MarkFlagRequired("user")
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
