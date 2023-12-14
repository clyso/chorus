package cmd

import (
	"context"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var (
	ruaFrom string
	ruaTo   string
	ruaUser string
)

// addUserCmd represents the addUser command
var addUserCmd = &cobra.Command{
	Use:   "add-user",
	Short: "add user-level replication rule",
	Long: `Example:
chorctl repl add-user -f main -t follower -u admin 
	- will create user replication rule for all existing and future buckets`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		_, err = client.AddReplication(ctx, &pb.AddReplicationRequest{
			User:            ruaUser,
			From:            ruaFrom,
			To:              ruaTo,
			Buckets:         nil,
			IsForAllBuckets: true,
		})
		if err != nil {
			logrus.WithError(err).Fatal("unable to add replication")
		}
	},
}

func init() {
	replCmd.AddCommand(addUserCmd)
	addUserCmd.Flags().StringVarP(&ruaFrom, "from", "f", "", "from storage")
	addUserCmd.Flags().StringVarP(&ruaTo, "to", "t", "", "to storage")
	addUserCmd.Flags().StringVarP(&ruaUser, "user", "u", "", "storage user")
	err := addUserCmd.MarkFlagRequired("from")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = addUserCmd.MarkFlagRequired("to")
	if err != nil {
		logrus.WithError(err).Fatal()
	}
	err = addUserCmd.MarkFlagRequired("user")
	if err != nil {
		logrus.WithError(err).Fatal()
	}

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addUserCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// addUserCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
