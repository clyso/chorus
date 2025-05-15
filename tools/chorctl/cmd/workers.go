package cmd

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

var workersCmd = &cobra.Command{
	Use:     "worker",
	Aliases: []string{"workers"},
	Short:   "Prints information about worker",
	Long:    ``,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tlsOption, err := getTLSOptions()
		if err != nil {
			logrus.WithError(err).Fatal("unable to get tls options")
		}
		conn, err := api.Connect(ctx, address, tlsOption)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		appVersion, err := client.GetAppVersion(ctx, &emptypb.Empty{})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get app version")
		}

		fmt.Printf("%q (Built on %q from Git SHA %q)", appVersion.Version, appVersion.Date, appVersion.Commit)
	},
}

func init() {
	rootCmd.AddCommand(workersCmd)
}
