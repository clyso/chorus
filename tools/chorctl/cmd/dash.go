package cmd

import (
	"context"
	"fmt"
	tea "github.com/charmbracelet/bubbletea"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"github.com/clyso/chorus/tools/chorctl/internal/ui"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// dashCmd represents the dash command
var dashCmd = &cobra.Command{
	Use:   "dash",
	Short: "Open migration interactive dashboard",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)
		model := ui.New(ctx, client)
		p := tea.NewProgram(model, tea.WithAltScreen())
		if _, err := p.Run(); err != nil {
			fmt.Println("could not start program:", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(dashCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dashCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dashCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
