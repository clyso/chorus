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

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

var (
	suStorage       string
	suUser          string
	suType          string
	suAccessKey     string
	suSecretKey     string
	suSwiftUsername string
	suSwiftPassword string
	suSwiftDomain   string
	suSwiftTenant   string
)

var setUserCmd = &cobra.Command{
	Use:   "set-user",
	Short: "Set user credentials for a storage",
	Long: `Set user credentials for a storage.

For S3 storage type:
  chorctl set-user --storage main --user admin --type s3 --access-key AKID --secret-key SECRET

For Swift storage type:
  chorctl set-user --storage main --user admin --type swift --swift-username user --swift-password pass --swift-domain default --swift-tenant tenant`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if suType != "s3" && suType != "swift" {
			return fmt.Errorf("--type must be either 's3' or 'swift'")
		}
		if suType == "s3" {
			if suAccessKey == "" || suSecretKey == "" {
				return fmt.Errorf("--access-key and --secret-key are required for S3 credentials")
			}
		}
		if suType == "swift" {
			if suSwiftUsername == "" || suSwiftPassword == "" {
				return fmt.Errorf("--swift-username and --swift-password are required for Swift credentials")
			}
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		req := &pb.SetUserCredentialsRequest{
			Storage: suStorage,
			User:    suUser,
		}

		if suType == "s3" {
			req.S3Cred = &pb.S3Credential{
				AccessKey: suAccessKey,
				SecretKey: suSecretKey,
			}
		} else {
			req.SwiftCred = &pb.SwiftCredential{
				Username:   suSwiftUsername,
				Password:   suSwiftPassword,
				DomainName: suSwiftDomain,
				TenantName: suSwiftTenant,
			}
		}

		_, err = client.SetUserCredentials(ctx, req)
		if err != nil {
			api.PrintGrpcError(err)
		}
		fmt.Printf("Credentials set successfully for user %q in storage %q\n", suUser, suStorage)
	},
}

func init() {
	rootCmd.AddCommand(setUserCmd)

	setUserCmd.Flags().StringVarP(&suStorage, "storage", "s", "", "storage name")
	setUserCmd.Flags().StringVarP(&suUser, "user", "u", "", "user name")
	setUserCmd.Flags().StringVarP(&suType, "type", "t", "", "credential type: 's3' or 'swift'")
	setUserCmd.Flags().StringVar(&suAccessKey, "access-key", "", "S3 access key")
	setUserCmd.Flags().StringVar(&suSecretKey, "secret-key", "", "S3 secret key")
	setUserCmd.Flags().StringVar(&suSwiftUsername, "swift-username", "", "Swift username")
	setUserCmd.Flags().StringVar(&suSwiftPassword, "swift-password", "", "Swift password")
	setUserCmd.Flags().StringVar(&suSwiftDomain, "swift-domain", "", "Swift domain name")
	setUserCmd.Flags().StringVar(&suSwiftTenant, "swift-tenant", "", "Swift tenant name")

	if err := setUserCmd.MarkFlagRequired("storage"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := setUserCmd.MarkFlagRequired("user"); err != nil {
		logrus.WithError(err).Fatal()
	}
	if err := setUserCmd.MarkFlagRequired("type"); err != nil {
		logrus.WithError(err).Fatal()
	}
}
