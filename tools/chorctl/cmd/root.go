/*
 * Copyright © 2023 Clyso GmbH
 * Copyright © 2025 Strato GmbH
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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var cfgFile string
var (
	// flags:
	address             = ""
	user                = ""
	useTLS              = false
	tlsSkipVerify       = false
	tlsAdditionalRootCA = ""
	tlsClientCert       = ""
	tlsClientKey        = ""
)

func getTLSOptions() (grpc.DialOption, error) {
	if !useTLS {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}

	certPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("failed to get system cert pool: %v", err)
	}
	if tlsAdditionalRootCA != "" {
		ca, err := os.ReadFile(tlsAdditionalRootCA)
		if err != nil {
			return nil, fmt.Errorf("failed to read additional root CA: %v", err)
		}
		ok := certPool.AppendCertsFromPEM(ca)
		if !ok {
			return nil, fmt.Errorf("failed to append additional root CA")
		}
	}

	var certificate tls.Certificate
	if tlsClientCert != "" || tlsClientKey != "" {
		if tlsClientCert == "" || tlsClientKey == "" {
			return nil, fmt.Errorf("both --tls-client-cert and --tls-client-key must be set")
		}
		certificate, err = tls.LoadX509KeyPair(tlsClientCert, tlsClientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %v", err)
		}
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs:            certPool,
		Certificates:       []tls.Certificate{certificate},
		InsecureSkipVerify: tlsSkipVerify,
	})), nil
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "chorctl",
	Short: "Manages and monitors s3 migrations",
	Long: `Chorctl is a CLI tool to monitor and manage chorus application
performing live migrations and replication of s3 storages.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func SetVersionInfo(version, commit, date string) {
	rootCmd.Version = fmt.Sprintf("%q (Built on %q from Git SHA %q)", version, date, commit)
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.chorctl.yaml)")

	rootCmd.PersistentFlags().StringVarP(&address, "address", "a", "localhost:9670", "address to chorus management grpc api (default: http://localhost:9670)")
	rootCmd.PersistentFlags().StringVarP(&user, "user", "u", "", "storage user")
	_ = viper.BindPFlag("address", rootCmd.PersistentFlags().Lookup("address"))
	_ = viper.BindPFlag("user", rootCmd.PersistentFlags().Lookup("user"))

	verbose := rootCmd.PersistentFlags().BoolP("verbose", "v", false, "prints additional log information")
	if verbose != nil && *verbose {
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetLevel(logrus.WarnLevel)
	}

	rootCmd.PersistentFlags().BoolVar(&useTLS, "tls", false, "use TLS for connection")
	rootCmd.PersistentFlags().BoolVar(&tlsSkipVerify, "tls-skip-verify", false, "skip TLS certificate verification")
	rootCmd.PersistentFlags().StringVar(&tlsAdditionalRootCA, "tls-root-ca", "", "additional root CA certificate")
	rootCmd.PersistentFlags().StringVar(&tlsClientCert, "tls-client-cert", "", "client certificate")
	rootCmd.PersistentFlags().StringVar(&tlsClientKey, "tls-client-key", "", "client key")
	_ = viper.BindPFlag("tls", rootCmd.PersistentFlags().Lookup("tls"))
	_ = viper.BindPFlag("tls-skip-verify", rootCmd.PersistentFlags().Lookup("tls-skip-verify"))
	_ = viper.BindPFlag("tls-root-ca", rootCmd.PersistentFlags().Lookup("tls-root-ca"))
	_ = viper.BindPFlag("tls-client-cert", rootCmd.PersistentFlags().Lookup("tls-client-cert"))
	_ = viper.BindPFlag("tls-client-key", rootCmd.PersistentFlags().Lookup("tls-client-key"))

}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".chorctl" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".chorctl")
	}
	viper.EnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("CHORUS")
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		logrus.Info("Using config file:", viper.ConfigFileUsed())
	}
	address = viper.GetString("address")
	useTLS = viper.GetBool("tls")
	tlsSkipVerify = viper.GetBool("tls-skip-verify")
	tlsAdditionalRootCA = viper.GetString("tls-root-ca")
	tlsClientCert = viper.GetString("tls-client-cert")
	tlsClientKey = viper.GetString("tls-client-key")
}
