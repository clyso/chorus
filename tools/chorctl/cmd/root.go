/*
 * Copyright © 2023 Clyso GmbH
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
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"strings"
)

var cfgFile string
var (
	// flags:
	address = ""
	user    = ""
)

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
}
