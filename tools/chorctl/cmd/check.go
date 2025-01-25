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
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"

	"github.com/spf13/cobra"
)

var (
	checkBucket string
)

// checkCmd represents the check command
var checkCmd = &cobra.Command{
	Use:   "check <from_storage> <to_storage> --bucket=<bucket_name>",
	Short: "Checks the files in the source and destination match.",
	Long:  ``,
	Args:  cobra.MatchAll(cobra.ExactArgs(2), cobra.OnlyValidArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		storages, err := client.GetStorages(ctx, &emptypb.Empty{})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get storages to api")
		}
		var res []string
		for _, storage := range storages.Storages {
			res = append(res, storage.Name)
		}
		return res, cobra.ShellCompDirectiveDefault
	},
	Run: func(cmd *cobra.Command, args []string) {
		var from, to = args[0], args[1]
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := api.Connect(ctx, address)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)
		if checkBucket != "" {
			fmt.Println("Checking files in bucket", checkBucket, "...")
			max := 6
			if len(checkBucket) > max {
				max = len(checkBucket)
			}
			fmt.Printf("🪣 %s | Match\t | MissSrc\t | MissDst\t | Differ\t | Error\n", fillName("BUCKET", max, " "))
			check(ctx, client, from, to, checkBucket, max)
			return
		}

		fmt.Println("Getting list of buckets...")

		m, err := client.ListReplications(ctx, &emptypb.Empty{})
		if err != nil {
			logrus.WithError(err).Fatal("unable to get migration")
		}
		var buckets []string
		for _, repl := range m.Replications {
			if repl.From != from || repl.To != to {
				continue
			}
			buckets = append(buckets, repl.Bucket)
		}
		if len(buckets) == 0 {
			logrus.Fatal("unable to get buckets list: no migration")
		}
		fmt.Println("Start check for", len(buckets), "buckets...")
		max := 6
		for _, b := range buckets {
			if len(b) > max {
				max = len(b)
			}
		}

		fmt.Printf("🪣 %s | Match\t | MissSrc\t | MissDst\t | Differ \t | Error\t|\n", fillName("BUCKET", max, " "))
		var wg sync.WaitGroup
		wg.Add(len(buckets))
		for i := range buckets {
			go func(bucket string) {
				defer wg.Done()
				check(ctx, client, from, to, bucket, max)
			}(buckets[i])
		}
		wg.Wait()
	},
}

func check(ctx context.Context, client pb.ChorusClient, from, to, bucket string, max int) {
	res, err := client.CompareBucket(ctx, &pb.CompareBucketRequest{
		Bucket:    bucket,
		From:      from,
		To:        to,
		ShowMatch: true,
		User:      user,
	})
	if err != nil {
		logrus.WithError(err).Fatal("unable to check bucket")
	}
	if res.IsMatch {
		fmt.Printf("✅ %s | %-7d\t | %-7d\t | %-7d\t | %-7d\t | %-7d\n", fillName(bucket, max, "."),
			len(res.Match),
			len(res.MissFrom),
			len(res.MissTo),
			len(res.Differ),
			len(res.Error),
		)
	} else {
		fmt.Println("\033[31m" + fmt.Sprintf("❌ %s | %-7d\t | %-7d\t | %-7d\t | %-7d\t | %-7d", fillName(bucket, max, "."),
			len(res.Match),
			len(res.MissFrom),
			len(res.MissTo),
			len(res.Differ),
			len(res.Error),
		) + "\033[0m")
	}
}

func fillName(in string, size int, fill string) string {
	//if len(in) == size {
	//	return in
	//}
	for i := len(in); i < size; i++ {
		in += fill
	}
	return in
}

func init() {
	rootCmd.AddCommand(checkCmd)
	checkCmd.Flags().StringVarP(&checkBucket, "check-bucket", "b", "", "check bucket name")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// checkCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// checkCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
