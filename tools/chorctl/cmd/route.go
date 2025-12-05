// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

var (
	routeFlagUser    string
	routeFlagStorage string
	routeFlagBucket  string
	routeFlagNoColor bool
)

func init() {
	rootCmd.AddCommand(routeCmd)
	routeCmd.Flags().StringVarP(&routeFlagUser, "user", "u", "", "filter by routing policy user")
	routeCmd.Flags().StringVarP(&routeFlagStorage, "storage", "s", "", "filter by destination storage")
	routeCmd.Flags().StringVarP(&routeFlagBucket, "bucket", "b", "", "filter by routing policy bucket")
	routeCmd.Flags().BoolVar(&routeFlagNoColor, "no-color", false, "disable colored output")
}

var routeCmd = &cobra.Command{
	Use:   "route",
	Short: "Routing policies sub commands",
	Long: `Lists routing policies.

List configured routing policies:
  chorctl route`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		conn, client := newPolicyClient(ctx)
		defer conn.Close()

		hasFilter := false
		filter := &pb.RoutingsRequest_Filter{}
		if routeFlagUser != "" {
			filter.User = &routeFlagUser
			hasFilter = true
		}
		if routeFlagStorage != "" {
			filter.ToStorage = &routeFlagStorage
			hasFilter = true
		}
		if routeFlagBucket != "" {
			filter.Bucket = &routeFlagBucket
			hasFilter = true
		}
		req := &pb.RoutingsRequest{}
		if hasFilter {
			req.Filter = filter
		}
		resp, err := client.ListRoutings(ctx, req)
		api.PrintGrpcError(err)

		result := make([][]string, 0, len(resp.UserRoutings)+len(resp.BucketRoutings)+1)
		// add user routings:
		for _, rt := range resp.UserRoutings {
			result = append(result, []string{"*", rt.User, rt.ToStorage, strconv.FormatBool(rt.IsBlocked)})
		}
		// add bucket routings:
		for _, rt := range resp.BucketRoutings {
			result = append(result, []string{rt.Bucket, rt.User, rt.ToStorage, strconv.FormatBool(rt.IsBlocked)})
		}
		// sort by user, then by bucket, then by storage:
		slices.SortFunc(result, func(a, b []string) int {
			userA, userB := a[1], b[1]
			if userA != userB {
				return strings.Compare(userA, userB)
			}
			bucketA, bucketB := a[0], b[0]
			if bucketA != bucketB {
				return strings.Compare(bucketA, bucketB)
			}
			storageA, storageB := a[2], b[2]
			return strings.Compare(storageA, storageB)
		})
		// default route last:
		result = append(result, []string{"*", "*", resp.Main, "false"})

		if !routeFlagNoColor {
			// apply colors:
			userColor := newColorFunc()
			storageColor := newColorFunc()
			for i := range result {
				result[i][0] = colorBucket(result[i][0])
				result[i][1] = userColor(result[i][1])
				result[i][2] = storageColor(result[i][2])
				result[i][3] = colorBool(result[i][3])
			}
		}

		w := tabwriter.NewWriter(os.Stdout, 10, 1, 5, ' ', 0)
		if routeFlagNoColor {
			fmt.Fprintln(w, "BUCKET\tUSER\tDESTINATION\tBLOCKED")
		} else {
			//wrap each in bold white
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", colorHeader("BUCKET"), colorHeader("USER"), colorHeader("DESTINATION"), colorHeader("BLOCKED"))
		}
		for _, row := range result {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", row[0], row[1], row[2], row[3])
		}
		w.Flush()
	},
}

var terminalColorNums = []string{
	// generate really distincct terminal colors
	`[94m`, // bright blue
	`[95m`, // bright magenta
	`[93m`, // bright yellow
	`[91m`, // bright red
	`[96m`, // bright cyan
	`[32m`, // green
	`[34m`, // blue
	`[36m`, // cyan
	`[35m`, // magenta
	`[92m`, // bright green
	`[33m`, // yellow
	`[31m`, // red

}

func colorHeader(s string) string {
	return fmt.Sprintf("\033[97m%s\033[0m", s) // bold white
}
func colorBool(b string) string {
	bb, err := strconv.ParseBool(b)
	if err != nil {
		panic("invalid bool string")
	}
	if bb {
		return fmt.Sprintf("\033[91m%s\033[0m", b) // red
	}
	return fmt.Sprintf("\033[92m%s\033[0m", b) // green
}

func colorBucket(s string) string {
	if s == "*" {
		// yellow for wildcard
		return fmt.Sprintf("\033[93m%s\033[0m", s)
	}
	//normal gray for bucket names
	return fmt.Sprintf("\033[90m%s\033[0m", s)
}
func newColorFunc() func(string) string {
	maxIdx := 0
	idxSet := map[string]int{}
	return func(s string) string {
		if s == "*" {
			// yellow for wildcard
			return fmt.Sprintf("\033[93m%s\033[0m", s)
		}
		idx, ok := idxSet[s]
		if !ok {
			idx = maxIdx
			idxSet[s] = idx
			maxIdx++
		}
		colorCode := terminalColorNums[idx%len(terminalColorNums)]
		return fmt.Sprintf("\033%s%s\033[0m", colorCode, s)
	}
}
