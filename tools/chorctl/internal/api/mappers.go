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

package api

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func StorageHeader() string {
	return "NAME\tADDRESS\tPROVIDER\tUSERS"
}

func StorageRow(in *pb.Storage) string {
	name := in.Name
	if in.IsMain {
		name += " [MAIN]"
	}
	var users []string
	for _, c := range in.Credentials {
		users = append(users, c.Alias)
	}
	return fmt.Sprintf("%s\t%s\t%s\t%s", name, in.Address, in.Provider, strings.Join(users, ","))
}

func ReplHeader() string {
	return "NAME\tPROGRESS\tSIZE\tOBJECTS\tEVENTS\tPAUSED\tAGE\tHAS_SWITCH"
}

func ReplRow(in *pb.Replication) string {
	p := 0.0
	if in.InitBytesListed != 0 {
		p = float64(in.InitBytesDone) / float64(in.InitBytesListed)
	}
	bytes := fmt.Sprintf("%s/%s", ByteCountIEC(in.InitBytesDone), ByteCountIEC(in.InitBytesListed))
	objects := fmt.Sprintf("%d/%d", in.InitObjDone, in.InitObjListed)
	events := fmt.Sprintf("%d/%d", in.EventsDone, in.Events)
	if in.ToBucket != "" {
		in.To += ":" + in.ToBucket
	}
	return fmt.Sprintf("%s:%s:%s->%s\t%s\t%s\t%s\t%s\t%v\t%s\t%v", in.User, in.Bucket, in.From, in.To, ToPercentage(p), bytes, objects, events, in.IsPaused, DateToAge(in.CreatedAt), in.HasSwitch)
}

func ToPercentage(in float64) string {
	progress := ""
	switch {
	case in < 0.1:
		progress = "[          ]"
	case in < 0.2:
		progress = "[#         ]"
	case in <= 0.3:
		progress = "[##        ]"
	case in <= 0.4:
		progress = "[###       ]"
	case in <= 0.5:
		progress = "[####      ]"
	case in <= 0.6:
		progress = "[#####     ]"
	case in <= 0.7:
		progress = "[######    ]"
	case in <= 0.8:
		progress = "[#######   ]"
	case in <= 0.9:
		progress = "[########  ]"
	case in < 1:
		progress = "[######### ]"
	default:
		progress = "[##########]"
	}
	in *= 100
	progress += fmt.Sprintf(" %5.1f %%", in)
	return progress
}

func ByteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

func DateToAge(d *timestamppb.Timestamp) string {
	if d == nil {
		return "-"
	}
	age := time.Now().Sub(d.AsTime())
	return DurationToStr(age)
}

func DateToStr(d *timestamppb.Timestamp) string {
	if d == nil {
		return "-"
	}
	return d.AsTime().Format("02/01 15:04:05")
}

func DurationToStr(age time.Duration) string {
	if age <= time.Second {
		return age.Round(time.Millisecond).String()
	}
	if age <= time.Minute {
		return fmt.Sprintf("%ds", int(age.Seconds()))
	}
	if age <= time.Hour {
		return fmt.Sprintf("%dm", int(age.Minutes()))
	}
	if age <= 24*time.Hour {
		return fmt.Sprintf("%dh%dm", int(age.Hours()), int(age.Minutes())%60)
	}
	if age <= 7*24*time.Hour {
		return fmt.Sprintf("%dd%dh", int(age.Hours()/24), int(age.Hours())%24)
	}
	return fmt.Sprintf("%dd", int(age.Hours()/24))
}

func ConsistencyCheckHeader() string {
	return "READY\tQUEUED\tCOMPLETED\tSTORAGES"
}

func ConsistencyCheckRow(in *pb.ConsistencyCheck) string {
	storageLocations := make([]string, 0, len(in.Locations))
	for _, location := range in.Locations {
		storageLocations = append(storageLocations, fmt.Sprintf("%s:%s", location.Storage, location.Bucket))
	}
	return fmt.Sprintf("%t\t%d\t%d\t%s", in.Ready, in.Queued, in.Completed, strings.Join(storageLocations, ", "))
}

func ConsistencyCheckReportBrief(in *pb.ConsistencyCheck) string {
	briefTable := `READY:	%t
QUEUED:	%d
COMPLETED:	%d
CONSISTENT:	%t`
	return fmt.Sprintf(briefTable, in.Ready, in.Queued, in.Completed, in.Consistent)
}

func ConsistencyCheckReportHeader(storages []string) string {
	return fmt.Sprintf("PATH\tETAG\t%s", strings.Join(storages, "\t"))
}

func ConsistencyCheckReportRow(storages []string, entry *pb.ConsistencyCheckReportEntry) string {
	storageMarkers := ""
	for _, storage := range storages {
		if slices.Contains(entry.Storages, storage) {
			storageMarkers += "\t✓"
		} else {
			storageMarkers += "\tX"
		}
	}
	return fmt.Sprintf("%s\t%s%s", entry.Object, entry.Etag, storageMarkers)
}

func SwitchHeader() string {
	return "USER\tBUCKET\tFROM\tTO\tSTATUS\tLAST_STARTED\tDONE"
}

func PrintSwitchRow(w io.Writer, in *pb.GetBucketSwitchStatusResponse, wide bool) {
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		in.ReplicationId.User,
		in.ReplicationId.Bucket,
		in.ReplicationId.From,
		in.ReplicationId.To,
		in.LastStatus.String(),
		DateToAge(in.LastStartedAt),
		DateToAge(in.DoneAt))
	if wide {
		for _, hist := range in.History {
			fmt.Fprintf(w, "\t\t%s\n", hist)
		}
	}
}
