/*
 * Copyright © 2025 STRATO GmbH
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

package format

import (
	"fmt"
	"time"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

type ReplNameBuilder func(*pb.Replication) string

func NewReplNameBuilder(inFormat string) (ReplNameBuilder, error) {
	isEscaped := -1
	outFormat := ""
	outFuncs := []ReplNameBuilder{}
	for i, c := range inFormat {
		if isEscaped >= 0 {
			switch c {
			case '%':
				outFormat += "%%"
			case 'f':
				outFormat += "%s"
				outFuncs = append(outFuncs, func(r *pb.Replication) string {
					return r.From
				})
			case 'F':
				outFormat += "%s"
				outFuncs = append(outFuncs, func(r *pb.Replication) string {
					return r.Bucket
				})
			case 't':
				outFormat += "%s"
				outFuncs = append(outFuncs, func(r *pb.Replication) string {
					return r.To
				})
			case 'T':
				outFormat += "%s"
				outFuncs = append(outFuncs, func(r *pb.Replication) string {
					if r.ToBucket == nil {
						return r.Bucket
					}
					return *r.ToBucket
				})
			case 'u':
				outFormat += "%s"
				outFuncs = append(outFuncs, func(r *pb.Replication) string {
					return r.User
				})
			case 'c':
				outFormat += "%s"
				outFuncs = append(outFuncs, func(r *pb.Replication) string {
					return r.CreatedAt.AsTime().Format(time.RFC3339)
				})
			case 'C':
				outFormat += "%s"
				outFuncs = append(outFuncs, func(r *pb.Replication) string {
					return fmt.Sprintf("%d", r.CreatedAt.AsTime().Unix())
				})
			default:
				return nil, fmt.Errorf("invalid format character: '%c' at index %d in '%s'", c, i, inFormat)
			}
			isEscaped = -1
		} else {
			if c == '%' {
				isEscaped = i
			} else {
				outFormat += string(c)
			}
		}
	}
	if isEscaped >= 0 {
		return nil, fmt.Errorf("incomplete escape sequence at index %d in format: '%s'", isEscaped, inFormat)
	}
	res := func(r *pb.Replication) string {
		outArgs := make([]interface{}, len(outFuncs))
		for i, f := range outFuncs {
			outArgs[i] = f(r)
		}
		return fmt.Sprintf(outFormat, outArgs...)
	}
	return res, nil
}

const ReplNameFormatHelp = "printf style format for replication name\n\t%%: literal '%'" +
	"\n\t%f: fromStorage\n\t%F: fromBucket\n\t%t: toStorage\n\t%T: toBucket\n\t" +
	"%c: createdAt (RFC3339)\n\t%C: createdAt (unix timestamp)\n"
