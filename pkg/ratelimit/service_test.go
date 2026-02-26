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

package ratelimit

import (
	"testing"

	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/swift"
)

func Test_filterRateLimitMethods(t *testing.T) {
	type args struct {
		conf    RateLimit
		n       int
		options []Opt
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "metadata api included, no options provided",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: true,
					RPM:                120,
				},
				n: 1,
			},
			want: 1,
		},
		{
			name: "metadata api included, s3 meta request",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: true,
					RPM:                120,
				},
				n: 1,
				options: []Opt{
					S3Method(s3.GetBucketAcl),
				},
			},
			want: 1,
		},
		{
			name: "metadata api included, s3 upload request",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: true,
					RPM:                120,
				},
				n: 1,
				options: []Opt{
					S3Method(s3.PutObject),
				},
			},
			want: 1,
		},
		{
			name: "metadata api included, swift meta request",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: true,
					RPM:                120,
				},
				n: 1,
				options: []Opt{
					SwiftMethod(swift.HeadObject),
				},
			},
			want: 1,
		},
		{
			name: "metadata api included, swift download request",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: true,
					RPM:                120,
				},
				n: 1,
				options: []Opt{
					SwiftMethod(swift.GetObject),
				},
			},
			want: 1,
		},
		// metadata api excluded
		{
			name: "metadata api excluded, s3 meta request",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: false,
					RPM:                120,
				},
				n: 1,
				options: []Opt{
					S3Method(s3.GetBucketAcl),
				},
			},
			want: 0,
		},
		{
			name: "metadata api excluded, s3 upload request",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: false,
					RPM:                120,
				},
				n: 1,
				options: []Opt{
					S3Method(s3.PutObject),
				},
			},
			want: 1,
		},
		{
			name: "metadata api excluded, swift meta request",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: false,
					RPM:                120,
				},
				n: 1,
				options: []Opt{
					SwiftMethod(swift.HeadObject),
				},
			},
			want: 0,
		},
		{
			name: "metadata api excluded, swift download request",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: false,
					RPM:                120,
				},
				n: 1,
				options: []Opt{
					SwiftMethod(swift.GetObject),
				},
			},
			want: 1,
		},
		{
			name: "multiple methods",
			args: args{
				conf: RateLimit{
					Enabled:            true,
					IncludeMetadataAPI: false,
					RPM:                120,
				},
				n: 4,
				options: []Opt{
					S3Method(s3.GetBucketAcl),
					S3Method(s3.PutObject),
					SwiftMethod(swift.HeadObject),
					SwiftMethod(swift.GetObject),
				},
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterRateLimitMethods(tt.args.conf, tt.args.n, tt.args.options...); got != tt.want {
				t.Errorf("filterRateLimitMethods() = %v, want %v", got, tt.want)
			}
		})
	}
}
