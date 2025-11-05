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

package router

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_replaceSwiftReqBaseURL(t *testing.T) {
	type args struct {
		old     string
		newBase string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "",
			args: args{
				old:     "https://old-host.com/some/random/endpoint",
				newBase: "https://new-host.com/",
			},
			want: "https://new-host.com/some/random/endpoint",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/some/random/endpoint",
				newBase: "https://new-host.com/newprefix",
			},
			want: "https://new-host.com/newprefix/some/random/endpoint",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/info",
				newBase: "https://new-host.com/newprefix",
			},
			want: "https://new-host.com/newprefix/info",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/info",
				newBase: "https://new-host.com",
			},
			want: "https://new-host.com/info",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/prefix/info",
				newBase: "https://new-host.com",
			},
			want: "https://new-host.com/info",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/v1/account/container/object?format=json",
				newBase: "https://new-host.com/prefix/",
			},
			want: "https://new-host.com/prefix/v1/account/container/object?format=json",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/v1/account/container/object?format=json",
				newBase: "https://new-host.com/prefix",
			},
			want: "https://new-host.com/prefix/v1/account/container/object?format=json",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/v1/account/container/object?format=json",
				newBase: "https://new-host.com/",
			},
			want: "https://new-host.com/v1/account/container/object?format=json",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/prefix/v1/account/container/object?format=json",
				newBase: "https://new-host.com/newprefix/",
			},
			want: "https://new-host.com/newprefix/v1/account/container/object?format=json",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/prefix/v1/account/container/object?format=json",
				newBase: "https://new-host.com/newprefix",
			},
			want: "https://new-host.com/newprefix/v1/account/container/object?format=json",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/prefix/v1/account/container/object?format=json",
				newBase: "https://new-host.com/",
			},
			want: "https://new-host.com/v1/account/container/object?format=json",
		},
		{
			name: "",
			args: args{
				old:     "https://old-host.com/prefix/v1/account/container/object?format=json",
				newBase: "http://new-host.com",
			},
			want: "http://new-host.com/v1/account/container/object?format=json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			ou, err := url.Parse(tt.args.old)
			r.NoError(err)
			nu, err := url.Parse(tt.args.newBase)
			r.NoError(err)
			got := replaceSwiftReqBaseURL(ou, *nu)
			r.EqualValues(tt.want, got.String())
		})
	}
}
