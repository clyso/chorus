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

package swift

import (
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
)

// Extends the gophercloud DownloadOpts to support swift "format=raw" query parameter.
type DownloadOpts struct {
	*objects.DownloadOpts
	Raw bool
}

func (d *DownloadOpts) ToObjectDownloadParams() (map[string]string, string, error) {
	if !d.Raw {
		return d.DownloadOpts.ToObjectDownloadParams()
	}
	headers, query, err := d.DownloadOpts.ToObjectDownloadParams()
	if query == "" {
		query = "format=raw"
	} else {
		query += "&format=raw"
	}
	return headers, query, err
}
