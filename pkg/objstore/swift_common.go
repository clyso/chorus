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

package objstore

import (
	"context"
	"net/http"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

func wrapSwiftCommon(client *gophercloud.ServiceClient) Common {
	return &swiftCommonClient{
		client: client,
	}
}

type swiftCommonClient struct {
	client *gophercloud.ServiceClient
}

func (s *swiftCommonClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	err := containers.Get(ctx, s.client, bucket, containers.GetOpts{}).Err
	if err != nil {
		if gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *swiftCommonClient) ListBuckets(ctx context.Context) ([]string, error) {
	res := []string{}
	const pageSize = 1000
	pager := containers.List(s.client, containers.ListOpts{
		Limit: pageSize,
	})
	err := pager.EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		containerList, err := containers.ExtractNames(page)
		if err != nil {
			return false, err
		}
		res = append(res, containerList...)
		// Continue to the next page
		return len(containerList) == pageSize, nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
