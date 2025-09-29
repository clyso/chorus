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

package validate

import (
	"errors"
	"fmt"

	"github.com/clyso/chorus/pkg/s3"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func StorageLocations(storages *s3.StorageConfig, locations []*pb.MigrateLocation) error {
	if len(locations) < 2 {
		return errors.New("at least 2 migration locations should be provided")
	}

	for idx, location := range locations {
		if location.Bucket == "" {
			return fmt.Errorf("location %d bucket is empty", idx)
		}
		if location.Storage == "" {
			return fmt.Errorf("location %d storage is empty", idx)
		}
		if _, ok := storages.Storages[location.Storage]; !ok {
			return fmt.Errorf("unable to find storage %s in config", location.Storage)
		}
	}

	return nil
}

func StorageLocationsWithUser(storages *s3.StorageConfig, locations []*pb.MigrateLocation, user string) error {
	if len(locations) < 2 {
		return errors.New("at least 2 migration locations should be provided")
	}

	for idx, location := range locations {
		if location.Bucket == "" {
			return fmt.Errorf("location %d bucket is empty", idx)
		}
		if location.Storage == "" {
			return fmt.Errorf("location %d storage is empty", idx)
		}
		if _, ok := storages.Storages[location.Storage]; !ok {
			return fmt.Errorf("unable to find storage %s in config", location.Storage)
		}
		_, ok := storages.Storages[location.Storage].Credentials[user]
		if !ok {
			return fmt.Errorf("unable to find user %s storage %s in config", user, location.Storage)
		}
	}

	return nil
}
