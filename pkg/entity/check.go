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

package entity

import (
	"sort"
)

type ConsistencyCheckLocation struct {
	Storage string
	Bucket  string
}

func NewConsistencyCheckLocation(storage string, bucket string) ConsistencyCheckLocation {
	return ConsistencyCheckLocation{
		Storage: storage,
		Bucket:  bucket,
	}
}

type ConsistencyCheckID struct {
	Locations []ConsistencyCheckLocation
}

func NewConsistencyCheckID(locations ...ConsistencyCheckLocation) ConsistencyCheckID {
	sort.Slice(locations, func(i, j int) bool {
		if locations[i].Storage < locations[j].Storage {
			return true
		}
		if locations[i].Storage > locations[j].Storage {
			return false
		}
		return locations[i].Bucket < locations[j].Bucket
	})
	return ConsistencyCheckID{
		Locations: locations,
	}
}

type ConsistencyCheckObjectID struct {
	Storage            string
	Prefix             string
	ConsistencyCheckID ConsistencyCheckID
}

func NewConsistencyCheckObjectID(consistencyCheckID ConsistencyCheckID, storage string, prefix string) ConsistencyCheckObjectID {
	return ConsistencyCheckObjectID{
		ConsistencyCheckID: consistencyCheckID,
		Storage:            storage,
		Prefix:             prefix,
	}
}

type ConsistencyCheckSetID struct {
	Object             string
	Etag               string
	ConsistencyCheckID ConsistencyCheckID
	VersionIndex       uint64
}

func NewConsistencyCheckSetID(consistencyCheckID ConsistencyCheckID, object string, etag string) ConsistencyCheckSetID {
	return ConsistencyCheckSetID{
		ConsistencyCheckID: consistencyCheckID,
		Object:             object,
		Etag:               etag,
	}
}

func NewVersionedConsistencyCheckSetID(consistencyCheckID ConsistencyCheckID, object string, versionIndex uint64, etag string) ConsistencyCheckSetID {
	return ConsistencyCheckSetID{
		ConsistencyCheckID: consistencyCheckID,
		Object:             object,
		VersionIndex:       versionIndex,
		Etag:               etag,
	}
}

type ConsistencyCheckSetEntry struct {
	Storage   string
	VersionID string
}

func NewConsistencyCheckSetEntry(storage string) ConsistencyCheckSetEntry {
	return ConsistencyCheckSetEntry{
		Storage: storage,
	}
}

func NewVersionedConsistencyCheckSetEntry(storage string, versionID string) ConsistencyCheckSetEntry {
	return ConsistencyCheckSetEntry{
		Storage:   storage,
		VersionID: versionID,
	}
}

type ConsistencyCheckReportEntry struct {
	Object         string
	Etag           string
	StorageEntries []ConsistencyCheckSetEntry
	VersionIndex   uint64
}

func NewConsistencyCheckReportEntry(object string, versionIndex uint64, etag string, storageEntries []ConsistencyCheckSetEntry) ConsistencyCheckReportEntry {
	return ConsistencyCheckReportEntry{
		Object:         object,
		VersionIndex:   versionIndex,
		Etag:           etag,
		StorageEntries: storageEntries,
	}
}

type ConsistencyCheckReportEntryPage struct {
	Entries []ConsistencyCheckReportEntry
	Cursor  uint64
}

func NewConsistencyCheckReportEntryPage(entries []ConsistencyCheckReportEntry, cursor uint64) ConsistencyCheckReportEntryPage {
	return ConsistencyCheckReportEntryPage{
		Entries: entries,
		Cursor:  cursor,
	}
}

type ConsistencyCheckStatus struct {
	Locations  []ConsistencyCheckLocation
	Queued     uint64
	Completed  uint64
	Ready      bool
	Consistent bool
}
