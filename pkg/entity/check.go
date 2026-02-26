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

type ConsistencyCheckSettings struct {
	Versioned bool
	WithSize  bool
	WithEtag  bool
}

func NewConsistencyCheckSettings(versioned bool, withSize bool, withEtag bool) ConsistencyCheckSettings {
	return ConsistencyCheckSettings{
		Versioned: versioned,
		WithSize:  withSize,
		WithEtag:  withEtag,
	}
}

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
	Size               uint64
	VersionIndex       uint64
}

func NewEtagConsistencyCheckSetID(consistencyCheckID ConsistencyCheckID, object string, size uint64, etag string) ConsistencyCheckSetID {
	return ConsistencyCheckSetID{
		ConsistencyCheckID: consistencyCheckID,
		Object:             object,
		Size:               size,
		Etag:               etag,
	}
}

func NewVersionedEtagConsistencyCheckSetID(consistencyCheckID ConsistencyCheckID, object string, versionIndex uint64, size uint64, etag string) ConsistencyCheckSetID {
	return ConsistencyCheckSetID{
		ConsistencyCheckID: consistencyCheckID,
		Object:             object,
		VersionIndex:       versionIndex,
		Size:               size,
		Etag:               etag,
	}
}

func NewSizeConsistencyCheckSetID(consistencyCheckID ConsistencyCheckID, object string, size uint64) ConsistencyCheckSetID {
	return ConsistencyCheckSetID{
		ConsistencyCheckID: consistencyCheckID,
		Object:             object,
		Size:               size,
	}
}

func NewVersionedSizeConsistencyCheckSetID(consistencyCheckID ConsistencyCheckID, object string, versionIndex uint64, size uint64) ConsistencyCheckSetID {
	return ConsistencyCheckSetID{
		ConsistencyCheckID: consistencyCheckID,
		Object:             object,
		VersionIndex:       versionIndex,
		Size:               size,
	}
}

func NewNameConsistencyCheckSetID(consistencyCheckID ConsistencyCheckID, object string) ConsistencyCheckSetID {
	return ConsistencyCheckSetID{
		ConsistencyCheckID: consistencyCheckID,
		Object:             object,
	}
}

func NewVersionedNameConsistencyCheckSetID(consistencyCheckID ConsistencyCheckID, object string, versionIndex uint64) ConsistencyCheckSetID {
	return ConsistencyCheckSetID{
		ConsistencyCheckID: consistencyCheckID,
		Object:             object,
		VersionIndex:       versionIndex,
	}
}

type ConsistencyCheckSetEntry struct {
	Location  ConsistencyCheckLocation
	VersionID string
}

func NewConsistencyCheckSetEntry(location ConsistencyCheckLocation) ConsistencyCheckSetEntry {
	return ConsistencyCheckSetEntry{
		Location: location,
	}
}

func NewVersionedConsistencyCheckSetEntry(location ConsistencyCheckLocation, versionID string) ConsistencyCheckSetEntry {
	return ConsistencyCheckSetEntry{
		Location:  location,
		VersionID: versionID,
	}
}

type ConsistencyCheckReportEntry struct {
	Object         string
	Etag           string
	StorageEntries []ConsistencyCheckSetEntry
	Size           uint64
	VersionIndex   uint64
}

func NewConsistencyCheckReportEntry(object string, versionIndex uint64, size uint64, etag string, storageEntries []ConsistencyCheckSetEntry) ConsistencyCheckReportEntry {
	return ConsistencyCheckReportEntry{
		Object:         object,
		VersionIndex:   versionIndex,
		Size:           size,
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
	ConsistencyCheckSettings
}

type ObjectVersionInfo struct {
	Version string
	Size    uint64
}
