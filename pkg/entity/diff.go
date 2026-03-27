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

type DiffSettings struct {
	User        string
	Versioned   bool
	IgnoreEtags bool
	IgnoreSizes bool
}

func NewDiffSettings(user string, versioned bool, ignoreSizes bool, ignoreEtags bool) DiffSettings {
	if ignoreSizes {
		ignoreEtags = true
	}
	return DiffSettings{
		User:        user,
		Versioned:   versioned,
		IgnoreSizes: ignoreSizes,
		IgnoreEtags: ignoreEtags,
	}
}

type DiffLocation struct {
	Storage string
	Bucket  string
}

func NewDiffLocation(storage string, bucket string) DiffLocation {
	return DiffLocation{
		Storage: storage,
		Bucket:  bucket,
	}
}

type DiffID struct {
	Locations []DiffLocation
}

func NewDiffID(locations ...DiffLocation) DiffID {
	sort.Slice(locations, func(i, j int) bool {
		if locations[i].Storage < locations[j].Storage {
			return true
		}
		if locations[i].Storage > locations[j].Storage {
			return false
		}
		return locations[i].Bucket < locations[j].Bucket
	})
	return DiffID{
		Locations: locations,
	}
}

type DiffObjectID struct {
	Storage string
	Prefix  string
	DiffID  DiffID
}

func NewDiffObjectID(diffID DiffID, storage string, prefix string) DiffObjectID {
	return DiffObjectID{
		DiffID:  diffID,
		Storage: storage,
		Prefix:  prefix,
	}
}

type DiffSetID struct {
	Object       string
	Etag         string
	DiffID       DiffID
	Size         uint64
	VersionIndex uint64
}

func NewEtagDiffSetID(diffID DiffID, object string, size uint64, etag string) DiffSetID {
	return DiffSetID{
		DiffID: diffID,
		Object: object,
		Size:   size,
		Etag:   etag,
	}
}

func NewVersionedEtagDiffSetID(diffID DiffID, object string, versionIndex uint64, size uint64, etag string) DiffSetID {
	return DiffSetID{
		DiffID:       diffID,
		Object:       object,
		VersionIndex: versionIndex,
		Size:         size,
		Etag:         etag,
	}
}

func NewSizeDiffSetID(diffID DiffID, object string, size uint64) DiffSetID {
	return DiffSetID{
		DiffID: diffID,
		Object: object,
		Size:   size,
	}
}

func NewVersionedSizeDiffSetID(diffID DiffID, object string, versionIndex uint64, size uint64) DiffSetID {
	return DiffSetID{
		DiffID:       diffID,
		Object:       object,
		VersionIndex: versionIndex,
		Size:         size,
	}
}

func NewNameDiffSetID(diffID DiffID, object string) DiffSetID {
	return DiffSetID{
		DiffID: diffID,
		Object: object,
	}
}

func NewVersionedNameDiffSetID(diffID DiffID, object string, versionIndex uint64) DiffSetID {
	return DiffSetID{
		DiffID:       diffID,
		Object:       object,
		VersionIndex: versionIndex,
	}
}

type DiffSetEntry struct {
	Location  DiffLocation
	VersionID string
	IsDir     bool
}

func NewDiffSetEntry(location DiffLocation, isDir bool) DiffSetEntry {
	return DiffSetEntry{
		Location: location,
		IsDir:    isDir,
	}
}

func NewVersionedDiffSetEntry(location DiffLocation, versionID string) DiffSetEntry {
	return DiffSetEntry{
		Location:  location,
		VersionID: versionID,
	}
}

type DiffReportEntry struct {
	Object         string
	Etag           string
	StorageEntries []DiffSetEntry
	Size           uint64
	VersionIndex   uint64
}

func NewDiffReportEntry(object string, versionIndex uint64, size uint64, etag string, storageEntries []DiffSetEntry) DiffReportEntry {
	return DiffReportEntry{
		Object:         object,
		VersionIndex:   versionIndex,
		Size:           size,
		Etag:           etag,
		StorageEntries: storageEntries,
	}
}

type DiffReportEntryPage struct {
	Entries []DiffReportEntry
	Cursor  uint64
}

func NewDiffReportEntryPage(entries []DiffReportEntry, cursor uint64) DiffReportEntryPage {
	return DiffReportEntryPage{
		Entries: entries,
		Cursor:  cursor,
	}
}

type DiffQueueStatus struct {
	Queued    uint64
	Completed uint64
	Ready     bool
}

type DiffCheckStatus struct {
	Settings   DiffSettings
	Queue      DiffQueueStatus
	Consistent bool
}

type DiffStatus struct {
	FixQueue  *DiffQueueStatus
	Locations []DiffLocation
	Check     DiffCheckStatus
}

type ObjectVersionInfo struct {
	Version      string
	Size         uint64
	DeleteMarker bool
}

type DiffFixID struct {
	Destination string
	DiffID      DiffID
}

func NewDiffFixID(diffID DiffID, destination string) DiffFixID {
	return DiffFixID{
		DiffID:      diffID,
		Destination: destination,
	}
}

type DiffFixCopyObject struct {
	Name  string
	IsDir bool
}

func NewDiffFixCopyObject(name string, isDir bool) DiffFixCopyObject {
	return DiffFixCopyObject{
		Name:  name,
		IsDir: isDir,
	}
}
