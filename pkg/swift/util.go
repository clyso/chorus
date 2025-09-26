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
