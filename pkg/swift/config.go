package swift

import (
	"fmt"
	"time"
)

type ProxyConfig struct {
	// AuthType auth token check on proxy.
	// Valid values:
	// 1. "bypass" - [DEFAULT] no auth check - proxy will forward requests with existing auth token
	//               to destination swift storage according to routing policy.
	//               Auth check will be done on destination swift storage side.
	// 2. "<storage_name>" - name of the storage from storages. If routing destination equals to storage name,
	//                       request will be forwarded as it is to swift storage. If destination is different,
	//                       proxy will check token validity in storage keystone and obtain username.
	//                       If username is provided in target storage credentials, proxy will obtain new token for destination
	//                       storage and forward request to it.
	AuthType string `yaml:"authType"`
	// Storages is a map of swift storages to redirect requests to.
	// Storage aliases in this map should be the same across all chorus services and configs and should not be changed.
	Storages map[string]ProxyStorageCredentials `yaml:"storages"`
	// MainStorage [REQUIRED] is a name of the main storage from storages map.
	// By default, proxy will forward all requests to main storage unless configured routing policy says different.
	MainStorage string `yaml:"mainStorage"`
	// HttpTimeout - timeout to target storage
	HttpTimeout time.Duration `yaml:"httpTimeout"`
}

func (c *ProxyConfig) Validate() error {
	if c.MainStorage == "" {
		return fmt.Errorf("swift proxy config: main storage not set")
	}
	if c.AuthType == "" {
		c.AuthType = "bypass"
	}
	if _, ok := c.Storages[c.AuthType]; c.AuthType != "bypass" && !ok {
		return fmt.Errorf("swift proxy config: invalid auth type %q", c.AuthType)
	}
	if _, ok := c.Storages[c.MainStorage]; !ok {
		return fmt.Errorf("swift proxy config: main storage %q not found in storages", c.MainStorage)
	}
	for name, storage := range c.Storages {
		if storage.StorageURL == "" {
			return fmt.Errorf("swift proxy config: storage %q URL not set", name)
		}
	}
	return nil
}

type WorkerConfig struct {
	// Storages is a map of swift storages to redirect requests to.
	// Storage aliases in this map should be the same across all chorus services and configs and should not be changed.
	Storages map[string]WokerStorageCredentials `yaml:"storages"`
}

type ProxyStorageCredentials struct {
	// StorageURL - [REQUIRED] swift storage URL.
	StorageURL string `yaml:"storageURL"`
	// AuthURL[OPTIONAL] - keystone auth URL.
	// Empty if authType is set to "bypass".
	AuthURL string `yaml:"authURL"`

	// Users[OPTIONAL] - list of users with access to this storage.
	// Will be used to obtain token if authType is not bypass and request should be forwarded to
	// swift storage using different keystone.
	Users []UserCredentials `yaml:"users"`
}

type WokerStorageCredentials struct {
	// StorageEndpointName - [REQUIRED] name of the object storage endpoint in openstack keystone.
	StorageEndpointName string `yaml:"storageEndpointName"`
	// Region - [OPTIONAL] openstack region name.
	Region string `yaml:"region"`
	// AuthURL - [REQUIRED] keystone auth URL.
	AuthURL string `yaml:"authURL"`
	// Superuser - user with access to all projects.
	// Will be used to modify projects or if project user is not configured.
	Superuser UserCredentials `yaml:"superuser"`
	// ProjectUsers - [OPTIONAL] user credentials per project. If set, will be used to sync data within project.
	ProjectUsers map[string]UserCredentials `yaml:"projectUsers"`
}

type UserCredentials struct {
	// Username - [REQUIRED] user name. Equal to openstack OS_USERNAME.
	Username string `yaml:"username"`
	// Password - [REQUIRED] user password. Equal to openstack OS_PASSWORD.
	Password string `yaml:"password"`
	// DomainName - [REQUIRED] user domain name. Equal to openstack OS_DOMAIN_NAME.
	DomainName string `yaml:"domainName"`
}
