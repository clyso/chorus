// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package swift

import (
	"context"
	"fmt"
	"sync"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"

	"github.com/clyso/chorus/pkg/dom"
)

const defaultEndpointType = "object-store"

type Client interface {
	For(ctx context.Context, storage, account string) (*gophercloud.ServiceClient, error)
}

type Storage struct {
	// ProjectUsers - [OPTIONAL] user credentials per **projectID** (ex: 26720c5080bd4fd9af5b1e4a7dc565f8). If set, will be used to sync data within project.
	Credentials map[string]Credentials `yaml:"credentials"`
	// StorageEndpointName - [REQUIRED] name of the object storage endpoint in openstack keystone.
	StorageEndpointName string `yaml:"storageEndpointName"`
	// Keystone Endpoint service type. Default is "object-storage"
	StorageEndpointType string `yaml:"storageEndpointType"`
	// Region - [OPTIONAL] openstack region name.
	Region string `yaml:"region"`
	// AuthURL - [REQUIRED] keystone auth URL.
	AuthURL string `yaml:"authURL"`
}

func (s *Storage) HasUser(user string) bool {
	_, ok := s.Credentials[user]
	return ok
}

func (s *Storage) UserList() []string {
	users := make([]string, 0, len(s.Credentials))
	for user := range s.Credentials {
		users = append(users, user)
	}
	return users
}

type Credentials struct {
	// Username - [REQUIRED] user name. Equal to openstack OS_USERNAME.
	Username string `yaml:"username"`
	// Password - [REQUIRED] user password. Equal to openstack OS_PASSWORD.
	Password string `yaml:"password"`
	// DomainName - [REQUIRED] user domain name. Equal to openstack OS_DOMAIN_NAME.
	DomainName string `yaml:"domainName"`
	// TenantName - [REQUIRED] tenant/project name. Equal to openstack OS_TENANT_NAME.
	TenantName string `yaml:"tenantName"`
}

func (s *Storage) Validate() error {
	if len(s.Credentials) == 0 {
		return fmt.Errorf("%w: swift storage config: no credentials provided", dom.ErrInvalidStorageConfig)
	}
	for user, cred := range s.Credentials {
		if cred.Password == "" {
			return fmt.Errorf("%w: swift storage config: password for user %q is not set", dom.ErrInvalidStorageConfig, user)
		}
		if cred.Username == "" {
			return fmt.Errorf("%w: swift storage config: username for user %q is not set", dom.ErrInvalidStorageConfig, user)
		}
		if cred.DomainName == "" {
			return fmt.Errorf("%w: swift storage config: domainName for user %q is not set", dom.ErrInvalidStorageConfig, user)
		}
		if cred.TenantName == "" {
			return fmt.Errorf("%w: swift storage config: tenantName for user %q is not set", dom.ErrInvalidStorageConfig, user)
		}
	}
	if s.StorageEndpointName == "" {
		return fmt.Errorf("%w: swift storage config: storageEndpointName is not set", dom.ErrInvalidStorageConfig)
	}
	if s.StorageEndpointType == "" {
		s.StorageEndpointType = defaultEndpointType
	}
	if s.AuthURL == "" {
		return fmt.Errorf("%w: swift storage config: authURL is not set", dom.ErrInvalidStorageConfig)
	}
	return nil
}

func New(conf map[string]*Storage) (Client, error) {
	return &client{
		conf:    conf,
		clients: make(map[string]*gophercloud.ServiceClient),
	}, nil
}

type client struct {
	clients map[string]*gophercloud.ServiceClient
	conf    map[string]*Storage
	rw      sync.RWMutex
}

func (c *client) For(ctx context.Context, storage string, account string) (*gophercloud.ServiceClient, error) {
	storConf, ok := c.conf[storage]
	if !ok {
		return nil, fmt.Errorf("swift client: storage %q not found in config", storage)
	}
	user, ok := storConf.Credentials[account]
	if !ok {
		// zerolog.Ctx(ctx).Debug().Msgf("swift client: account %q not found in project users for storage %q, using superuser", account, storage)
		// user = storConf.Superuser
		return nil, fmt.Errorf("swift client: account %q not found in project users for storage %q", account, storage)
	}
	// check client in cache
	cacheKey := storage + ":" + user.DomainName + ":" + user.TenantName + ":" + user.Username
	c.rw.RLock()
	client, ok := c.clients[cacheKey]
	c.rw.RUnlock()
	if ok {
		return client, nil
	}
	return c.create(ctx, storage, user, cacheKey)
}

func (c *client) create(ctx context.Context, storage string, user Credentials, cacheKey string) (*gophercloud.ServiceClient, error) {
	c.rw.Lock()
	defer c.rw.Unlock()
	client, ok := c.clients[cacheKey]
	if ok {
		return client, nil
	}

	providerClient, err := openstack.AuthenticatedClient(ctx, gophercloud.AuthOptions{
		IdentityEndpoint: c.conf[storage].AuthURL,
		Username:         user.Username,
		Password:         user.Password,
		TenantName:       user.TenantName,
		DomainName:       user.DomainName,
	})
	if err != nil {
		return nil, fmt.Errorf("swift client: failed to authenticate with storage %q for tenant %q: %w", storage, user.TenantName, err)
	}

	storConf := c.conf[storage]
	if storConf.StorageEndpointType == "" {
		storConf.StorageEndpointType = defaultEndpointType
	}
	swiftClient, err := openstack.NewObjectStorageV1(providerClient, gophercloud.EndpointOpts{
		Type:         storConf.StorageEndpointType,
		Name:         storConf.StorageEndpointName,
		Availability: gophercloud.AvailabilityPublic,
		Region:       storConf.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("swift client: failed to create swift client for storage %q and tenant %q, region %q, endpoint (%q %q): %w", storage, user.TenantName, storConf.Region, storConf.StorageEndpointType, storConf.StorageEndpointName, err)
	}
	c.clients[cacheKey] = swiftClient
	return swiftClient, nil
}
