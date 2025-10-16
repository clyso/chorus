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
)

type Client interface {
	For(ctx context.Context, storage, account string) (*gophercloud.ServiceClient, error)
}

type ClientConfig struct {
	// Storages is a map of swift storages to redirect requests to.
	// Storage aliases in this map should be the same across all chorus services and configs and should not be changed.
	Storages    map[string]StorageCredentials `yaml:"storages"`
	MainStorage string                        `yaml:"mainStorage"`
}

type StorageCredentials struct {
	// Superuser - user with access to all projects.
	// Will be used to modify projects or if project user is not configured.
	// Superuser UserCredentials `yaml:"superuser"`
	// ProjectUsers - [OPTIONAL] user credentials per **projectID** (ex: 26720c5080bd4fd9af5b1e4a7dc565f8). If set, will be used to sync data within project.
	ProjectUsers map[string]UserCredentials `yaml:"projectUsers"`
	// StorageEndpointName - [REQUIRED] name of the object storage endpoint in openstack keystone.
	StorageEndpointName string `yaml:"storageEndpointName"`
	// Region - [OPTIONAL] openstack region name.
	Region string `yaml:"region"`
	// AuthURL - [REQUIRED] keystone auth URL.
	AuthURL string `yaml:"authURL"`
}

type UserCredentials struct {
	// Username - [REQUIRED] user name. Equal to openstack OS_USERNAME.
	Username string `yaml:"username"`
	// Password - [REQUIRED] user password. Equal to openstack OS_PASSWORD.
	Password string `yaml:"password"`
	// DomainName - [REQUIRED] user domain name. Equal to openstack OS_DOMAIN_NAME.
	DomainName string `yaml:"domainName"`
	// TenantName - [REQUIRED] tenant/project name. Equal to openstack OS_TENANT_NAME.
	TenantName string `yaml:"tenantName"`
}

func New(conf ClientConfig) (Client, error) {
	return &client{
		conf:    conf,
		clients: make(map[string]*gophercloud.ServiceClient),
	}, nil
}

type client struct {
	clients map[string]*gophercloud.ServiceClient
	conf    ClientConfig
	rw      sync.RWMutex
}

func (c *client) For(ctx context.Context, storage string, account string) (*gophercloud.ServiceClient, error) {
	storConf, ok := c.conf.Storages[storage]
	if !ok {
		return nil, fmt.Errorf("swift client: storage %q not found in config", storage)
	}
	user, ok := storConf.ProjectUsers[account]
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

func (c *client) create(ctx context.Context, storage string, user UserCredentials, cacheKey string) (*gophercloud.ServiceClient, error) {
	c.rw.Lock()
	defer c.rw.Unlock()
	client, ok := c.clients[cacheKey]
	if ok {
		return client, nil
	}

	providerClient, err := openstack.AuthenticatedClient(ctx, gophercloud.AuthOptions{
		IdentityEndpoint: c.conf.Storages[storage].AuthURL,
		Username:         user.Username,
		Password:         user.Password,
		TenantName:       user.TenantName,
		DomainName:       user.DomainName,
	})
	if err != nil {
		return nil, fmt.Errorf("swift client: failed to authenticate with storage %q for tenant %q: %w", storage, user.TenantName, err)
	}

	swiftClient, err := openstack.NewObjectStorageV1(providerClient, gophercloud.EndpointOpts{
		Type:         "object-store",
		Name:         c.conf.Storages[storage].StorageEndpointName,
		Availability: gophercloud.AvailabilityPublic,
		Region:       c.conf.Storages[storage].Region,
	})
	if err != nil {
		return nil, fmt.Errorf("swift client: failed to create swift client for storage %q and tenant %q: %w", storage, user.TenantName, err)
	}
	c.clients[cacheKey] = swiftClient
	return swiftClient, nil
}
