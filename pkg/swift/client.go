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
	"context"
	"fmt"
	"sync"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/rs/zerolog"
)

type Client interface {
	For(ctx context.Context, storage, account string) (*gophercloud.ServiceClient, error)
}

func New(conf WorkerConfig) (Client, error) {
	return &client{
		conf:    conf,
		clients: make(map[string]*gophercloud.ServiceClient),
	}, nil
}

type client struct {
	rw      sync.RWMutex
	conf    WorkerConfig
	clients map[string]*gophercloud.ServiceClient
}

// For implements Client.
func (c *client) For(ctx context.Context, storage string, account string) (*gophercloud.ServiceClient, error) {
	storConf, ok := c.conf.Storages[storage]
	if !ok {
		return nil, fmt.Errorf("swift client: storage %q not found in config", storage)
	}
	user, ok := storConf.ProjectUsers[account]
	if !ok {
		zerolog.Ctx(ctx).Debug().Msgf("swift client: account %q not found in project users for storage %q, using superuser", account, storage)
		user = storConf.Superuser
	}
	// check client in cache
	key := storage + ":" + account + ":" + user.Username
	c.rw.RLock()
	client, ok := c.clients[key]
	c.rw.RUnlock()
	if ok {
		return client, nil
	}
	return c.create(ctx, storage, account, user)
}

// For implements Client.
func (c *client) create(ctx context.Context, storage string, account string, user UserCredentials) (*gophercloud.ServiceClient, error) {
	c.rw.Lock()
	defer c.rw.Unlock()
	key := storage + ":" + account + ":" + user.Username
	client, ok := c.clients[key]
	if ok {
		return client, nil
	}

	providerClient, err := openstack.AuthenticatedClient(ctx, gophercloud.AuthOptions{
		IdentityEndpoint: c.conf.Storages[storage].AuthURL,
		Username:         user.Username,
		Password:         user.Password,
		TenantName:       account,
		DomainName:       user.DomainName,
	})
	if err != nil {
		return nil, fmt.Errorf("swift client: failed to authenticate with storage %q for account %q: %w", storage, account, err)
	}

	swiftClient, err := openstack.NewObjectStorageV1(providerClient, gophercloud.EndpointOpts{
		Type:         "object-store",
		Name:         c.conf.Storages[storage].StorageEndpointName,
		Availability: gophercloud.AvailabilityPublic,
		Region:       c.conf.Storages[storage].Region,
	})
	if err != nil {
		return nil, fmt.Errorf("swift client: failed to create swift client for storage %q and account %q: %w", storage, account, err)
	}
	c.clients[key] = swiftClient
	return swiftClient, nil
}
