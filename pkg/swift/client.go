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
	Credentials    map[string]Credentials `yaml:"credentials"`
	StorageAddress `yaml:",inline" mapstructure:",squash"`
}
type StorageAddress struct {
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

func (c Credentials) Validate() error {
	if c.Password == "" {
		return fmt.Errorf("%w: swift credentials: password is not set", dom.ErrInvalidStorageConfig)
	}
	if c.Username == "" {
		return fmt.Errorf("%w: swift credentials: username is not set", dom.ErrInvalidStorageConfig)
	}
	if c.DomainName == "" {
		return fmt.Errorf("%w: swift credentials: domainName is not set", dom.ErrInvalidStorageConfig)
	}
	if c.TenantName == "" {
		return fmt.Errorf("%w: swift credentials: tenantName is not set", dom.ErrInvalidStorageConfig)
	}
	return nil
}

func (s *Storage) Validate() error {
	for user, cred := range s.Credentials {
		if err := cred.Validate(); err != nil {
			return fmt.Errorf("%w: for user %q", err, user)
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

func NewClient(ctx context.Context, addr StorageAddress, user Credentials) (*gophercloud.ServiceClient, error) {
	providerClient, err := openstack.AuthenticatedClient(ctx, gophercloud.AuthOptions{
		IdentityEndpoint: addr.AuthURL,
		Username:         user.Username,
		Password:         user.Password,
		TenantName:       user.TenantName,
		DomainName:       user.DomainName,
	})
	if err != nil {
		return nil, fmt.Errorf("swift client: failed to authenticate  for tenant %q: %w", user.TenantName, err)
	}

	if addr.StorageEndpointType == "" {
		addr.StorageEndpointType = defaultEndpointType
	}
	swiftClient, err := openstack.NewObjectStorageV1(providerClient, gophercloud.EndpointOpts{
		Type:         addr.StorageEndpointType,
		Name:         addr.StorageEndpointName,
		Availability: gophercloud.AvailabilityPublic,
		Region:       addr.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("swift client: failed to create swift client for tenant %q, region %q, endpoint (%q %q): %w", user.TenantName, addr.Region, addr.StorageEndpointType, addr.StorageEndpointName, err)
	}
	return swiftClient, nil
}
