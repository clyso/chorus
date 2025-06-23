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

package env

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"runtime"
	"text/template"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/domains"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/endpoints"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/roles"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/services"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/users"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	//go:embed proxy-server.conf
	proxyServerConf string
	//go:embed ceph-entrypoint.sh
	cephEntrypointSh string
)

const (
	CRedisImage    = "redis:8.0.1-alpine"
	CRedisPort     = 6379
	CRedisPassword = "password"

	CKeystoneImage                    = "ghcr.io/aiivashchenko/docker-keystone:27.0.0"
	CKeystoneExternalPort             = 5000
	CKeystoneAdminPort                = 35357
	CKeystoneAdminUsername            = "admin"
	CKeystoneAdminPassword            = "password"
	CKeystoneAdminDomainName          = "Default"
	CKeystoneAdminTenantName          = "admin"
	CkeystoneAdminRoleName            = "admin"
	CKeystoneObjectStoreServiceType   = "object-store"
	CKeystoneServiceProjectName       = "service"
	CKeystoneSwiftServiceName         = "swift"
	CKeystoneSwiftUsername            = "swift"
	CKeystoneSwiftPassword            = "swiftpass"
	CKeystoneSwiftEndpointName        = "swift"
	CKeystoneSwiftEndpointURLTemplate = "http://%s:%d/v1/AUTH_$(tenant_id)s"
	CKeystoneSwiftOperatorRole        = "swift-operator"
	CKeystoneSwiftResellerRole        = "swift-reseller"
	CKeystoneCephUsername             = "ceph"
	CKeystoneCephPassword             = "cephpass"
	CKeystoneCephServiceName          = "ceph"
	CKeystoneCephEndpointName         = "ceph"
	CKeystoneCephEndpointURLTemplate  = "http://%s:%d/swift/v1/AUTH_$(tenant_id)s"
	CKeystoneCephOperatorRole         = "ceph-operator"
	CKeystoneCephResellerRole         = "ceph-reseller"

	CSwiftImage = "ghcr.io/aiivashchenko/docker-swift:2.35.0"
	CSwiftPort  = 8080

	CMinioImage          = "minio/minio:RELEASE.2025-05-24T17-08-30Z-cpuv1"
	CMinioUsername       = "minioadmin"
	CMinioPassword       = "minioadmin"
	CMinioS3Port         = 9000
	CMinioManagementPort = 9001

	CCephX8664Image       = "quay.io/ceph/demo:main-985bb830-main-centos-stream8-x86_64"
	CCephARM64Image       = "quay.io/ceph/demo:main-985bb83-main-centos-arm64-stream8-aarch64"
	CCephPublicNetwork    = "127.0.0.1/32"
	CCephMonIP            = "127.0.0.1"
	CCephDomainName       = "localhost"
	CCephDemoUID          = "cephuid"
	CCephOSDDirectoryMode = "directory"
	CCephAPIPort          = 8080

	CContainerStopDeadline = 10 * time.Second

	CNATPortTemplate = "%d/tcp"
)

// ComponentOption interface to support Functional options for test-containers.
type ComponentOption interface {
	apply(*ComponentCreationConfig)
}

type disableContainerSTDOut struct{}

type disableContainerSTDErr struct{}

func (o disableContainerSTDOut) apply(cfg *ComponentCreationConfig) {
	cfg.DisabledLogs = append(cfg.DisabledLogs, testcontainers.StdoutLog)
}

func (o disableContainerSTDErr) apply(cfg *ComponentCreationConfig) {
	cfg.DisabledLogs = append(cfg.DisabledLogs, testcontainers.StderrLog)
}

// WithDisabledSTDOutLog disables STDOUT logs for the container.
func WithDisabledSTDOutLog() ComponentOption {
	return disableContainerSTDOut{}
}

// WithDisabledSTDErrLog disables STDERR logs for the container.
func WithDisabledSTDErrLog() ComponentOption {
	return disableContainerSTDErr{}
}

type ContainerLogConsumer struct {
	componentName string
	disabledLogs  map[string]struct{}
}

func NewContainerLogConsumer(componenetName string, disabled []string) *ContainerLogConsumer {
	d := make(map[string]struct{}, len(disabled))
	for _, logType := range disabled {
		d[logType] = struct{}{}
	}
	return &ContainerLogConsumer{
		componentName: componenetName,
		disabledLogs:  d,
	}
}

func (r *ContainerLogConsumer) Accept(l testcontainers.Log) {
	if _, ok := r.disabledLogs[l.LogType]; ok {
		return
	}
	fmt.Printf("%s %s %s", r.componentName, l.LogType, l.Content)
}

type ContainerHost struct {
	NAT   string
	Local string
}

type ContainerPort struct {
	Exposed   int
	Forwarded int
}

type ComponentCreationConfig struct {
	Dependencies    []string
	DisabledLogs    []string
	InstantiateFunc func(context.Context, *TestEnvironment, string, *ComponentCreationConfig) error
}

type RedisAccessConfig struct {
	Port     ContainerPort
	Host     ContainerHost
	Password string
}

type KeystoneAccessConfig struct {
	ExternalPort   ContainerPort
	AdminPort      ContainerPort
	Host           ContainerHost
	User           string
	Password       string
	TenantName     string
	DefaultDomain  *domains.Domain
	ServiceProject *projects.Project
}

type StorageKeystoneAccessConfig struct {
	OperatorRole *roles.Role
	ResellerRole *roles.Role
}

type SwiftAccessConfig struct {
	Port     ContainerPort
	Host     ContainerHost
	Keystone StorageKeystoneAccessConfig
}

type MinioAccessConfig struct {
	S3Port         ContainerPort
	ManagementPort ContainerPort
	Host           ContainerHost
	User           string
	Password       string
}

type CephAccessConfig struct {
	Port     ContainerPort
	Host     ContainerHost
	Keystone StorageKeystoneAccessConfig
}

type SwiftProxyConfigTemplateValues struct {
	AuthHost         string
	AdminAuthPort    int
	ExternalAuthPort int
	AdminTenant      string
	AdminDomain      string
	AdminUser        string
	AdminPassword    string
	OperatorRole     string
	ResellerRole     string
}

type CephRGWTemplateValues struct {
	AuthHost         string
	ExternalAuthPort int
	AdminProject     string
	AdminDomain      string
	AdminUser        string
	AdminPassword    string
	OperatorRole     string
	ResellerRole     string
}

type TestEnvironment struct {
	creationConfigs map[string]ComponentCreationConfig
	network         *testcontainers.DockerNetwork
	containers      map[string]testcontainers.Container
	accessConfigs   map[string]any
}

func NewTestEnvironment(ctx context.Context, envConfig map[string]ComponentCreationConfig) (*TestEnvironment, error) {
	configLen := len(envConfig)
	env := &TestEnvironment{
		creationConfigs: make(map[string]ComponentCreationConfig, configLen),
		containers:      make(map[string]testcontainers.Container, configLen),
		accessConfigs:   make(map[string]any, configLen),
	}
	env.creationConfigs = envConfig

	vlan, err := vlan(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to create env network: %w", err)
	}
	env.network = vlan

	for componentName, componentConfig := range envConfig {
		if err := instantiate(ctx, env, componentName, &componentConfig); err != nil {
			return nil, fmt.Errorf("unable to create component instance %s: %w", componentName, err)
		}
	}

	return env, nil
}

func (r *TestEnvironment) Terminate(ctx context.Context) error {
	stopDuration := CContainerStopDeadline
	for name, container := range r.containers {
		if err := container.Stop(ctx, &stopDuration); err != nil {
			return fmt.Errorf("unable to stop container %s: %w", name, err)
		}
	}
	if err := r.network.Remove(ctx); err != nil {
		return fmt.Errorf("unable to remove network: %w", err)
	}
	return nil
}

func (r *TestEnvironment) GetRedisAccessConfig(instanceName string) (*RedisAccessConfig, error) {
	instanceAccessCfg, ok := r.accessConfigs[instanceName]
	if !ok {
		return nil, fmt.Errorf("unable to find instance %s", instanceName)
	}
	redisAccessCfg, ok := instanceAccessCfg.(RedisAccessConfig)
	if !ok {
		return nil, fmt.Errorf("unable to cast instance access cfg %s to redis access cfg", instanceName)
	}
	return &redisAccessCfg, nil
}

func (r *TestEnvironment) GetKeystoneAccessConfig(instanceName string) (*KeystoneAccessConfig, error) {
	instanceAccessCfg, ok := r.accessConfigs[instanceName]
	if !ok {
		return nil, fmt.Errorf("unable to find instance %s", instanceName)
	}
	keystoneAccessCfg, ok := instanceAccessCfg.(KeystoneAccessConfig)
	if !ok {
		return nil, fmt.Errorf("unable to cast instance access cfg %s to keystone access cfg", instanceName)
	}
	return &keystoneAccessCfg, nil
}

func (r *TestEnvironment) GetSwiftAccessConfig(instanceName string) (*SwiftAccessConfig, error) {
	instanceAccessCfg, ok := r.accessConfigs[instanceName]
	if !ok {
		return nil, fmt.Errorf("unable to find instance %s", instanceName)
	}
	swiftAccessCfg, ok := instanceAccessCfg.(SwiftAccessConfig)
	if !ok {
		return nil, fmt.Errorf("unable to cast instance access cfg %s to swift access cfg", instanceName)
	}
	return &swiftAccessCfg, nil
}

func (r *TestEnvironment) GetMinioAccessConfig(instanceName string) (*MinioAccessConfig, error) {
	instanceAccessCfg, ok := r.accessConfigs[instanceName]
	if !ok {
		return nil, fmt.Errorf("unable to find instance %s", instanceName)
	}
	minioAccessCfg, ok := instanceAccessCfg.(MinioAccessConfig)
	if !ok {
		return nil, fmt.Errorf("unable to cast instance access cfg %s to minio access cfg", instanceName)
	}
	return &minioAccessCfg, nil
}

func (r *TestEnvironment) GetCephAccessConfig(instanceName string) (*CephAccessConfig, error) {
	instanceAccessCfg, ok := r.accessConfigs[instanceName]
	if !ok {
		return nil, fmt.Errorf("unable to find instance %s", instanceName)
	}
	cephAccessCfg, ok := instanceAccessCfg.(CephAccessConfig)
	if !ok {
		return nil, fmt.Errorf("unable to cast instance access cfg %s to ceph access cfg", instanceName)
	}
	return &cephAccessCfg, nil
}

func AsMinio(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startMinioInstance,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsRedis(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startRedisInstance,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsKeystone(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startKeystoneInstance,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsSwift(keystoneInstance string, opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		Dependencies:    []string{keystoneInstance},
		InstantiateFunc: startSwiftInstance,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsCeph(keystoneInstance string, opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		Dependencies:    []string{keystoneInstance},
		InstantiateFunc: startCephInstance,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func vlan(ctx context.Context) (*testcontainers.DockerNetwork, error) {
	vlan, err := network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to create docker network: %w", err)
	}
	return vlan, nil
}

func instantiate(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	if _, ok := env.containers[componentName]; ok {
		return nil
	}

	for _, dependencyName := range componentConfig.Dependencies {
		if _, ok := env.containers[dependencyName]; ok {
			continue
		}

		dependencyConfig, ok := env.creationConfigs[dependencyName]
		if !ok {
			return fmt.Errorf("unable to find config for dependency %s", dependencyName)
		}

		if err := instantiate(ctx, env, dependencyName, &dependencyConfig); err != nil {
			return fmt.Errorf("unable to create dependency instance %s: %w", dependencyName, err)
		}
	}

	if err := componentConfig.InstantiateFunc(ctx, env, componentName, componentConfig); err != nil {
		return fmt.Errorf("unable to create instance: %w", err)
	}

	return nil
}

func startSwiftInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	keystoneEnv, err := env.GetKeystoneAccessConfig(componentConfig.Dependencies[0])
	if err != nil {
		return fmt.Errorf("unable to get keystone env: %w", err)
	}

	providerClient, err := openstack.AuthenticatedClient(ctx, gophercloud.AuthOptions{
		IdentityEndpoint: fmt.Sprintf("http://%s:%d/v3", keystoneEnv.Host.Local, keystoneEnv.ExternalPort.Forwarded),
		Username:         keystoneEnv.User,
		Password:         keystoneEnv.Password,
		DomainName:       CKeystoneAdminDomainName,
		TenantName:       CKeystoneAdminTenantName,
	})
	if err != nil {
		return fmt.Errorf("unable to create provider client: %w", err)
	}

	identityClient, err := openstack.NewIdentityV3(providerClient, gophercloud.EndpointOpts{})
	if err != nil {
		return fmt.Errorf("unable to create identity client: %w", err)
	}

	swiftUser, err := users.Create(ctx, identityClient, users.CreateOpts{
		Name:     CKeystoneSwiftUsername,
		Password: CKeystoneSwiftPassword,
		DomainID: keystoneEnv.DefaultDomain.ID,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create user: %w", err)
	}

	var adminRole *roles.Role
	err = roles.List(identityClient, roles.ListOpts{
		Name: CkeystoneAdminRoleName,
	}).EachPage(ctx, func(ctx context.Context, p pagination.Page) (bool, error) {
		roleList, err := roles.ExtractRoles(p)
		if err != nil {
			return false, fmt.Errorf("unable to extract roles: %w", err)
		}
		for _, role := range roleList {
			if role.Name == CkeystoneAdminRoleName {
				adminRole = &role
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("unable to list roles: %w", err)
	}
	if adminRole == nil {
		return errors.New("unable to find admin role")
	}

	if err := roles.Assign(ctx, identityClient, adminRole.ID, roles.AssignOpts{
		UserID:    swiftUser.ID,
		ProjectID: keystoneEnv.ServiceProject.ID,
	}).ExtractErr(); err != nil {
		return fmt.Errorf("unable to add user to admin role: %w", err)
	}

	swiftService, err := services.Create(ctx, identityClient, services.CreateOpts{
		Type: CKeystoneObjectStoreServiceType,
		Extra: map[string]any{
			"name": CKeystoneSwiftServiceName,
		},
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create swift service: %w", err)
	}

	operatorRole, err := roles.Create(ctx, identityClient, roles.CreateOpts{
		Name: CKeystoneSwiftOperatorRole,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create swift operator role: %w", err)
	}

	resellerRole, err := roles.Create(ctx, identityClient, roles.CreateOpts{
		Name: CKeystoneSwiftResellerRole,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create swift reseller role: %w", err)
	}

	swiftProxyConfig := SwiftProxyConfigTemplateValues{
		AuthHost:         keystoneEnv.Host.NAT,
		AdminAuthPort:    keystoneEnv.AdminPort.Exposed,
		ExternalAuthPort: keystoneEnv.ExternalPort.Exposed,
		AdminTenant:      keystoneEnv.ServiceProject.Name,
		AdminDomain:      keystoneEnv.DefaultDomain.ID,
		AdminUser:        swiftUser.Name,
		AdminPassword:    CKeystoneSwiftPassword,
		OperatorRole:     operatorRole.Name,
		ResellerRole:     resellerRole.Name,
	}

	swiftProxyTemplate, err := template.New("proxy-server.conf").Parse(proxyServerConf)
	if err != nil {
		return fmt.Errorf("unable to create swift proxy config template: %w", err)
	}

	templateBuffer := &bytes.Buffer{}

	if err := swiftProxyTemplate.Execute(templateBuffer, &swiftProxyConfig); err != nil {
		return fmt.Errorf("unable to create proxy config out of template: %w", err)
	}

	natPortString := fmt.Sprintf(CNATPortTemplate, CSwiftPort)
	natPort := nat.Port(natPortString)
	req := testcontainers.ContainerRequest{
		Image:      CSwiftImage,
		WaitingFor: wait.ForHTTP("/healthcheck").WithPort(natPort).WithStartupTimeout(5 * time.Minute),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
		},
		ExposedPorts: []string{natPortString},
		Networks:     []string{env.network.Name},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            templateBuffer,
				ContainerFilePath: "/etc/swift/proxy-server.conf",
				FileMode:          0o777,
			},
		},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(1 * time.Second)},
			Consumers: []testcontainers.LogConsumer{NewContainerLogConsumer(componentName, componentConfig.DisabledLogs)},
		},
	}

	container, err := startContainer(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to start swift container: %w", err)
	}

	containerIP, err := container.ContainerIP(ctx)
	if err != nil {
		return fmt.Errorf("unable to get swift host: %w", err)
	}

	containerHost, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("unable to get swift container host: %w", err)
	}

	forwardedPort, err := container.MappedPort(ctx, natPort)
	if err != nil {
		return fmt.Errorf("unable to get swift api forwarded port: %w", err)
	}

	_, err = endpoints.Create(ctx, identityClient, endpoints.CreateOpts{
		Name:         CKeystoneSwiftEndpointName,
		Availability: gophercloud.AvailabilityInternal,
		URL:          fmt.Sprintf(CKeystoneSwiftEndpointURLTemplate, containerHost, forwardedPort.Int()),
		ServiceID:    swiftService.ID,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create internal swift endpoint: %w", err)
	}

	_, err = endpoints.Create(ctx, identityClient, endpoints.CreateOpts{
		Name:         CKeystoneSwiftEndpointName,
		Availability: gophercloud.AvailabilityPublic,
		URL:          fmt.Sprintf(CKeystoneSwiftEndpointURLTemplate, containerHost, forwardedPort.Int()),
		ServiceID:    swiftService.ID,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create public swift endpoint: %w", err)
	}

	env.accessConfigs[componentName] = SwiftAccessConfig{
		Port: ContainerPort{
			Exposed:   CSwiftPort,
			Forwarded: forwardedPort.Int(),
		},
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		Keystone: StorageKeystoneAccessConfig{
			OperatorRole: operatorRole,
			ResellerRole: resellerRole,
		},
	}
	env.containers[componentName] = container

	return nil
}

func startKeystoneInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	adminNATPortString := fmt.Sprintf(CNATPortTemplate, CKeystoneAdminPort)
	externalNATPortString := fmt.Sprintf(CNATPortTemplate, CKeystoneExternalPort)
	adminNATPort := nat.Port(adminNATPortString)
	externalNATPort := nat.Port(externalNATPortString)
	req := testcontainers.ContainerRequest{
		Image:      CKeystoneImage,
		WaitingFor: wait.ForHTTP("/v3").WithPort(externalNATPort),
		Env: map[string]string{
			"ADMIN_TENANT_NAME": CKeystoneAdminTenantName,
			"ADMIN_USERNAME":    CKeystoneAdminUsername,
			"ADMIN_PASSWORD":    CKeystoneAdminPassword,
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
		},
		ExposedPorts: []string{adminNATPortString, externalNATPortString},
		Networks:     []string{env.network.Name},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(1 * time.Second)},
			Consumers: []testcontainers.LogConsumer{NewContainerLogConsumer(componentName, componentConfig.DisabledLogs)},
		},
	}

	container, err := startContainer(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to start keystone container: %w", err)
	}

	containerIP, err := container.ContainerIP(ctx)
	if err != nil {
		return fmt.Errorf("unable to get keystone host: %w", err)
	}

	containerHost, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("unable to get keystone container host: %w", err)
	}

	adminForwardedPort, err := container.MappedPort(ctx, adminNATPort)
	if err != nil {
		return fmt.Errorf("unable to get keystone api forwarded port: %w", err)
	}

	externalForwardedPort, err := container.MappedPort(ctx, externalNATPort)
	if err != nil {
		return fmt.Errorf("unable to get keystone api forwarded port: %w", err)
	}

	providerClient, err := openstack.AuthenticatedClient(ctx, gophercloud.AuthOptions{
		IdentityEndpoint: fmt.Sprintf("http://%s:%d/v3", containerHost, externalForwardedPort.Int()),
		Username:         CKeystoneAdminUsername,
		Password:         CKeystoneAdminPassword,
		DomainName:       CKeystoneAdminDomainName,
		TenantName:       CKeystoneAdminTenantName,
	})
	if err != nil {
		return fmt.Errorf("unable to create provider client: %w", err)
	}

	identityClient, err := openstack.NewIdentityV3(providerClient, gophercloud.EndpointOpts{})
	if err != nil {
		return fmt.Errorf("unable to create identity client: %w", err)
	}

	var defaultDomain *domains.Domain
	err = domains.List(identityClient, domains.ListOpts{
		Name: CKeystoneAdminDomainName,
	}).EachPage(ctx, func(ctx context.Context, p pagination.Page) (bool, error) {
		domainList, err := domains.ExtractDomains(p)
		if err != nil {
			return false, fmt.Errorf("unable to extract domains: %w", err)
		}
		for _, domain := range domainList {
			if domain.Name == CKeystoneAdminDomainName {
				defaultDomain = &domain
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("unable to list roles: %w", err)
	}
	if defaultDomain == nil {
		return errors.New("unable to find admin domain")
	}

	serviceProject, err := projects.Create(ctx, identityClient, projects.CreateOpts{
		DomainID: defaultDomain.ID,
		Name:     CKeystoneServiceProjectName,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create project: %w", err)
	}

	env.accessConfigs[componentName] = KeystoneAccessConfig{
		AdminPort: ContainerPort{
			Exposed:   CKeystoneAdminPort,
			Forwarded: adminForwardedPort.Int(),
		},
		ExternalPort: ContainerPort{
			Exposed:   CKeystoneExternalPort,
			Forwarded: externalForwardedPort.Int(),
		},
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		User:           CKeystoneAdminUsername,
		Password:       CKeystoneAdminPassword,
		DefaultDomain:  defaultDomain,
		TenantName:     CKeystoneAdminTenantName,
		ServiceProject: serviceProject,
	}
	env.containers[componentName] = container

	return nil
}

func startRedisInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	natPortString := fmt.Sprintf(CNATPortTemplate, CRedisPort)
	natPort := nat.Port(natPortString)
	req := testcontainers.ContainerRequest{
		Image:      CRedisImage,
		Cmd:        []string{"/bin/sh", "-c", "redis-server", "--requirepass", CRedisPassword},
		WaitingFor: wait.ForExec([]string{"redis-cli", "ping"}),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
		},
		ExposedPorts: []string{natPortString},
		Networks:     []string{env.network.Name},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(1 * time.Second)},
			Consumers: []testcontainers.LogConsumer{NewContainerLogConsumer(componentName, componentConfig.DisabledLogs)},
		},
	}

	container, err := startContainer(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to start redis container: %w", err)
	}

	containerIP, err := container.ContainerIP(ctx)
	if err != nil {
		return fmt.Errorf("unable to get redis host: %w", err)
	}

	containerHost, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("unable to get redis container host: %w", err)
	}

	forwardedPort, err := container.MappedPort(ctx, natPort)
	if err != nil {
		return fmt.Errorf("unable to get redis api forwarded port: %w", err)
	}

	env.accessConfigs[componentName] = RedisAccessConfig{
		Port: ContainerPort{
			Exposed:   CRedisPort,
			Forwarded: forwardedPort.Int(),
		},
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		Password: CRedisPassword,
	}
	env.containers[componentName] = container

	return nil
}

func startMinioInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	s3NATPortString := fmt.Sprintf(CNATPortTemplate, CMinioS3Port)
	s3NATPort := nat.Port(s3NATPortString)
	managementNATPortString := fmt.Sprintf(CNATPortTemplate, CMinioManagementPort)
	managementNATPort := nat.Port(managementNATPortString)
	req := testcontainers.ContainerRequest{
		Image:      CMinioImage,
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort(s3NATPort),
		Cmd:        []string{"server", "/data", "--address", fmt.Sprintf(":%d", CMinioS3Port), "--console-address", fmt.Sprintf(":%d", CMinioManagementPort)},
		Env: map[string]string{
			"MINIO_ROOT_USER":     CMinioUsername,
			"MINIO_ROOT_PASSWORD": CMinioPassword,
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
		},
		ExposedPorts: []string{s3NATPortString, managementNATPortString},
		Networks:     []string{env.network.Name},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(1 * time.Second)},
			Consumers: []testcontainers.LogConsumer{NewContainerLogConsumer(componentName, componentConfig.DisabledLogs)},
		},
	}

	container, err := startContainer(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to start minio container: %w", err)
	}

	containerIP, err := container.ContainerIP(ctx)
	if err != nil {
		return fmt.Errorf("unable to get minio host: %w", err)
	}

	containerHost, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("unable to get minio container host: %w", err)
	}

	s3ForwardedPort, err := container.MappedPort(ctx, s3NATPort)
	if err != nil {
		return fmt.Errorf("unable to get minio api forwarded port: %w", err)
	}

	managementForwardedPort, err := container.MappedPort(ctx, managementNATPort)
	if err != nil {
		return fmt.Errorf("unable to get minio api forwarded port: %w", err)
	}

	env.accessConfigs[componentName] = MinioAccessConfig{
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		S3Port: ContainerPort{
			Exposed:   CMinioS3Port,
			Forwarded: s3ForwardedPort.Int(),
		},
		ManagementPort: ContainerPort{
			Exposed:   CMinioManagementPort,
			Forwarded: managementForwardedPort.Int(),
		},
		User:     CMinioUsername,
		Password: CMinioPassword,
	}
	env.containers[componentName] = container

	return nil
}

func startCephInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	keystoneEnv, err := env.GetKeystoneAccessConfig(componentConfig.Dependencies[0])
	if err != nil {
		return fmt.Errorf("unable to get keystone env: %w", err)
	}

	providerClient, err := openstack.AuthenticatedClient(ctx, gophercloud.AuthOptions{
		IdentityEndpoint: fmt.Sprintf("http://%s:%d/v3", keystoneEnv.Host.Local, keystoneEnv.ExternalPort.Forwarded),
		Username:         keystoneEnv.User,
		Password:         keystoneEnv.Password,
		DomainName:       CKeystoneAdminDomainName,
		TenantName:       CKeystoneAdminTenantName,
	})
	if err != nil {
		return fmt.Errorf("unable to create provider client: %w", err)
	}

	identityClient, err := openstack.NewIdentityV3(providerClient, gophercloud.EndpointOpts{})
	if err != nil {
		return fmt.Errorf("unable to create identity client: %w", err)
	}

	cephUser, err := users.Create(ctx, identityClient, users.CreateOpts{
		Name:     CKeystoneCephUsername,
		Password: CKeystoneCephPassword,
		DomainID: keystoneEnv.DefaultDomain.ID,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create user: %w", err)
	}

	var adminRole *roles.Role
	err = roles.List(identityClient, roles.ListOpts{
		Name: CkeystoneAdminRoleName,
	}).EachPage(ctx, func(ctx context.Context, p pagination.Page) (bool, error) {
		roleList, err := roles.ExtractRoles(p)
		if err != nil {
			return false, fmt.Errorf("unable to extract roles: %w", err)
		}
		for _, role := range roleList {
			if role.Name == CkeystoneAdminRoleName {
				adminRole = &role
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("unable to list roles: %w", err)
	}
	if adminRole == nil {
		return errors.New("unable to find admin role")
	}

	if err := roles.Assign(ctx, identityClient, adminRole.ID, roles.AssignOpts{
		UserID:    cephUser.ID,
		ProjectID: keystoneEnv.ServiceProject.ID,
	}).ExtractErr(); err != nil {
		return fmt.Errorf("unable to add user to admin role: %w", err)
	}

	cephService, err := services.Create(ctx, identityClient, services.CreateOpts{
		Type: CKeystoneObjectStoreServiceType,
		Extra: map[string]any{
			"name": CKeystoneCephServiceName,
		},
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create ceph service: %w", err)
	}

	operatorRole, err := roles.Create(ctx, identityClient, roles.CreateOpts{
		Name: CKeystoneCephOperatorRole,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create ceph operator role: %w", err)
	}

	resellerRole, err := roles.Create(ctx, identityClient, roles.CreateOpts{
		Name: CKeystoneCephResellerRole,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create ceph reseller role: %w", err)
	}

	cephRGWConfig := CephRGWTemplateValues{
		AuthHost:         keystoneEnv.Host.NAT,
		ExternalAuthPort: keystoneEnv.ExternalPort.Exposed,
		AdminProject:     keystoneEnv.ServiceProject.Name,
		AdminDomain:      keystoneEnv.DefaultDomain.Name,
		AdminUser:        cephUser.Name,
		AdminPassword:    CKeystoneCephPassword,
		OperatorRole:     operatorRole.Name,
		ResellerRole:     resellerRole.Name,
	}

	cephRGWConfigTemplate, err := template.New("ceph-entrypoint.sh").Parse(cephEntrypointSh)
	if err != nil {
		return fmt.Errorf("unable to create ceph proxy config template: %w", err)
	}

	templateBuffer := &bytes.Buffer{}

	if err := cephRGWConfigTemplate.Execute(templateBuffer, &cephRGWConfig); err != nil {
		return fmt.Errorf("unable to create proxy config out of template: %w", err)
	}

	var imageName string
	switch runtime.GOARCH {
	case "amd64":
		imageName = CCephX8664Image
	case "arm64":
		imageName = CCephARM64Image
	default:
		return fmt.Errorf("platform %s not supported for ceph image", runtime.GOARCH)
	}

	apiNatPortString := fmt.Sprintf(CNATPortTemplate, CCephAPIPort)
	apiNatPort := nat.Port(apiNatPortString)
	req := testcontainers.ContainerRequest{
		Image:      imageName,
		WaitingFor: wait.ForHTTP("/").WithStartupTimeout(5 * time.Minute).WithPort(apiNatPort),
		Env: map[string]string{
			"RGW_NAME":            CCephDomainName,
			"CEPH_PUBLIC_NETWORK": CCephPublicNetwork,
			"MON_IP":              CCephMonIP,
			"CEPH_DEMO_UID":       CCephDemoUID,
			"OSD_TYPE":            CCephOSDDirectoryMode,
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
		},
		Entrypoint:      []string{"/bin/bash", "/entrypoint.sh"},
		Cmd:             []string{"/bin/bash", "/opt/ceph-container/bin/demo"},
		HostAccessPorts: []int{CCephAPIPort},
		ExposedPorts:    []string{apiNatPortString},
		Networks:        []string{env.network.Name},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            templateBuffer,
				ContainerFilePath: "/entrypoint.sh",
				FileMode:          0o777,
			},
		},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(1 * time.Second)},
			Consumers: []testcontainers.LogConsumer{NewContainerLogConsumer(componentName, componentConfig.DisabledLogs)},
		},
	}

	container, err := startContainer(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to start ceph container: %w", err)
	}

	containerIP, err := container.ContainerIP(ctx)
	if err != nil {
		return fmt.Errorf("unable to get ceph container ip: %w", err)
	}

	containerHost, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("unable to get ceph container host: %w", err)
	}

	apiForwardedPort, err := container.MappedPort(ctx, apiNatPort)
	if err != nil {
		return fmt.Errorf("unable to get ceph api forwarded port: %w", err)
	}

	_, err = endpoints.Create(ctx, identityClient, endpoints.CreateOpts{
		Name:         CKeystoneCephEndpointName,
		Availability: gophercloud.AvailabilityInternal,
		URL:          fmt.Sprintf(CKeystoneCephEndpointURLTemplate, containerHost, apiForwardedPort.Int()),
		ServiceID:    cephService.ID,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create internal ceph endpoint: %w", err)
	}

	_, err = endpoints.Create(ctx, identityClient, endpoints.CreateOpts{
		Name:         CKeystoneCephEndpointName,
		Availability: gophercloud.AvailabilityPublic,
		URL:          fmt.Sprintf(CKeystoneCephEndpointURLTemplate, containerHost, apiForwardedPort.Int()),
		ServiceID:    cephService.ID,
	}).Extract()
	if err != nil {
		return fmt.Errorf("unable to create public ceph endpoint: %w", err)
	}

	env.accessConfigs[componentName] = CephAccessConfig{
		Port: ContainerPort{
			Exposed:   CCephAPIPort,
			Forwarded: apiForwardedPort.Int(),
		},
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		Keystone: StorageKeystoneAccessConfig{
			OperatorRole: operatorRole,
			ResellerRole: resellerRole,
		},
	}
	env.containers[componentName] = container

	return nil
}

func startContainer(ctx context.Context, req testcontainers.ContainerRequest) (testcontainers.Container, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create container: %w", err)
	}
	return container, nil
}
