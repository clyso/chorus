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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/domains"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/endpoints"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/roles"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/services"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/users"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func init() {
	// Disable HTTP keep-alive for the default transport.
	// Keystone's uwsgi http-socket mode closes connections between requests,
	// causing EOF errors when Go's HTTP client reuses pooled connections.
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.DisableKeepAlives = true
	}
}

const (
	CRedisImage    = "redis:8.4.0-alpine"
	CRedisPort     = 6379
	CRedisPassword = "password"

	CKeystoneImage                    = "ghcr.io/aiivashchenko/docker-keystone:28.0.0"
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

	CGoFakeS3EC2AccessToken = "a7f1e798b7c2417cba4a02de97dc3cdc"
	CGoFakeS3EC2SecretToken = "18f4f6761ada4e3795fa5273c30349b9"

	CSwiftImage = "ghcr.io/aiivashchenko/docker-swift:2.37.0"
	CSwiftPort  = 8080

	CMinioImage          = "minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1"
	CMinioUsername       = "minioadmin"
	CMinioPassword       = "minioadmin"
	CMinioS3Port         = 9000
	CMinioManagementPort = 9001

	CGarageImage             = "dxflrs/garage:v1.0.1"
	CGarageReplicationFactor = 1
	CGarageRPCSecret         = "d90f9a1e0d5c4a5d8f1b7c3e6a2d4b8f0c1e3a5d7b9f2c4e6a8d0b1f3c5e7a9d"
	CGarageS3Port            = 3900
	CGarageRPCPort           = 3901
	CGarageAdminPort         = 3903
	CGarageRegion            = "garage"
	CGarageZone              = "dc1"
	CGarageKeyPermPath       = "/v1/key"
	CGarageCapacityBytes     = 1_000_000_000
	CGarageKeyName           = "chorus"
	CGarageAccessToken       = "GK0123456789abcdef01234567"
	CGarageSecretToken       = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	CGarageAdminToken        = "garage-admin-token"
	CGarageConfigPath        = "/etc/garage.toml"

	CSeaweedFSImage        = "chrislusf/seaweedfs:3.80"
	CSeaweedFSS3Port       = 8333
	CSeaweedFSMasterPort   = 9333
	CSeaweedFSFilerPort    = 8888
	CSeaweedFSIdentityName = "chorus"
	CSeaweedFSAccessToken  = "seaweedfsaccesskey"
	CSeaweedFSSecretToken  = "seaweedfssecretkey"
	CSeaweedFSS3ConfigPath = "/etc/seaweedfs/s3.json"

	CCephImage                 = "ghcr.io/arttor/ceph-test:v19"
	CCephDemoUID               = "cephuid"
	CCephSystemUserUID         = "superuser"
	CCephSystemUserDisplayName = "superuser"
	CCephSystemUserAccessToken = "superuser"
	CCephSystemUserSecretToken = "superuser"
	CCephAPIPort               = 8080

	CContainerStopDeadline = 10 * time.Second

	CNATPortTemplate = "%d/tcp"
)

var (
	//go:embed proxy-server.conf
	proxyServerConf string
	//go:embed ceph-keystone.conf
	cephKeystoneConf string
	//go:embed garage.toml
	garageConf string
	//go:embed seaweedfs-s3.json
	seaweedFSS3Conf string
)

// ComponentOption interface to support Functional options for test-containers.
type ComponentOption interface {
	apply(*ComponentCreationConfig)
}

type disableContainerSTDOut struct{}

func (r *disableContainerSTDOut) apply(cfg *ComponentCreationConfig) {
	cfg.DisabledLogs = append(cfg.DisabledLogs, testcontainers.StdoutLog)
}

type disableContainerSTDErr struct{}

func (r *disableContainerSTDErr) apply(cfg *ComponentCreationConfig) {
	cfg.DisabledLogs = append(cfg.DisabledLogs, testcontainers.StderrLog)
}

type addDependency struct {
	instanceName string
}

func (r *addDependency) apply(cfg *ComponentCreationConfig) {
	cfg.Dependencies = append(cfg.Dependencies, r.instanceName)
}

// WithDisabledSTDOutLog disables STDOUT logs for the container.
func WithDisabledSTDOutLog() ComponentOption {
	return &disableContainerSTDOut{}
}

// WithDisabledSTDErrLog disables STDERR logs for the container.
func WithDisabledSTDErrLog() ComponentOption {
	return &disableContainerSTDErr{}
}

func WithKeystone(instanceName string) ComponentOption {
	return &addDependency{
		instanceName: instanceName,
	}
}

type hostAccessPorts struct {
	ports []int
}

func (r *hostAccessPorts) apply(cfg *ComponentCreationConfig) {
	cfg.HostAccessPorts = append(cfg.HostAccessPorts, r.ports...)
}

func WithHostAccessPorts(ports ...int) ComponentOption {
	return &hostAccessPorts{ports: ports}
}

type ContainerLogConsumer struct {
	disabledLogs  map[string]struct{}
	componentName string
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
	InstantiateFunc func(context.Context, *TestEnvironment, string, *ComponentCreationConfig) error
	Dependencies    []string
	DisabledLogs    []string
	HostAccessPorts []int
	Container       bool
}

type RedisAccessConfig struct {
	Host     ContainerHost
	Password string
	Port     ContainerPort
}

type MiniRedisAccessConfig struct {
	Host     string
	Password string
	Port     int
}

type GoFakeS3AccessConfig struct {
	Host        string
	AccessToken string
	SecretToken string
	Port        int
}

type KeystoneAccessConfig struct {
	DefaultDomain  *domains.Domain
	ServiceProject *projects.Project
	Host           ContainerHost
	User           string
	Password       string
	TenantName     string
	ExternalPort   ContainerPort
	AdminPort      ContainerPort
}

type StorageKeystoneAccessConfig struct {
	OperatorRole *roles.Role
	ResellerRole *roles.Role
	EndpointName string
	ServiceType  string
}

type SwiftAccessConfig struct {
	Keystone StorageKeystoneAccessConfig
	Host     ContainerHost
	Port     ContainerPort
}

type MinioAccessConfig struct {
	Host           ContainerHost
	User           string
	Password       string
	S3Port         ContainerPort
	ManagementPort ContainerPort
}

type GarageAccessConfig struct {
	Host        ContainerHost
	Region      string
	AccessToken string
	SecretToken string
	AdminToken  string
	S3Port      ContainerPort
	AdminPort   ContainerPort
}

type SeaweedFSAccessConfig struct {
	Host        ContainerHost
	AccessToken string
	SecretToken string
	S3Port      ContainerPort
	MasterPort  ContainerPort
}

type CephAccessConfig struct {
	Keystone       StorageKeystoneAccessConfig
	Host           ContainerHost
	SystemUser     string
	SystemPassword string
	Port           ContainerPort
}

type SwiftProxyConfigTemplateValues struct {
	AuthHost         string
	AdminTenant      string
	AdminDomain      string
	AdminUser        string
	AdminPassword    string
	OperatorRole     string
	ResellerRole     string
	AdminAuthPort    int
	ExternalAuthPort int
}

type GarageConfigTemplateValues struct {
	RPCSecret         string
	AdminToken        string
	Region            string
	ReplicationFactor int
	RPCPort           int
	S3Port            int
	AdminPort         int
}

type SeaweedFSS3ConfigTemplateValues struct {
	Name        string
	AccessToken string
	SecretToken string
}

type CephRGWTemplateValues struct {
	AuthHost         string
	AdminProject     string
	AdminDomain      string
	AdminUser        string
	AdminPassword    string
	OperatorRole     string
	ResellerRole     string
	ExternalAuthPort int
	WithKeystone     bool
}

type Terminator func(context.Context) error

type TestEnvironment struct {
	creationConfigs map[string]ComponentCreationConfig
	network         *testcontainers.DockerNetwork
	accessConfigs   map[string]any
	terminators     map[string]Terminator
}

func NewTestEnvironment(ctx context.Context, envConfig map[string]ComponentCreationConfig) (*TestEnvironment, error) {
	configLen := len(envConfig)
	env := &TestEnvironment{
		creationConfigs: envConfig,
		accessConfigs:   make(map[string]any, configLen),
		terminators:     make(map[string]Terminator, configLen),
	}

	for componentName, componentConfig := range envConfig {
		if err := instantiate(ctx, env, componentName, &componentConfig); err != nil {
			return nil, fmt.Errorf("unable to create component instance %s: %w", componentName, err)
		}
	}

	go func() {
		<-ctx.Done()
		_ = env.Terminate(ctx)
	}()

	return env, nil
}

func (r *TestEnvironment) Terminate(ctx context.Context) error {
	for name, terminate := range r.terminators {
		if err := terminate(ctx); err != nil {
			return fmt.Errorf("unable to stop component %s: %w", name, err)
		}
	}
	if r.network == nil {
		return nil
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

func (r *TestEnvironment) GetMiniRedisAccessConfig(instanceName string) (*MiniRedisAccessConfig, error) {
	instanceAccessCfg, ok := r.accessConfigs[instanceName]
	if !ok {
		return nil, fmt.Errorf("unable to find instance %s", instanceName)
	}
	miniRedisAccessCfg, ok := instanceAccessCfg.(MiniRedisAccessConfig)
	if !ok {
		return nil, fmt.Errorf("unable to cast instance access cfg %s to mini redis access cfg", instanceName)
	}
	return &miniRedisAccessCfg, nil
}

func (r *TestEnvironment) GetGoFakeS3AccessConfig(instanceName string) (*GoFakeS3AccessConfig, error) {
	instanceAccessCfg, ok := r.accessConfigs[instanceName]
	if !ok {
		return nil, fmt.Errorf("unable to find instance %s", instanceName)
	}
	goFakeS3AccessCfg, ok := instanceAccessCfg.(GoFakeS3AccessConfig)
	if !ok {
		return nil, fmt.Errorf("unable to cast instance access cfg %s to go fake s3 access cfg", instanceName)
	}
	return &goFakeS3AccessCfg, nil
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

func (r *TestEnvironment) GetGarageAccessConfig(instanceName string) (*GarageAccessConfig, error) {
	instanceAccessCfg, ok := r.accessConfigs[instanceName]
	if !ok {
		return nil, fmt.Errorf("unable to find instance %s", instanceName)
	}
	garageAccessCfg, ok := instanceAccessCfg.(GarageAccessConfig)
	if !ok {
		return nil, fmt.Errorf("unable to cast instance access cfg %s to garage access cfg", instanceName)
	}
	return &garageAccessCfg, nil
}

func (r *TestEnvironment) GetSeaweedFSAccessConfig(instanceName string) (*SeaweedFSAccessConfig, error) {
	instanceAccessCfg, ok := r.accessConfigs[instanceName]
	if !ok {
		return nil, fmt.Errorf("unable to find instance %s", instanceName)
	}
	seaweedFSAccessCfg, ok := instanceAccessCfg.(SeaweedFSAccessConfig)
	if !ok {
		return nil, fmt.Errorf("unable to cast instance access cfg %s to seaweedfs access cfg", instanceName)
	}
	return &seaweedFSAccessCfg, nil
}

func AsGarage(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startGarageInstance,
		Container:       true,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsSeaweedFS(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startSeaweedFSInstance,
		Container:       true,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsMinio(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startMinioInstance,
		Container:       true,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsRedis(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startRedisInstance,
		Container:       true,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsMiniRedis(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startMiniRedisInstance,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsGoFakeS3(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startGoFakeS3instance,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsKeystone(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startKeystoneInstance,
		Container:       true,
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
		Container:       true,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

func AsCeph(opts ...ComponentOption) ComponentCreationConfig {
	cfg := ComponentCreationConfig{
		InstantiateFunc: startCephInstance,
		Container:       true,
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
	if _, ok := env.terminators[componentName]; ok {
		return nil
	}

	if componentConfig.Container && env.network == nil {
		vlan, err := vlan(ctx)
		if err != nil {
			return fmt.Errorf("unable to create env network: %w", err)
		}
		env.network = vlan
	}

	for _, dependencyName := range componentConfig.Dependencies {
		if _, ok := env.terminators[dependencyName]; ok {
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
	natPort := natPortString
	req := testcontainers.ContainerRequest{
		Image:      CSwiftImage,
		WaitingFor: wait.ForHTTP("/healthcheck").WithPort(natPort).WithStartupTimeout(5 * time.Minute),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
			// swift.common.utils.checksum opens an AF_ALG socket at import,
			// which the default seccomp profile denies.
			hc.SecurityOpt = []string{"seccomp=unconfined"}
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

	closeIdleConns()

	if err := retryOnTransient(ctx, 3, time.Second, func() error {
		_, err := endpoints.Create(ctx, identityClient, endpoints.CreateOpts{
			Name:         CKeystoneSwiftEndpointName,
			Availability: gophercloud.AvailabilityInternal,
			URL:          fmt.Sprintf(CKeystoneSwiftEndpointURLTemplate, containerHost, forwardedPort.Num()),
			ServiceID:    swiftService.ID,
		}).Extract()
		return err
	}); err != nil {
		return fmt.Errorf("unable to create internal swift endpoint: %w", err)
	}

	if err := retryOnTransient(ctx, 3, time.Second, func() error {
		_, err := endpoints.Create(ctx, identityClient, endpoints.CreateOpts{
			Name:         CKeystoneSwiftEndpointName,
			Availability: gophercloud.AvailabilityPublic,
			URL:          fmt.Sprintf(CKeystoneSwiftEndpointURLTemplate, containerHost, forwardedPort.Num()),
			ServiceID:    swiftService.ID,
		}).Extract()
		return err
	}); err != nil {
		return fmt.Errorf("unable to create public swift endpoint: %w", err)
	}

	env.accessConfigs[componentName] = SwiftAccessConfig{
		Port: ContainerPort{
			Exposed:   CSwiftPort,
			Forwarded: int(forwardedPort.Num()),
		},
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		Keystone: StorageKeystoneAccessConfig{
			EndpointName: CKeystoneSwiftEndpointName,
			ServiceType:  CKeystoneObjectStoreServiceType,
			OperatorRole: operatorRole,
			ResellerRole: resellerRole,
		},
	}
	env.terminators[componentName] = func(ctx context.Context) error {
		return stopContainer(ctx, container)
	}

	return nil
}

func startKeystoneInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	adminNATPortString := fmt.Sprintf(CNATPortTemplate, CKeystoneAdminPort)
	externalNATPortString := fmt.Sprintf(CNATPortTemplate, CKeystoneExternalPort)
	adminNATPort := adminNATPortString
	externalNATPort := externalNATPortString
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
		IdentityEndpoint: fmt.Sprintf("http://%s:%d/v3", containerHost, externalForwardedPort.Num()),
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
			Forwarded: int(adminForwardedPort.Num()),
		},
		ExternalPort: ContainerPort{
			Exposed:   CKeystoneExternalPort,
			Forwarded: int(externalForwardedPort.Num()),
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
	env.terminators[componentName] = func(ctx context.Context) error {
		return stopContainer(ctx, container)
	}

	return nil
}

func startRedisInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	natPortString := fmt.Sprintf(CNATPortTemplate, CRedisPort)
	natPort := natPortString
	req := testcontainers.ContainerRequest{
		Image: CRedisImage,
		Cmd:   []string{"redis-server", "--save", "\"\"", "--appendonly", "no", "--requirepass", CRedisPassword},
		WaitingFor: wait.ForAll(
			wait.ForExec([]string{"redis-cli", "-a", CRedisPassword, "ping"}),
			wait.ForListeningPort(natPort),
		),
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
			Forwarded: int(forwardedPort.Num()),
		},
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		Password: CRedisPassword,
	}
	env.terminators[componentName] = func(ctx context.Context) error {
		return stopContainer(ctx, container)
	}

	return nil
}

func startMiniRedisInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	miniRedis := miniredis.NewMiniRedis()
	miniRedis.RequireAuth(CRedisPassword)

	if err := miniRedis.Start(); err != nil {
		return fmt.Errorf("unable to start miniredis: %w", err)
	}

	go func() {
		<-ctx.Done()
		miniRedis.Close()
	}()

	portString := miniRedis.Port()
	port, err := strconv.Atoi(portString)
	if err != nil {
		return fmt.Errorf("unable to convert port string %s to int: %w", portString, err)
	}

	host := miniRedis.Host()

	env.accessConfigs[componentName] = MiniRedisAccessConfig{
		Port:     port,
		Host:     host,
		Password: CRedisPassword,
	}

	env.terminators[componentName] = func(ctx context.Context) error {
		miniRedis.Close()
		return nil
	}

	return nil
}

func startGoFakeS3instance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	fakeS3Backend := s3mem.New()
	fakeS3 := gofakes3.New(fakeS3Backend)
	fakeS3Server := httptest.NewServer(fakeS3.Server())
	url, err := url.Parse(fakeS3Server.URL)
	if err != nil {
		return fmt.Errorf("unable to parse fake s3 url: %w", err)
	}

	portString := url.Port()
	port, err := strconv.Atoi(portString)
	if err != nil {
		return fmt.Errorf("unable to convert port string %s to int: %w", portString, err)
	}

	env.accessConfigs[componentName] = GoFakeS3AccessConfig{
		Port:        port,
		Host:        url.Host,
		AccessToken: CGoFakeS3EC2AccessToken,
		SecretToken: CGoFakeS3EC2SecretToken,
	}

	env.terminators[componentName] = func(ctx context.Context) error {
		fakeS3Server.Close()
		return nil
	}

	return nil
}

func startMinioInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	s3NATPortString := fmt.Sprintf(CNATPortTemplate, CMinioS3Port)
	s3NATPort := s3NATPortString
	managementNATPortString := fmt.Sprintf(CNATPortTemplate, CMinioManagementPort)
	managementNATPort := managementNATPortString
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
			Forwarded: int(s3ForwardedPort.Num()),
		},
		ManagementPort: ContainerPort{
			Exposed:   CMinioManagementPort,
			Forwarded: int(managementForwardedPort.Num()),
		},
		User:     CMinioUsername,
		Password: CMinioPassword,
	}
	env.terminators[componentName] = func(ctx context.Context) error {
		return stopContainer(ctx, container)
	}

	return nil
}

type garageAdmin struct {
	baseURL string
	token   string
}

func (r garageAdmin) do(ctx context.Context, method string, path string, body any, out any) error {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("unable to encode %s %s body: %w", method, path, err)
		}
		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, r.baseURL+path, payload)
	if err != nil {
		return fmt.Errorf("unable to create %s %s request: %w", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("unable to perform %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unable to read %s %s response: %w", method, path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s returned %d: %s", method, path, resp.StatusCode, string(responseBody))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(responseBody, out); err != nil {
		return fmt.Errorf("unable to decode %s %s response %s: %w", method, path, string(responseBody), err)
	}
	return nil
}

type garageStatus struct {
	Node string `json:"node"`
}

type garageRole struct {
	ID       string   `json:"id"`
	Zone     string   `json:"zone"`
	Tags     []string `json:"tags"`
	Capacity int64    `json:"capacity"`
}

type garageHealth struct {
	StorageNodesOk int `json:"storageNodesOk"`
}

func waitForGarageStorage(ctx context.Context, admin garageAdmin, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		var health garageHealth
		if lastErr = admin.do(ctx, http.MethodGet, "/v1/health", nil, &health); lastErr == nil {
			if health.StorageNodesOk > 0 {
				return nil
			}
			lastErr = fmt.Errorf("no storage node is ready yet")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("garage storage did not become ready in %s: %w", timeout, lastErr)
}

func s3GatewayReady(port string, timeout time.Duration) wait.Strategy {
	return wait.ForHTTP("/").
		WithPort(port).
		WithStatusCodeMatcher(func(status int) bool { return status < http.StatusInternalServerError }).
		WithStartupTimeout(timeout)
}

func startGarageInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	s3NATPortString := fmt.Sprintf(CNATPortTemplate, CGarageS3Port)
	s3NATPort := s3NATPortString
	adminNATPortString := fmt.Sprintf(CNATPortTemplate, CGarageAdminPort)
	adminNATPort := adminNATPortString
	rpcNATPortString := fmt.Sprintf(CNATPortTemplate, CGarageRPCPort)

	garageTemplate, err := template.New("garage.toml").Parse(garageConf)
	if err != nil {
		return fmt.Errorf("unable to parse garage config template: %w", err)
	}
	templateBuffer := &bytes.Buffer{}
	if err := garageTemplate.Execute(templateBuffer, &GarageConfigTemplateValues{
		RPCSecret:         CGarageRPCSecret,
		AdminToken:        CGarageAdminToken,
		Region:            CGarageRegion,
		ReplicationFactor: CGarageReplicationFactor,
		RPCPort:           CGarageRPCPort,
		S3Port:            CGarageS3Port,
		AdminPort:         CGarageAdminPort,
	}); err != nil {
		return fmt.Errorf("unable to render garage config: %w", err)
	}

	req := testcontainers.ContainerRequest{
		Image:      CGarageImage,
		WaitingFor: wait.ForHTTP("/health").WithPort(adminNATPort).WithStartupTimeout(2 * time.Minute),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
		},
		HostAccessPorts: append([]int{CGarageS3Port}, componentConfig.HostAccessPorts...),
		ExposedPorts:    []string{s3NATPortString, adminNATPortString, rpcNATPortString},
		Networks:        []string{env.network.Name},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            templateBuffer,
				ContainerFilePath: CGarageConfigPath,
				FileMode:          0o644,
			},
		},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(1 * time.Second)},
			Consumers: []testcontainers.LogConsumer{NewContainerLogConsumer(componentName, componentConfig.DisabledLogs)},
		},
	}

	container, err := startContainer(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to start garage container: %w", err)
	}

	containerIP, err := container.ContainerIP(ctx)
	if err != nil {
		return fmt.Errorf("unable to get garage container ip: %w", err)
	}

	containerHost, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("unable to get garage container host: %w", err)
	}

	s3ForwardedPort, err := container.MappedPort(ctx, s3NATPort)
	if err != nil {
		return fmt.Errorf("unable to get garage s3 forwarded port: %w", err)
	}

	adminForwardedPort, err := container.MappedPort(ctx, adminNATPort)
	if err != nil {
		return fmt.Errorf("unable to get garage admin forwarded port: %w", err)
	}

	admin := garageAdmin{
		baseURL: fmt.Sprintf("http://%s:%d", containerHost, adminForwardedPort.Num()),
		token:   CGarageAdminToken,
	}

	var status garageStatus
	if err := admin.do(ctx, http.MethodGet, "/v1/status", nil, &status); err != nil {
		return fmt.Errorf("unable to get garage status: %w", err)
	}
	if status.Node == "" {
		return errors.New("garage reported an empty node id")
	}

	if err := admin.do(ctx, http.MethodPost, "/v1/layout", []garageRole{{
		ID:       status.Node,
		Zone:     CGarageZone,
		Capacity: CGarageCapacityBytes,
		Tags:     []string{},
	}}, nil); err != nil {
		return fmt.Errorf("unable to stage garage layout: %w", err)
	}

	if err := admin.do(ctx, http.MethodPost, "/v1/layout/apply",
		map[string]int{"version": 1}, nil); err != nil {
		return fmt.Errorf("unable to apply garage layout: %w", err)
	}

	if err := admin.do(ctx, http.MethodPost, "/v1/key/import", map[string]string{
		"accessKeyId":     CGarageAccessToken,
		"secretAccessKey": CGarageSecretToken,
		"name":            CGarageKeyName,
	}, nil); err != nil {
		return fmt.Errorf("unable to import garage key: %w", err)
	}

	if err := admin.do(ctx, http.MethodPost,
		fmt.Sprintf("%s?id=%s", CGarageKeyPermPath, CGarageAccessToken),
		map[string]any{"allow": map[string]bool{"createBucket": true}}, nil); err != nil {
		return fmt.Errorf("unable to allow garage key to create buckets: %w", err)
	}

	if err := waitForGarageStorage(ctx, admin, time.Minute); err != nil {
		return err
	}

	env.accessConfigs[componentName] = GarageAccessConfig{
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		S3Port: ContainerPort{
			Exposed:   CGarageS3Port,
			Forwarded: int(s3ForwardedPort.Num()),
		},
		AdminPort: ContainerPort{
			Exposed:   CGarageAdminPort,
			Forwarded: int(adminForwardedPort.Num()),
		},
		Region:      CGarageRegion,
		AccessToken: CGarageAccessToken,
		SecretToken: CGarageSecretToken,
		AdminToken:  CGarageAdminToken,
	}
	env.terminators[componentName] = func(ctx context.Context) error {
		return stopContainer(ctx, container)
	}

	return nil
}

func startSeaweedFSInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	s3NATPortString := fmt.Sprintf(CNATPortTemplate, CSeaweedFSS3Port)
	s3NATPort := s3NATPortString
	masterNATPortString := fmt.Sprintf(CNATPortTemplate, CSeaweedFSMasterPort)
	masterNATPort := masterNATPortString
	filerNATPortString := fmt.Sprintf(CNATPortTemplate, CSeaweedFSFilerPort)

	seaweedFSTemplate, err := template.New("seaweedfs-s3.json").Parse(seaweedFSS3Conf)
	if err != nil {
		return fmt.Errorf("unable to parse seaweedfs s3 config template: %w", err)
	}
	templateBuffer := &bytes.Buffer{}
	if err := seaweedFSTemplate.Execute(templateBuffer, &SeaweedFSS3ConfigTemplateValues{
		Name:        CSeaweedFSIdentityName,
		AccessToken: CSeaweedFSAccessToken,
		SecretToken: CSeaweedFSSecretToken,
	}); err != nil {
		return fmt.Errorf("unable to render seaweedfs s3 config: %w", err)
	}

	req := testcontainers.ContainerRequest{
		Image: CSeaweedFSImage,
		Cmd: []string{
			"server",
			"-dir=/data",
			"-master.volumeSizeLimitMB=64",
			"-s3",
			fmt.Sprintf("-s3.port=%d", CSeaweedFSS3Port),
			fmt.Sprintf("-s3.config=%s", CSeaweedFSS3ConfigPath),
		},
		WaitingFor: s3GatewayReady(s3NATPort, 2*time.Minute),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
		},
		HostAccessPorts: append([]int{CSeaweedFSS3Port}, componentConfig.HostAccessPorts...),
		ExposedPorts:    []string{s3NATPortString, masterNATPortString, filerNATPortString},
		Networks:        []string{env.network.Name},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            templateBuffer,
				ContainerFilePath: CSeaweedFSS3ConfigPath,
				FileMode:          0o644,
			},
		},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(1 * time.Second)},
			Consumers: []testcontainers.LogConsumer{NewContainerLogConsumer(componentName, componentConfig.DisabledLogs)},
		},
	}

	container, err := startContainer(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to start seaweedfs container: %w", err)
	}

	containerIP, err := container.ContainerIP(ctx)
	if err != nil {
		return fmt.Errorf("unable to get seaweedfs container ip: %w", err)
	}

	containerHost, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("unable to get seaweedfs container host: %w", err)
	}

	s3ForwardedPort, err := container.MappedPort(ctx, s3NATPort)
	if err != nil {
		return fmt.Errorf("unable to get seaweedfs s3 forwarded port: %w", err)
	}

	masterForwardedPort, err := container.MappedPort(ctx, masterNATPort)
	if err != nil {
		return fmt.Errorf("unable to get seaweedfs master forwarded port: %w", err)
	}

	env.accessConfigs[componentName] = SeaweedFSAccessConfig{
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		S3Port: ContainerPort{
			Exposed:   CSeaweedFSS3Port,
			Forwarded: int(s3ForwardedPort.Num()),
		},
		MasterPort: ContainerPort{
			Exposed:   CSeaweedFSMasterPort,
			Forwarded: int(masterForwardedPort.Num()),
		},
		AccessToken: CSeaweedFSAccessToken,
		SecretToken: CSeaweedFSSecretToken,
	}
	env.terminators[componentName] = func(ctx context.Context) error {
		return stopContainer(ctx, container)
	}

	return nil
}

func startCephInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	withKeystone := len(componentConfig.Dependencies) != 0
	if withKeystone {
		return startCephInstanceWithKeystone(ctx, env, componentName, componentConfig)
	} else {
		return startStandaloneCephInstance(ctx, env, componentName, componentConfig)
	}
}

func startCephInstanceWithKeystone(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
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
		WithKeystone:     true,
		AuthHost:         keystoneEnv.Host.NAT,
		ExternalAuthPort: keystoneEnv.ExternalPort.Exposed,
		AdminProject:     keystoneEnv.ServiceProject.Name,
		AdminDomain:      keystoneEnv.DefaultDomain.Name,
		AdminUser:        cephUser.Name,
		AdminPassword:    CKeystoneCephPassword,
		OperatorRole:     operatorRole.Name,
		ResellerRole:     resellerRole.Name,
	}

	cephKeystoneTemplate, err := template.New("ceph-keystone.conf").Parse(cephKeystoneConf)
	if err != nil {
		return fmt.Errorf("unable to create ceph keystone config template: %w", err)
	}

	templateBuffer := &bytes.Buffer{}

	if err := cephKeystoneTemplate.Execute(templateBuffer, &cephRGWConfig); err != nil {
		return fmt.Errorf("unable to create keystone config out of template: %w", err)
	}

	apiNatPortString := fmt.Sprintf(CNATPortTemplate, CCephAPIPort)
	apiNatPort := apiNatPortString
	req := testcontainers.ContainerRequest{
		Image:      CCephImage,
		WaitingFor: wait.ForHTTP("/").WithStartupTimeout(5 * time.Minute).WithPort(apiNatPort),
		Env: map[string]string{
			"CEPH_DEMO_UID":   CCephDemoUID,
			"CEPH_EXTRA_CONF": templateBuffer.String(),
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
		},
		HostAccessPorts: []int{CCephAPIPort},
		ExposedPorts:    []string{apiNatPortString},
		Networks:        []string{env.network.Name},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(1 * time.Second)},
			Consumers: []testcontainers.LogConsumer{NewContainerLogConsumer(componentName, componentConfig.DisabledLogs)},
		},
	}

	container, err := startContainer(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to start ceph container: %w", err)
	}

	retCode, outReader, err := container.Exec(ctx,
		[]string{"radosgw-admin", "user", "create",
			"--uid", CCephSystemUserUID,
			"--display-name", CCephSystemUserDisplayName,
			"--access-key", CCephSystemUserAccessToken,
			"--secret-key", CCephSystemUserSecretToken,
			"--system"})
	if err != nil {
		return fmt.Errorf("unable to execute command: %w", err)
	}
	if retCode != 0 {
		outBytes, err := io.ReadAll(outReader)
		if err != nil {
			return fmt.Errorf("unable to read output: %w", err)
		}
		out := string(outBytes)
		return fmt.Errorf("command exit code %d: %s", retCode, out)
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

	closeIdleConns()

	var cephService *services.Service
	if err := retryOnTransient(ctx, 3, time.Second, func() error {
		var err error
		cephService, err = services.Create(ctx, identityClient, services.CreateOpts{
			Type: CKeystoneObjectStoreServiceType,
			Extra: map[string]any{
				"name": CKeystoneCephServiceName,
			},
		}).Extract()
		return err
	}); err != nil {
		return fmt.Errorf("unable to create ceph service: %w", err)
	}

	if err := retryOnTransient(ctx, 3, time.Second, func() error {
		_, err := endpoints.Create(ctx, identityClient, endpoints.CreateOpts{
			Name:         CKeystoneCephEndpointName,
			Availability: gophercloud.AvailabilityInternal,
			URL:          fmt.Sprintf(CKeystoneCephEndpointURLTemplate, containerHost, apiForwardedPort.Num()),
			ServiceID:    cephService.ID,
		}).Extract()
		return err
	}); err != nil {
		return fmt.Errorf("unable to create internal ceph endpoint: %w", err)
	}

	if err := retryOnTransient(ctx, 3, time.Second, func() error {
		_, err := endpoints.Create(ctx, identityClient, endpoints.CreateOpts{
			Name:         CKeystoneCephEndpointName,
			Availability: gophercloud.AvailabilityPublic,
			URL:          fmt.Sprintf(CKeystoneCephEndpointURLTemplate, containerHost, apiForwardedPort.Num()),
			ServiceID:    cephService.ID,
		}).Extract()
		return err
	}); err != nil {
		return fmt.Errorf("unable to create public ceph endpoint: %w", err)
	}

	env.accessConfigs[componentName] = CephAccessConfig{
		Port: ContainerPort{
			Exposed:   CCephAPIPort,
			Forwarded: int(apiForwardedPort.Num()),
		},
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		Keystone: StorageKeystoneAccessConfig{
			EndpointName: CKeystoneCephEndpointName,
			ServiceType:  CKeystoneObjectStoreServiceType,
			OperatorRole: operatorRole,
			ResellerRole: resellerRole,
		},
		SystemUser:     CCephSystemUserAccessToken,
		SystemPassword: CCephSystemUserSecretToken,
	}
	env.terminators[componentName] = func(ctx context.Context) error {
		return stopContainer(ctx, container)
	}

	return nil
}

func startStandaloneCephInstance(ctx context.Context, env *TestEnvironment, componentName string, componentConfig *ComponentCreationConfig) error {
	apiNatPortString := fmt.Sprintf(CNATPortTemplate, CCephAPIPort)
	apiNatPort := apiNatPortString
	req := testcontainers.ContainerRequest{
		Image:      CCephImage,
		WaitingFor: wait.ForHTTP("/").WithStartupTimeout(5 * time.Minute).WithPort(apiNatPort),
		Env: map[string]string{
			"CEPH_DEMO_UID":   CCephDemoUID,
			"CEPH_EXTRA_CONF": "[client.rgw.demo]\nrgw dns name = localhost",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.AutoRemove = true
		},
		HostAccessPorts: append([]int{CCephAPIPort}, componentConfig.HostAccessPorts...),
		ExposedPorts:    []string{apiNatPortString},
		Networks:        []string{env.network.Name},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(1 * time.Second)},
			Consumers: []testcontainers.LogConsumer{NewContainerLogConsumer(componentName, componentConfig.DisabledLogs)},
		},
	}

	container, err := startContainer(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to start ceph container: %w", err)
	}

	retCode, outReader, err := container.Exec(ctx,
		[]string{"radosgw-admin", "user", "create",
			"--uid", CCephSystemUserUID,
			"--display-name", CCephSystemUserDisplayName,
			"--access-key", CCephSystemUserAccessToken,
			"--secret-key", CCephSystemUserSecretToken,
			"--system"})
	if err != nil {
		return fmt.Errorf("unable to execute command: %w", err)
	}
	if retCode != 0 {
		outBytes, err := io.ReadAll(outReader)
		if err != nil {
			return fmt.Errorf("unable to read output: %w", err)
		}
		out := string(outBytes)
		return fmt.Errorf("command exit code %d: %s", retCode, out)
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

	env.accessConfigs[componentName] = CephAccessConfig{
		Port: ContainerPort{
			Exposed:   CCephAPIPort,
			Forwarded: int(apiForwardedPort.Num()),
		},
		Host: ContainerHost{
			NAT:   containerIP,
			Local: containerHost,
		},
		SystemUser:     CCephSystemUserAccessToken,
		SystemPassword: CCephSystemUserSecretToken,
	}
	env.terminators[componentName] = func(ctx context.Context) error {
		return stopContainer(ctx, container)
	}

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

func stopContainer(ctx context.Context, container testcontainers.Container) error {
	stopDuration := CContainerStopDeadline
	if err := container.Stop(ctx, &stopDuration); err != nil {
		return fmt.Errorf("unable to stop container: %w", err)
	}
	return nil
}

// closeIdleConns closes idle HTTP connections in the default transport.
// This prevents EOF errors caused by reusing connections that were closed
// server-side (e.g., by uwsgi) during long container startup delays.
func closeIdleConns() {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
}

func retryOnTransient(ctx context.Context, maxAttempts int, delay time.Duration, fn func() error) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isTransientErr(lastErr) {
			return lastErr
		}
		if i < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return lastErr
}

func isTransientErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "EOF") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "connection refused")
}
