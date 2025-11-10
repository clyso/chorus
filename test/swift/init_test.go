package swift

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/proxy/router"
	"github.com/clyso/chorus/test/env"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/endpoints"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/roles"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/services"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/users"
	"github.com/stretchr/testify/require"
)

const (
	CMultipartContentType = "application/octet-stream"

	CKeystoneURLTemplate = "http://%s:%d/v3"

	CKeystoneTestProjectName = "demo"
	CKeystoneTestUsername    = "demo"
	CKeystoneTestPassword    = "demo"

	CKeystoneTestComponentKey = "keystone"
	swiftTestKey              = "swift"
	cephTestKey               = "ceph"
)

var (
	tstCtx        context.Context
	tstEnv        *env.TestEnvironment
	testAcc       string
	testAccTenant string

	swiftConf     objstore.Config
	proxyStorages proxy.Storages

	clients objstore.Clients

	keystoneAdminClient *gophercloud.ServiceClient
)

func TestMain(m *testing.M) {
	tstCtx = context.Background()

	componentConfig := map[string]env.ComponentCreationConfig{
		CKeystoneTestComponentKey: env.AsKeystone(env.WithDisabledSTDErrLog(), env.WithDisabledSTDOutLog()),
		swiftTestKey:              env.AsSwift(CKeystoneTestComponentKey, env.WithDisabledSTDErrLog(), env.WithDisabledSTDOutLog()),
		cephTestKey:               env.AsCeph(env.WithKeystone(CKeystoneTestComponentKey), env.WithDisabledSTDErrLog(), env.WithDisabledSTDOutLog()),
	}
	testEnv, err := env.NewTestEnvironment(tstCtx, componentConfig)
	if err != nil {
		panic(err)
	}
	tstEnv = testEnv
	defer tstEnv.Terminate(tstCtx)

	// Create keystone user and allow it to interact with swift and keystone
	keystoneAccessConfig, err := tstEnv.GetKeystoneAccessConfig(CKeystoneTestComponentKey)
	if err != nil {
		panic(err)
	}

	swiftAccessConfig, err := tstEnv.GetSwiftAccessConfig(swiftTestKey)
	if err != nil {
		panic(err)
	}

	cephAccessConfig, err := tstEnv.GetCephAccessConfig(cephTestKey)
	if err != nil {
		panic(err)
	}

	providerClient, err := openstack.AuthenticatedClient(tstCtx, gophercloud.AuthOptions{
		IdentityEndpoint: fmt.Sprintf(CKeystoneURLTemplate, keystoneAccessConfig.Host.Local, keystoneAccessConfig.ExternalPort.Forwarded),
		Username:         keystoneAccessConfig.User,
		Password:         keystoneAccessConfig.Password,
		DomainName:       keystoneAccessConfig.DefaultDomain.Name,
		TenantName:       keystoneAccessConfig.TenantName,
	})
	if err != nil {
		panic(err)
	}

	identityClient, err := openstack.NewIdentityV3(providerClient, gophercloud.EndpointOpts{})
	if err != nil {
		panic(err)
	}
	keystoneAdmin, err := openstack.AuthenticatedClient(tstCtx, gophercloud.AuthOptions{
		IdentityEndpoint: fmt.Sprintf(CKeystoneURLTemplate, keystoneAccessConfig.Host.Local, keystoneAccessConfig.ExternalPort.Forwarded),
		Username:         keystoneAccessConfig.User,
		Password:         keystoneAccessConfig.Password,
		DomainName:       env.CKeystoneAdminDomainName,
		TenantName:       env.CKeystoneAdminTenantName,
	})
	if err != nil {
		panic(err)
	}

	keystoneAdminClient, err = openstack.NewIdentityV3(keystoneAdmin, gophercloud.EndpointOpts{})
	if err != nil {
		panic(err)
	}

	newProject, err := projects.Create(tstCtx, identityClient, projects.CreateOpts{
		DomainID: keystoneAccessConfig.DefaultDomain.ID,
		Name:     CKeystoneTestProjectName,
	}).Extract()
	if err != nil {
		panic(err)
	}
	testAcc = newProject.ID
	testAccTenant = CKeystoneTestProjectName

	newUser, err := users.Create(tstCtx, identityClient, users.CreateOpts{
		Name:     CKeystoneTestUsername,
		Password: CKeystoneTestPassword,
		DomainID: keystoneAccessConfig.DefaultDomain.ID,
	}).Extract()
	if err != nil {
		panic(err)
	}

	err = roles.Assign(tstCtx, identityClient, swiftAccessConfig.Keystone.OperatorRole.ID, roles.AssignOpts{
		UserID:    newUser.ID,
		ProjectID: newProject.ID,
	}).ExtractErr()
	if err != nil {
		panic(err)
	}

	err = roles.Assign(tstCtx, identityClient, cephAccessConfig.Keystone.OperatorRole.ID, roles.AssignOpts{
		UserID:    newUser.ID,
		ProjectID: newProject.ID,
	}).ExtractErr()
	if err != nil {
		panic(err)
	}
	swiftCreds := swift.Storage{
		StorageEndpointName: env.CKeystoneSwiftEndpointName,
		AuthURL:             fmt.Sprintf(CKeystoneURLTemplate, keystoneAccessConfig.Host.Local, keystoneAccessConfig.ExternalPort.Forwarded),
		Credentials: map[string]swift.Credentials{
			testAcc: {
				Username:   CKeystoneTestUsername,
				Password:   CKeystoneTestPassword,
				DomainName: keystoneAccessConfig.DefaultDomain.Name,
				TenantName: testAccTenant,
			},
		},
	}
	cephCreds := swiftCreds
	cephCreds.StorageEndpointName = env.CKeystoneCephEndpointName
	swiftConf = objstore.Config{
		Storages: map[string]objstore.Storage{
			swiftTestKey: {
				CommonConfig: objstore.CommonConfig{
					Type: dom.Swift,
				},
				Swift: &swiftCreds,
			},
			cephTestKey: {
				CommonConfig: objstore.CommonConfig{
					Type: dom.Swift,
				},
				Swift: &cephCreds,
			},
		},
		Main: swiftTestKey,
	}
	if err := swiftConf.Validate(); err != nil {
		panic(err)
	}

	cephURL := fmt.Sprintf("http://%s:%d/swift", cephAccessConfig.Host.Local, cephAccessConfig.Port.Forwarded)
	swiftURL := fmt.Sprintf("http://%s:%d", swiftAccessConfig.Host.Local, swiftAccessConfig.Port.Forwarded)

	proxyStorages = proxy.Storages{
		Storages: map[string]proxy.Storage{
			swiftTestKey: {
				CommonConfig: objstore.CommonConfig{
					Type: dom.Swift,
				},
				Swift: &router.SwiftStorage{
					StorageURL: swiftURL,
				},
			},
			cephTestKey: {
				CommonConfig: objstore.CommonConfig{
					Type: dom.Swift,
				},
				Swift: &router.SwiftStorage{
					StorageURL: cephURL,
				},
			},
		},
		Main: swiftTestKey,
	}
	if err := proxyStorages.Validate(); err != nil {
		panic(err)
	}
	swiftClient, err := swift.New(swiftConf.SwiftStorages())
	if err != nil {
		panic(err)
	}
	clients, err = objstore.New(swiftConf, map[dom.StorageType]any{dom.Swift: swiftClient})
	if err != nil {
		panic(err)
	}
	exitCode := m.Run()
	tstEnv.Terminate(tstCtx)
	// exit
	os.Exit(exitCode)
}

func registerSwiftEndpoitnt(t testing.TB, name, host string, port int) {
	t.Helper()
	ctx := t.Context()
	r := require.New(t)
	objectService, err := services.Create(ctx, keystoneAdminClient, services.CreateOpts{
		Type: env.CKeystoneObjectStoreServiceType,
		Extra: map[string]any{
			"name": name,
		},
	}).Extract()

	r.NoError(err, "unable to create swift service in keystone")
	_, err = endpoints.Create(ctx, keystoneAdminClient, endpoints.CreateOpts{
		Name:         name,
		Availability: gophercloud.AvailabilityInternal,
		URL:          fmt.Sprintf(env.CKeystoneSwiftEndpointURLTemplate, host, port),
		ServiceID:    objectService.ID,
	}).Extract()
	r.NoError(err, "unable to create internal swift endpoint")

	_, err = endpoints.Create(ctx, keystoneAdminClient, endpoints.CreateOpts{
		Name:         name,
		Availability: gophercloud.AvailabilityPublic,
		URL:          fmt.Sprintf(env.CKeystoneSwiftEndpointURLTemplate, host, port),
		ServiceID:    objectService.ID,
	}).Extract()
	r.NoError(err, "unable to create public swift endpoint")
}
