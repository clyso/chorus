package swift

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/test/env"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/roles"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/users"
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
	tstCtx    context.Context
	tstEnv    *env.TestEnvironment
	testAcc   string
	swiftConf swift.WorkerConfig
)

func TestMain(m *testing.M) {
	tstCtx = context.Background()

	componentConfig := map[string]env.ComponentCreationConfig{
		CKeystoneTestComponentKey: env.AsKeystone(env.WithDisabledSTDErrLog(), env.WithDisabledSTDOutLog()),
		swiftTestKey:              env.AsSwift(CKeystoneTestComponentKey, env.WithDisabledSTDErrLog(), env.WithDisabledSTDOutLog()),
		cephTestKey:               env.AsCeph(CKeystoneTestComponentKey, env.WithDisabledSTDErrLog(), env.WithDisabledSTDOutLog()),
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

	newProject, err := projects.Create(tstCtx, identityClient, projects.CreateOpts{
		DomainID: keystoneAccessConfig.DefaultDomain.ID,
		Name:     CKeystoneTestProjectName,
	}).Extract()
	if err != nil {
		panic(err)
	}

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
	testAcc = CKeystoneTestProjectName
	swiftCreds := swift.WokerStorageCredentials{
		StorageEndpointName: env.CKeystoneSwiftEndpointName,
		AuthURL:             fmt.Sprintf(CKeystoneURLTemplate, keystoneAccessConfig.Host.Local, keystoneAccessConfig.ExternalPort.Forwarded),
		Superuser: swift.UserCredentials{
			Username:   CKeystoneTestUsername,
			Password:   CKeystoneTestPassword,
			DomainName: keystoneAccessConfig.DefaultDomain.Name,
		},
	}
	cephCreds := swiftCreds
	cephCreds.StorageEndpointName = env.CKeystoneCephEndpointName
	swiftConf = swift.WorkerConfig{
		Storages: map[string]swift.WokerStorageCredentials{
			swiftTestKey: swiftCreds,
			cephTestKey:  cephCreds,
		},
	}
	exitCode := m.Run()
	tstEnv.Terminate(tstCtx)
	// exit
	os.Exit(exitCode)
}
