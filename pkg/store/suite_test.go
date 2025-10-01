package store

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/test/env"
	"github.com/clyso/chorus/test/gen"
)

const (
	CMiniRedisComponent = "miniredis"
)

var (
	testCtx           context.Context
	testCtxCancelFunc context.CancelFunc
	testRedisClient   redis.UniversalClient
	testRnd           *gen.Rnd
)

func TestEnv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Consistency Checker Suite")
}

var _ = BeforeSuite(func() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	envConfig := map[string]env.ComponentCreationConfig{
		CMiniRedisComponent: env.AsMiniRedis(),
	}
	testEnv, err := env.NewTestEnvironment(ctx, envConfig)
	Expect(err).NotTo(HaveOccurred())

	redisAccessConfig, err := testEnv.GetMiniRedisAccessConfig(CMiniRedisComponent)
	Expect(err).NotTo(HaveOccurred())

	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{fmt.Sprintf("%s:%d", redisAccessConfig.Host, redisAccessConfig.Port)},
		Password: redisAccessConfig.Password,
	})

	var rnd *gen.Rnd

	if gen.CUseTestGenSeed {
		rnd = gen.NewRnd(gen.CTestGenSeed)
	} else {
		rnd = gen.NewRnd(GinkgoRandomSeed())
	}

	testCtx = ctx
	testCtxCancelFunc = cancelFunc
	testRedisClient = redisClient
	testRnd = rnd
})

var _ = AfterSuite(func() {
	testCtxCancelFunc()
})
