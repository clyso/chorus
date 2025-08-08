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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/minio/madmin-go/v4"
	"github.com/minio/minio-go/v7"
	mcredentials "github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/clyso/chorus/test/env"
	"github.com/clyso/chorus/test/gen"
)

const (
	Seed        = 811509576612567777
	Minio       = "minio"
	MinioBucket = "buck"
	MinioUser   = "user"
	MinioPass   = "userpass"
)

func main() {
	ctx := context.Background()

	objGen := gen.NewS3ObjectGenerator(
		gen.WithVersioned(),
	)
	treeGen, err := gen.NewTreeGenerator(
		gen.WithRandomSeed[*gen.GeneratedS3Object](Seed),
		gen.WithObjectGenerator[*gen.GeneratedS3Object](objGen),
	)
	if err != nil {
		panic(err)
	}

	tree, err := treeGen.Generate()
	if err != nil {
		panic(err)
	}

	testEnv, err := env.NewTestEnvironment(ctx, map[string]env.ComponentCreationConfig{
		Minio: env.AsMinio(),
	})
	if err != nil {
		panic(err)
	}

	minioAccessConfig, err := testEnv.GetMinioAccessConfig(Minio)
	if err != nil {
		panic(err)
	}
	minioS3Endpoint := fmt.Sprintf("%s:%d", minioAccessConfig.Host.Local, minioAccessConfig.S3Port.Forwarded)
	adminClient, err := madmin.NewWithOptions(minioS3Endpoint, &madmin.Options{
		Creds:  mcredentials.NewStaticV4(minioAccessConfig.User, minioAccessConfig.Password, ""),
		Secure: false,
	})
	if err != nil {
		panic(err)
	}

	err = adminClient.AddUser(ctx, MinioUser, MinioPass)
	if err != nil {
		panic(err)
	}

	_, err = adminClient.AttachPolicy(ctx, madmin.PolicyAssociationReq{
		Policies: []string{"readwrite"},
		User:     MinioUser,
	})
	if err != nil {
		panic(err)
	}

	userClient, err := minio.New(minioS3Endpoint, &minio.Options{
		Creds:  mcredentials.NewStaticV4(MinioUser, MinioPass, ""),
		Secure: false,
	})
	if err != nil {
		panic(err)
	}
	err = userClient.MakeBucket(ctx, MinioBucket, minio.MakeBucketOptions{})
	if err != nil {
		panic(err)
	}
	err = userClient.EnableVersioning(ctx, MinioBucket)
	if err != nil {
		panic(err)
	}

	filler := gen.NewS3Filler(tree, userClient)
	err = filler.Fill(ctx, MinioBucket)
	if err != nil {
		panic(err)
	}

	osSigChan := make(chan os.Signal, 1)
	signal.Notify(osSigChan, os.Interrupt)

	<-osSigChan
}
