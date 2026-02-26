/*
 * Copyright © 2025 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
)

// newPolicyClient dials the management API and returns a Policy client.
// The caller is responsible for closing the connection.
func newPolicyClient(ctx context.Context) (*grpc.ClientConn, pb.PolicyClient) {
	conn, err := api.Connect(ctx, address)
	if err != nil {
		logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
	}
	return conn, pb.NewPolicyClient(conn)
}

// buildReplicationID constructs a ReplicationID from common CLI flags.
// Semantics follow proto/chorus/policy.proto:
//   - user-level replication: user/from/to set, fromBucket/toBucket empty
//   - bucket-level replication: user/from/to and fromBucket set, toBucket optional
func buildReplicationID(user, from, to, fromBucket, toBucket string) *pb.ReplicationID {
	id := &pb.ReplicationID{
		User:        user,
		FromStorage: from,
		ToStorage:   to,
	}
	if fromBucket != "" {
		id.FromBucket = &fromBucket
	}
	// if to-bucket is not set, default to from-bucket for bucket-level policies
	if toBucket != "" {
		id.ToBucket = &toBucket
	} else if fromBucket != "" {
		b := fromBucket
		id.ToBucket = &b
	}
	return id
}
