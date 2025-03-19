/*
 * Copyright © 2023 Clyso GmbH
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

package api

import (
	"context"

	"github.com/sirupsen/logrus"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func Connect(ctx context.Context, url string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, url,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		//grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{MaxDelay: time.Second}}),
		//grpc.WithInsecure(),
	)
}

func PrintGrpcError(err error) {
	if err == nil {
		return
	}
	// Extract the status from the error
	st, ok := status.FromError(err)
	if !ok {
		logrus.WithError(err).Fatal("error from server")
	}

	log := logrus.WithError(err).
		WithField("code", st.Code())

	msg := ""

	// Iterate over the details
	for _, detail := range st.Details() {
		switch d := detail.(type) {
		case *errdetails.RequestInfo:
			log = log.WithField("request_id", d.RequestId)
		case *errdetails.ErrorInfo:
			msg += d.Reason
		case *errdetails.RetryInfo:
			if d.RetryDelay != nil {
				log = log.WithField("retry_delay", d.RetryDelay.AsDuration())
			}
		default:
		}
	}
	if msg == "" {
		msg = "error from server: " + st.Message()
	}
	log.Fatal(msg)
}
