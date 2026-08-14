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
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"

	"github.com/sirupsen/logrus"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

// Connect creates a client for the chorus management REST API.
// Address must be a full URL including http:// or https:// scheme,
// e.g. http://localhost:9671.
func Connect(_ context.Context, address string, insecureSkipTLSVerify bool) (*Conn, error) {
	u, err := url.Parse(address)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("address %q must be a full URL of chorus management REST api with http:// or https:// scheme, e.g. %q (note: REST api port 9671, not gRPC port 9670)", address, "http://localhost:9671")
	}
	if u.Host == "" {
		return nil, fmt.Errorf("address %q must contain a host, e.g. %q", address, "http://localhost:9671")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if insecureSkipTLSVerify {
		if u.Scheme == "http" {
			logrus.Warn("--insecure flag has no effect for http:// address")
		}
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit user opt-in via --insecure flag
	}
	return &Conn{
		httpClient: &http.Client{Transport: transport},
		baseURL:    u,
	}, nil
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
