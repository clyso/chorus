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

package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

const (
	proxyCreds = "rr:proxy:creds"
)

type GetCredentialsResp struct {
	Creds   map[string]s3.CredentialsV4
	Address string
}

type Proxy interface {
	GetCredentials(ctx context.Context) (*pb.GetProxyCredentialsResponse, error)
}

type ProxyGetCredentials func(ctx context.Context) (*pb.GetProxyCredentialsResponse, error)

func (f ProxyGetCredentials) GetCredentials(ctx context.Context) (*pb.GetProxyCredentialsResponse, error) {
	return f(ctx)
}

var _ Proxy = &ProxyClient{}

type ProxyClient struct {
	client redis.UniversalClient
}

func NewProxyClient(client redis.UniversalClient) Proxy {
	return &ProxyClient{client: client}
}

func (c *ProxyClient) GetCredentials(ctx context.Context) (*pb.GetProxyCredentialsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	replyChanName := xid.New().String()
	ps := c.client.Subscribe(ctx, replyChanName)
	defer func() {
		_ = ps.Close()
		_ = ps.Unsubscribe(context.Background(), replyChanName)
	}()

	// Subscribe is asynchronous. Wait for Redis to confirm the reply-channel
	// subscription before publishing the request, or the response can be lost.
	if _, err := ps.Receive(ctx); err != nil {
		return nil, fmt.Errorf("subscribe to proxy credentials reply channel: %w", err)
	}

	messages := ps.Channel()
	received, err := c.client.Publish(ctx, proxyCreds, replyChanName).Result()
	if err != nil {
		return nil, err
	}
	if received == 0 {
		return nil, fmt.Errorf("%w: proxy is not listening", dom.ErrNotImplemented)
	}

	select {
	case msg, ok := <-messages:
		if !ok || msg == nil {
			return nil, fmt.Errorf("%w: proxy credentials reply channel closed", dom.ErrInternal)
		}
		var res pb.GetProxyCredentialsResponse
		if err := proto.Unmarshal([]byte(msg.Payload), &res); err != nil {
			return nil, err
		}
		return &res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func ProxyServe(ctx context.Context, client redis.UniversalClient, proxy Proxy) error {
	ps := client.Subscribe(ctx, proxyCreds)
	defer func() {
		_ = ps.Close()
		_ = ps.Unsubscribe(context.Background(), proxyCreds)
	}()

	// Do not start consuming requests until Redis confirms this subscription.
	if _, err := ps.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe to proxy credentials requests: %w", err)
	}

	messages := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return nil
			}
			if msg == nil || msg.Payload == "" {
				zerolog.Ctx(ctx).Warn().Msg("request reply proxy server: empty payload for proxy creds pubSub received")
				continue
			}
			creds, err := proxy.GetCredentials(ctx)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("request reply proxy server: get creds err")
				continue
			}
			credsBytes, err := proto.Marshal(creds)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("request reply proxy server: unable to marshal payload")
				continue
			}
			received, err := client.Publish(ctx, msg.Payload, string(credsBytes)).Result()
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("request reply proxy server: unable to publish reply")
				continue
			}
			if received == 0 {
				zerolog.Ctx(ctx).Warn().Msg("request reply proxy server: reply not received")
			}
		}
	}
}
