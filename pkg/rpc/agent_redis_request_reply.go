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
	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"strings"
	"time"
)

const (
	agentPing = "rr:agent:ping"
)

type AgentClient struct {
	client redis.UniversalClient
}

func NewAgentClient(client redis.UniversalClient) *AgentClient {
	return &AgentClient{client: client}
}

type AgentInfo struct {
	URL         string
	FromStorage string
}

func (a *AgentClient) Ping(ctx context.Context) ([]AgentInfo, error) {
	replyChanName := xid.New().String()
	ps := a.client.Subscribe(ctx, replyChanName)
	defer ps.Close()
	defer ps.Unsubscribe(context.Background(), replyChanName)
	resCh := make(chan AgentInfo, 1)
	reqCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	go func() {
		defer close(resCh)
		for {
			select {
			case <-reqCtx.Done():
				return
			case msg, more := <-ps.Channel():
				msgArr := strings.Split(msg.Payload, "|")
				if len(msgArr) != 2 {
					zerolog.Ctx(reqCtx).Error().Msgf("agent client received invalid payload: %s", msg.Payload)
					return
				}
				resCh <- AgentInfo{
					URL:         msgArr[0],
					FromStorage: msgArr[1],
				}
				if !more {
					return
				}
			}
		}
	}()
	received, err := a.client.Publish(reqCtx, agentPing, replyChanName).Result()
	if err != nil {
		return nil, err
	}
	if received == 0 {
		// no agents registered
		return nil, nil
	}
	var agents []AgentInfo
	for {
		select {
		case res, more := <-resCh:
			agents = append(agents, res)
			if !more || len(agents) == int(received) {
				return agents, nil
			}
		case <-reqCtx.Done():
			return agents, nil
		}
	}
}

func AgentServe(ctx context.Context, client redis.UniversalClient, url, fromStorage string) error {
	ps := client.Subscribe(ctx, agentPing)
	defer ps.Close()
	defer ps.Unsubscribe(context.Background(), agentPing)
	messages := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messages:
			if msg.Payload == "" {
				zerolog.Ctx(ctx).Warn().Msg("request reply agent server: empty payload for proxy creds pubSub received")
				continue
			}
			received, err := client.Publish(ctx, msg.Payload, fmt.Sprintf("%s|%s", url, fromStorage)).Result()
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("request reply agent server: unable to publish reply")
				continue
			}
			if received == 0 {
				zerolog.Ctx(ctx).Warn().Msg("request reply agent server: reply not received")
			}
		}
	}
}
