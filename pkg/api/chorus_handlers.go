/*
 * Copyright © 2024 Clyso GmbH
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

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/rpc"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func ChorusHandlers(
	credsSvc objstore.CredsService,
	proxyClient rpc.Proxy,
	agentClient *rpc.AgentClient,
	appInfo *dom.AppInfo,
) pb.ChorusServer {
	return &chorusHandlers{
		credsSvc:    credsSvc,
		proxyClient: proxyClient,
		agentClient: agentClient,
		appInfo:     appInfo,
	}
}

var _ pb.ChorusServer = &chorusHandlers{}

type chorusHandlers struct {
	proxyClient rpc.Proxy
	agentClient *rpc.AgentClient
	appInfo     *dom.AppInfo
	credsSvc    objstore.CredsService
}

func (h *chorusHandlers) GetAppVersion(_ context.Context, _ *emptypb.Empty) (*pb.GetAppVersionResponse, error) {
	return &pb.GetAppVersionResponse{
		Version: h.appInfo.Version,
		Commit:  h.appInfo.Commit,
		Date:    h.appInfo.Date,
	}, nil
}

func (h *chorusHandlers) GetStorages(_ context.Context, _ *emptypb.Empty) (*pb.GetStoragesResponse, error) {
	res := toPbStorages(h.credsSvc)
	return &pb.GetStoragesResponse{Storages: res}, nil
}

func (h *chorusHandlers) GetProxyCredentials(ctx context.Context, _ *emptypb.Empty) (*pb.GetProxyCredentialsResponse, error) {
	return h.proxyClient.GetCredentials(ctx)
}

func (h *chorusHandlers) GetAgents(ctx context.Context, _ *emptypb.Empty) (*pb.GetAgentsResponse, error) {
	agents, err := h.agentClient.Ping(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*pb.Agent, len(agents))
	for i, agent := range agents {
		res[i] = &pb.Agent{
			Storage: agent.FromStorage,
			Url:     agent.URL,
		}
	}
	return &pb.GetAgentsResponse{Agents: res}, nil
}
