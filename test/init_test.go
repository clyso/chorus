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

package test

import (
	"time"

	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
)

var (
	workerConf *worker.Config
	proxyConf  *proxy.Config
)

func init() {
	var err error
	workerConf, err = worker.GetConfig()
	if err != nil {
		panic(err)
	}
	workerConf.Worker.QueueUpdateInterval = 500 * time.Millisecond
	workerConf.Features.ACL = false
	workerConf.Features.Tagging = false
	workerConf.Log.Level = "warn"

	proxyConf, err = proxy.GetConfig()
	if err != nil {
		panic(err)
	}
	proxyConf.Features.ACL = false
	proxyConf.Features.Tagging = false
	proxyConf.Log.Level = "warn"
}
