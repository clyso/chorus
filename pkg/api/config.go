/*
 * Copyright © 2023 Clyso GmbH
 * Copyright © 2025 STRATO GmbH
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

type Config struct {
	Enabled             bool   `yaml:"enabled"`
	GrpcPort            int    `yaml:"grpcPort"`
	HttpPort            int    `yaml:"httpPort"`
	Secure              bool   `yaml:"secure"`
	GrpcTLSCertFile     string `yaml:"grpcCertFile"`
	GrpcTLSKeyFile      string `yaml:"grpcKeyFile"`
	GrpcTLSClientAuth   bool   `yaml:"grpcClientAuth"`
	GrpcTLSClientCAFile string `yaml:"grpcClientCAFile"`
}
