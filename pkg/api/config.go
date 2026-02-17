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
	"fmt"
	"net/url"
	"strings"
)

type Config struct {
	Webhook  WebhookConfig `yaml:"webhook"`
	GrpcPort int           `yaml:"grpcPort"`
	HttpPort int           `yaml:"httpPort"`
	Enabled  bool          `yaml:"enabled"`
	Secure   bool          `yaml:"secure"`
}

type WebhookConfig struct {
	BaseURL  string `yaml:"baseUrl"`
	GrpcPort int    `yaml:"grpcPort"`
	HttpPort int    `yaml:"httpPort"`
	Enabled  bool   `yaml:"enabled"`
}

func (c *WebhookConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.GrpcPort > 0 && c.HttpPort <= 0 {
		return fmt.Errorf("webhook config: httpPort must be set when grpcPort is set (separate webhook ports require both)")
	}
	if c.HttpPort > 0 && c.GrpcPort <= 0 {
		return fmt.Errorf("webhook config: grpcPort must be set when httpPort is set (separate webhook ports require both)")
	}
	if c.BaseURL != "" {
		u, err := url.Parse(c.BaseURL)
		if err != nil {
			return fmt.Errorf("webhook config: invalid baseUrl: %w", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("webhook config: baseUrl must include scheme (http:// or https://)")
		}
		if u.Host == "" {
			return fmt.Errorf("webhook config: baseUrl must include host")
		}
	}
	return nil
}

func (c *WebhookConfig) S3NotificationURL(storage string) (string, error) {
	if !c.Enabled {
		return "", fmt.Errorf("webhook must be enabled for s3_notification event source")
	}
	if c.BaseURL == "" {
		return "", fmt.Errorf("webhook baseUrl is required for s3_notification event source")
	}
	base := strings.TrimRight(c.BaseURL, "/")
	return base + "/webhook/" + storage + "/s3-notifications", nil
}
