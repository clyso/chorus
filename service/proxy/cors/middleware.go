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

package cors

import (
	"net/http"

	"github.com/rs/cors"
)

func HttpMiddleware(conf *Config, next http.Handler) http.Handler {
	if !conf.Enabled {
		return next
	}

	if conf.AllowAll {
		conf.Whitelist = []string{"*"}
	}

	return cors.New(cors.Options{
		AllowedOrigins: conf.Whitelist,
		AllowedMethods: []string{
			http.MethodHead,
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodDelete,
		},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: false,
		// response headers required for multipart upload
		ExposedHeaders: []string{"*"},
	}).Handler(next)
}
