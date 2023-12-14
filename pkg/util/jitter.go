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

package util

import (
	"math/rand"
	"time"
)

func DurationJitter(min, max time.Duration) time.Duration {
	maxSec := int(max.Seconds())
	if maxSec == 0 {
		maxSec = 1
	}
	minSec := int(min.Seconds())
	sec := rand.Intn(maxSec-minSec+1) + minSec
	return time.Duration(sec) * time.Second
}
