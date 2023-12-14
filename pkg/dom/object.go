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

package dom

import (
	"fmt"
)

type Object struct {
	Bucket  string
	Name    string
	Version string
}

func (o Object) Key() string {
	if o.Version == "" {
		return fmt.Sprintf("%s:%s", o.Bucket, o.Name)
	}
	return fmt.Sprintf("%s:%s:%s", o.Bucket, o.Name, o.Version)
}

func (o Object) ACLKey() string {
	return o.Key() + ":a"
}

func (o Object) TagKey() string {
	return o.Key() + ":t"
}
