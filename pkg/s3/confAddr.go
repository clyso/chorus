/*
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

package s3

import (
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfAddr is a custom string type for addresses in yaml config. As chrous
// never determines protocol from this field (http vs. https) but rather from a
// dedicated "Secure" config option, the protocol here is obsolete.
// For compatibility reasons we still allow for the protocol to be specified,
// but the code using the address should not need to cope with these prefixes.
// The resulting type contains the protocol, too, for error checking reasons.
type ConfAddr struct {
	protocol string
	address  string
	rawValue string /* should only be used for error messages to show the value the user used in the config file */
	set      bool
}

// force compiler error if interface is not implemented.
var _ yaml.Unmarshaler = &ConfAddr{}
var _ yaml.Marshaler = ConfAddr{}

func NewConfAddr(protoAddr string) ConfAddr {
	if protoAddr == "" {
		return ConfAddr{}
	}
	parts := strings.SplitN(protoAddr, "://", 2)
	if len(parts) == 1 {
		return ConfAddr{
			address:  parts[0],
			rawValue: protoAddr,
			set:      true,
		}
	}
	return ConfAddr{
		protocol: parts[0],
		address:  parts[1],
		rawValue: protoAddr,
		set:      true,
	}
}

func NewConfAddrs(protoAddr ...string) []ConfAddr {
	list := make([]ConfAddr, len(protoAddr))
	for i, pa := range protoAddr {
		list[i] = NewConfAddr(pa)
	}
	return list
}

func (a *ConfAddr) UnmarshalYAML(node *yaml.Node) error {
	val := ""
	if err := node.Decode(&val); err != nil {
		return err
	}
	*a = NewConfAddr(val)
	return nil
}

func (a ConfAddr) MarshalYAML() (interface{}, error) {
	return a.ValueWithProtocol(), nil
}

func (a *ConfAddr) Protocol() string {
	return a.protocol
}

func (a *ConfAddr) SetProtocol(protocol string) {
	a.protocol = protocol
}

func (a *ConfAddr) Value() string {
	return a.address
}

func (a ConfAddr) ValueWithProtocol() string {
	if a.protocol == "" {
		return a.address
	}
	return a.protocol + "://" + a.address
}

func (a *ConfAddr) RawValue() string {
	return a.rawValue
}

func (a *ConfAddr) GetEndpoint(isSecure bool) string {
	if a.protocol == "" {
		if isSecure {
			return "https://" + a.address
		} else {
			return "http://" + a.address
		}
	}
	return a.ValueWithProtocol()
}

func (a *ConfAddr) IsSet() bool {
	return a.set
}
