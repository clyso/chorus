// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/go-viper/mapstructure/v2"
	"gopkg.in/yaml.v3"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/ratelimit"
)

type CommonConfig struct {
	Type      dom.StorageType     `yaml:"type"`
	RateLimit ratelimit.RateLimit `yaml:"rateLimit"`
}

type StorageConfig interface {
	Validate() error
	UserList() []string
	HasUser(user string) bool
	comparable
}

type StoragesConfig[
	S3Config StorageConfig,
	SwiftConfig StorageConfig,
] struct {
	Storages map[string]GenericStorage[S3Config, SwiftConfig] `yaml:"storages"`
	Main     string                                           `yaml:"main"`
}

type GenericStorage[
	S3Config StorageConfig,
	SwiftConfig StorageConfig,
] struct {
	S3    S3Config
	Swift SwiftConfig
	CommonConfig
}

func (s StoragesConfig[_, _]) Validate() error {
	if len(s.Storages) == 0 {
		return fmt.Errorf("%w: storages config is empty", dom.ErrInvalidStorageConfig)
	}
	if s.Main == "" {
		return fmt.Errorf("%w: main storage is not set", dom.ErrInvalidStorageConfig)
	}
	if _, ok := s.Storages[s.Main]; !ok {
		return fmt.Errorf("%w: main storage %q is not found in storages list", dom.ErrInvalidStorageConfig, s.Main)
	}
	for name, storConf := range s.Storages {
		if err := storConf.Validate(); err != nil {
			return fmt.Errorf("%w: for storage %q", err, name)
		}
	}
	return nil
}

func (s GenericStorage[S3Config, SwiftConfig]) Validate() error {
	var s3Zero S3Config
	var swiftZero SwiftConfig
	switch s.Type {
	case dom.S3:
		if s.S3 == s3Zero {
			return fmt.Errorf("%w: storage type is S3 but S3 config is missing", dom.ErrInvalidStorageConfig)
		}
		if s.Swift != swiftZero {
			return fmt.Errorf("%w: storage type is S3 but SWIFT config is set", dom.ErrInvalidStorageConfig)
		}
		return s.S3.Validate()
	case dom.Swift:
		if s.Swift == swiftZero {
			return fmt.Errorf("%w: storage type is SWIFT but SWIFT config is missing", dom.ErrInvalidStorageConfig)
		}
		if s.S3 != s3Zero {
			return fmt.Errorf("%w: storage type is SWIFT but S3 config is set", dom.ErrInvalidStorageConfig)
		}
		return s.Swift.Validate()
	default:
		return fmt.Errorf("%w: unsupported storage type %q", dom.ErrInvalidStorageConfig, s.Type)
	}
}

func (s StoragesConfig[S3Config, SwiftConfig]) Exists(stor, user string) error {
	got, ok := s.Storages[stor]
	if !ok {
		return fmt.Errorf("%w: unknown storage %s", dom.ErrInvalidArg, stor)
	}
	switch got.Type {
	case dom.S3:
		if ok := got.S3.HasUser(user); !ok {
			return fmt.Errorf("%w: unknown user %s for storage %s", dom.ErrInvalidArg, user, stor)
		}
		return nil
	case dom.Swift:
		if ok := got.Swift.HasUser(user); !ok {
			return fmt.Errorf("%w: unknown user %s for storage %s", dom.ErrInvalidArg, user, stor)
		}
		return nil
	default:
		return fmt.Errorf("%w: cannot get user: unsupported storage type %q", dom.ErrInvalidStorageConfig, got.Type)
	}
}

func (s StoragesConfig[S3Config, SwiftConfig]) ValidateReplicationID(id entity.UniversalReplicationID) error {
	if err := s.Exists(id.FromStorage(), id.User()); err != nil {
		return fmt.Errorf("%w: unknown replication source", err)
	}
	if err := s.Exists(id.ToStorage(), id.User()); err != nil {
		return fmt.Errorf("%w: unknown replication destination", err)
	}
	fromType, toType := s.Storages[id.FromStorage()].Type, s.Storages[id.ToStorage()].Type
	if fromType != toType {
		// TODO: allow cross-type replication in the future?
		return fmt.Errorf("%w: from_storage %q type %q is different from to_storage %q type %q",
			dom.ErrInvalidArg, id.FromStorage(), fromType, id.ToStorage(), toType)
	}
	return nil
}

func (s StoragesConfig[S3conf, SwiftConf]) S3Storages() map[string]S3conf {
	res := make(map[string]S3conf)
	for name, conf := range s.Storages {
		if conf.Type == dom.S3 {
			res[name] = conf.S3
		}
	}
	return res
}

func (s StoragesConfig[S3conf, SwiftConf]) SwiftStorages() map[string]SwiftConf {
	res := make(map[string]SwiftConf)
	for name, conf := range s.Storages {
		if conf.Type == dom.Swift {
			res[name] = conf.Swift
		}
	}
	return res
}

func (s StoragesConfig[_, _]) RateLimitConf() map[string]ratelimit.RateLimit {
	res := make(map[string]ratelimit.RateLimit, len(s.Storages))
	for name, conf := range s.Storages {
		res[name] = conf.RateLimit
	}
	return res
}

func (s StoragesConfig[S3conf, SwiftConf]) GetMain() GenericStorage[S3conf, SwiftConf] {
	return s.Storages[s.Main]
}

func (a GenericStorage[S3conf, SwiftConf]) UserList() []string {
	var res []string
	switch a.Type {
	case dom.S3:
		res = a.S3.UserList()
	case dom.Swift:
		res = a.Swift.UserList()
	default:
		panic(fmt.Sprintf("unsupported storage type %q in UserList()", a.Type))
	}
	// sort result for consistency
	slices.Sort(res)
	return res
}

func (s *GenericStorage[S3conf, SwiftConf]) UnmarshalYAML(node *yaml.Node) error {
	var common struct {
		CommonConfig `yaml:",inline"`
	}
	if err := node.Decode(&common); err != nil {
		return err
	}
	s.CommonConfig = common.CommonConfig
	// check storage type and decode specific config
	switch common.Type {
	case dom.S3:
		s3conf := new(S3conf)
		if err := node.Decode(s3conf); err != nil {
			return err
		}
		s.S3 = *s3conf
		return nil
	case dom.Swift:
		swiftconf := new(SwiftConf)
		if err := node.Decode(swiftconf); err != nil {
			return err
		}
		s.Swift = *swiftconf
		return nil

	default:
		return fmt.Errorf("%w: unsupported storage type %q", dom.ErrInvalidStorageConfig, common.Type)
	}
}

func (a GenericStorage[S3conf, SwiftConf]) MarshalYAML() (any, error) {
	switch a.Type {
	case dom.S3:
		return struct {
			S3           S3conf `yaml:",inline"`
			CommonConfig `yaml:",inline"`
		}{
			CommonConfig: a.CommonConfig,
			S3:           a.S3,
		}, nil
	case dom.Swift:
		return struct {
			Swift        SwiftConf `yaml:",inline"`
			CommonConfig `yaml:",inline"`
		}{
			CommonConfig: a.CommonConfig,
			Swift:        a.Swift,
		}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported storage type %q", dom.ErrInvalidStorageConfig, a.Type)
	}
}

func (s StoragesConfig[S3Config, SwiftConfig]) ViperUnmarshallerHookFunc() mapstructure.DecodeHookFuncType {
	return func(
		f reflect.Type,
		t reflect.Type,
		data any,
	) (any, error) {
		if t != reflect.TypeOf(GenericStorage[S3Config, SwiftConfig]{}) {
			return data, nil
		}
		common := CommonConfig{}
		if err := viperDecode(data, &common); err != nil {
			return nil, err
		}
		switch common.Type {
		case dom.S3:
			s3conf := new(S3Config)
			if err := viperDecode(data, s3conf); err != nil {
				return nil, err
			}
			return GenericStorage[S3Config, SwiftConfig]{
				CommonConfig: common,
				S3:           *s3conf,
			}, nil
		case dom.Swift:
			swiftconf := new(SwiftConfig)
			if err := viperDecode(data, swiftconf); err != nil {
				return nil, err
			}
			return GenericStorage[S3Config, SwiftConfig]{
				CommonConfig: common,
				Swift:        *swiftconf,
			}, nil
		default:
			return nil, fmt.Errorf("%w: unsupported storage type %q", dom.ErrInvalidStorageConfig, common.Type)
		}
	}
}

func viperDecode(data any, result any) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.StringToTimeDurationHookFunc(),
		Result:     result,
	})
	if err != nil {
		return err
	}
	return decoder.Decode(data)
}
