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

package api

import (
	"fmt"
	"time"

	"github.com/adhocore/gronx"

	"github.com/clyso/chorus/pkg/dom"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func validateSwitchDonwtimeOpts(in *pb.SwitchDowntimeOpts) error {
	if in == nil {
		return nil
	}
	if in.Cron != nil && in.StartAt != nil {
		return fmt.Errorf("%w: either cron or start_at can be set, but not both", dom.ErrInvalidArg)
	}
	if in.Cron != nil && !gronx.IsValid(*in.Cron) {
		return fmt.Errorf("%w: invalid cron expression %q", dom.ErrInvalidArg, *in.Cron)
	}
	if in.StartAt != nil && in.StartAt.AsTime().Before(time.Now()) {
		return fmt.Errorf("%w: start_at is in the past according to server time: %q", dom.ErrInvalidArg, time.Now().Format(time.RFC3339))
	}
	return nil
}
