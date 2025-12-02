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

package tasks

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/clyso/chorus/pkg/entity"
)

func eventQueue(p ReplicationTask) string {
	return replicationQueueName(QueueEventsPrefix, p.GetReplicationID())
}
func initMigrationListQueue(p ReplicationTask) string {
	return replicationQueueName(QueueMigrateListObjectsPrefix, p.GetReplicationID())
}

func initMigrationCopyQueue(p ReplicationTask) string {
	return replicationQueueName(QueueMigrateCopyObjectPrefix, p.GetReplicationID())
}

type Queue string

const (
	QueueAPI                      Queue = "api"
	QueueMigrateListObjectsPrefix Queue = "migr_list_obj"
	QueueConsistencyCheck         Queue = "consistency_check"
	QueueMigrateCopyObjectPrefix  Queue = "migr_copy_obj"
	QueueEventsPrefix             Queue = "event"
)

// Priority defines the priority of the queues from highest to lowest.
var Priority = map[string]int{
	string(QueueAPI): 200, // highest priority
	string(QueueMigrateListObjectsPrefix) + ":*": 100,
	string(QueueConsistencyCheck) + ":*":         50,
	string(QueueMigrateCopyObjectPrefix) + ":*":  10,
	string(QueueEventsPrefix) + ":*":             5, // lowest priority
	"*":                                          1, // fallback for legacy queues
}

func replicationQueueName(queuePrefix Queue, id entity.UniversalReplicationID) string {
	switch queuePrefix {
	case QueueMigrateCopyObjectPrefix,
		QueueMigrateListObjectsPrefix,
		QueueEventsPrefix:
		return fmt.Sprintf("%s:%s", queuePrefix, id.AsString())
	default:
		panic(fmt.Sprintf("%s is not a replication queue prefix", queuePrefix))
	}
}

func InitMigrationListQueue(id entity.UniversalReplicationID) string {
	return replicationQueueName(QueueMigrateListObjectsPrefix, id)
}

func InitMigrationCopyQueue(id entity.UniversalReplicationID) string {
	return replicationQueueName(QueueMigrateCopyObjectPrefix, id)
}

func EventMigrationQueue(id entity.UniversalReplicationID) string {
	return replicationQueueName(QueueEventsPrefix, id)
}

func AllReplicationQueues(id entity.UniversalReplicationID) []string {
	return []string{
		InitMigrationListQueue(id),
		InitMigrationCopyQueue(id),
		EventMigrationQueue(id),
	}
}

func ConsistencyCheckQueue(id entity.ConsistencyCheckID) string {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	if err := json.NewEncoder(encoder).Encode(&id); err != nil {
		panic(fmt.Errorf("unable to encode consistency check id: %w", err))
	}
	// add prefix
	return fmt.Sprintf("%s:%s", QueueConsistencyCheck, buf.String())
}
