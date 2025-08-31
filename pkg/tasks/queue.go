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
	string(QueueConsistencyCheck):                50,
	string(QueueMigrateCopyObjectPrefix) + ":*":  10,
	string(QueueEventsPrefix) + ":*":             5, // lowest priority
	"*":                                          1, // fallback for legacy queues
}

func replicationQueueName(queuePrefix Queue, id entity.ReplicationStatusID) string {
	switch queuePrefix {
	case QueueMigrateCopyObjectPrefix,
		QueueMigrateListObjectsPrefix,
		QueueEventsPrefix:
		return fmt.Sprintf("%s:%s:%s:%s:%s", queuePrefix, id.FromStorage, id.FromBucket, id.ToStorage, id.ToBucket)
	default:
		panic(fmt.Sprintf("%s is not a replication queue prefix", queuePrefix))
	}
}

func InitMigrationQueues(id entity.ReplicationStatusID) []string {
	return []string{
		replicationQueueName(QueueMigrateListObjectsPrefix, id),
		replicationQueueName(QueueMigrateCopyObjectPrefix, id),
	}
}

func EventMigrationQueues(id entity.ReplicationStatusID) []string {
	return []string{
		replicationQueueName(QueueEventsPrefix, id),
	}
}

func AllReplicationQueues(id entity.ReplicationStatusID) []string {
	return append(InitMigrationQueues(id), EventMigrationQueues(id)...)
}
