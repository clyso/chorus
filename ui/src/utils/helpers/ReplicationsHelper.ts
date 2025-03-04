/*
 * Copyright © 2025 Clyso GmbH
 *
 *  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  https://www.gnu.org/licenses/agpl-3.0.html
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import type { AddId } from '@/utils/types/helper';
import type { ChorusReplication } from '@/utils/types/chorus';
import { ReplicationStatusFilter } from '@/utils/types/chorus';

export abstract class ReplicationsHelper {
  static getLiveReplicationEventsDifference(
    replication: ChorusReplication,
  ): number {
    const { events, eventsDone } = replication;

    if (!events) {
      return 0;
    }

    return Number(events) - Number(eventsDone);
  }

  static isLiveReplicationBehind(replication: ChorusReplication): boolean {
    return this.getLiveReplicationEventsDifference(replication) > 0;
  }

  static isReplicationStatusMatched(
    replication: AddId<ChorusReplication>,
    status: ReplicationStatusFilter | null,
  ): boolean {
    if (status === ReplicationStatusFilter.ACTIVE) {
      return !replication.isPaused;
    }

    if (status === ReplicationStatusFilter.PAUSED) {
      return replication.isPaused;
    }

    if (status === ReplicationStatusFilter.INITIAL_IN_PROGRESS) {
      return !replication.isInitDone;
    }

    if (status === ReplicationStatusFilter.INITIAL_DONE) {
      return replication.isInitDone;
    }

    const isLiveBehind = this.isLiveReplicationBehind(replication);

    if (status === ReplicationStatusFilter.LIVE_UP_TO_DATE) {
      return !isLiveBehind && replication.isInitDone;
    }

    if (status === ReplicationStatusFilter.LIVE_BEHIND) {
      return isLiveBehind && replication.isInitDone;
    }

    return false;
  }

  static isReplicationCreateAtMatched(
    replication: AddId<ChorusReplication>,
    [rangeStartMs, rangeEndMs]: [number, number],
  ): boolean {
    const createdAtMs = new Date(replication.createdAt).getTime();

    return createdAtMs >= rangeStartMs && createdAtMs <= rangeEndMs;
  }
}
