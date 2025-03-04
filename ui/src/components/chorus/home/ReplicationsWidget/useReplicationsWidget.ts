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

import { storeToRefs } from 'pinia';
import { useRouter } from 'vue-router';
import { RouteName } from '@/utils/types/router';
import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
import { ReplicationStatusFilter } from '@/utils/types/chorus';

export const useReplicationsWidget = () => {
  const { filterStatuses } = storeToRefs(useChorusReplicationsStore());
  const router = useRouter();

  const goToReplicationsPage = () => {
    router.push({ name: RouteName.CHORUS_REPLICATION });
  };

  const showActiveReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.ACTIVE];
    goToReplicationsPage();
  };

  const showPausedReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.PAUSED];
    goToReplicationsPage();
  };

  const showUpToDateReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.LIVE_UP_TO_DATE];
    router.push({ name: RouteName.CHORUS_REPLICATION });
  };

  const showInProgressReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.INITIAL_IN_PROGRESS];
    router.push({ name: RouteName.CHORUS_REPLICATION });
  };

  const showBehindReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.LIVE_BEHIND];
    router.push({ name: RouteName.CHORUS_REPLICATION });
  };

  return {
    goToReplicationsPage,
    showActiveReplications,
    showPausedReplications,
    showUpToDateReplications,
    showInProgressReplications,
    showBehindReplications,
  };
};
