<!--
  - Copyright © 2025 Clyso GmbH
  -
  -  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
  -  you may not use this file except in compliance with the License.
  -  You may obtain a copy of the License at
  -
  -  https://www.gnu.org/licenses/agpl-3.0.html
  -
  -  Unless required by applicable law or agreed to in writing, software
  -  distributed under the License is distributed on an "AS IS" BASIS,
  -  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  -  See the License for the specific language governing permissions and
  -  limitations under the License.
  -->

<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { CTooltip, CTag } from '@clyso/clyso-ui-kit';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import type { AddId } from '@/utils/types/helper';
  import type { ChorusReplication } from '@/utils/types/chorus';
  import { ReplicationType } from '@/utils/types/chorus';
  import ChorusDirectionCell from '@/components/chorus/common/ChorusDirectionCell/ChorusDirectionCell.vue';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  defineProps<{
    replication: AddId<ChorusReplication>;
  }>();
</script>

<template>
  <div class="replication-bucket-cell">
    <div
      class="replication-bucket-cell__user"
      v-if="replication.replicationType === ReplicationType.USER"
    >
      <CTooltip :delay="500">
        <template #trigger>
          <CTag
            class="replication-bucket-cell__user-replication-tag"
            round
            type="success"
            size="small"
          >
            {{ t('userReplication') }}
          </CTag>
        </template>
        <span class="replication-bucket-cell__user-replication-tooltip">
          {{ t('userReplicationDescription') }}
        </span>
      </CTooltip>
    </div>
    <div
      class="replication-bucket-cell__bucket"
      v-else-if="replication.id.fromBucket && replication.id.toBucket"
    >
      <ChorusDirectionCell
        :from-text="replication.id.fromBucket"
        :to-text="replication.id.toBucket"
        :from-tooltip="t('replicationFrom')"
        :to-tooltip="t('replicationTo')"
        to-type="warning"
      />
    </div>
  </div>
</template>
