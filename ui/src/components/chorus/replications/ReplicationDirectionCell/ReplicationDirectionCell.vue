<!--
  - Copyright © 2026 Clyso GmbH
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
  import { CTooltip, CTag, CIcon } from '@clyso/clyso-ui-kit';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import type { ChorusReplication } from '@/utils/types/chorus';
  import type { AddId } from '@/utils/types/helper';
  import { IconName } from '@/utils/types/icon';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  defineProps<{
    replication: AddId<ChorusReplication>;
  }>();
</script>

<template>
  <div class="replication-direction-cell">
    <CTooltip :delay="500">
      <template #trigger>
        <CTag
          class="replication-direction-cell__from"
          round
          type="success"
          size="small"
        >
          {{ replication.from }}
        </CTag>
      </template>
      <span class="replication-direction-cell__from-tooltip">
        <strong>{{ replication.from }}</strong
        >: {{ t('replicationFrom') }}
      </span>
    </CTooltip>

    <CIcon
      class="replication-direction-cell__arrow"
      :is-inline="true"
      :name="IconName.BASE_ARROW_FORWARD"
    />

    <CTooltip :delay="500">
      <template #trigger>
        <CTag
          round
          class="replication-direction-cell__to"
          type="warning"
          size="small"
        >
          {{ replication.to }}
        </CTag>
      </template>
      <span class="replication-direction-cell__to-tooltip">
        <strong>{{ replication.to }}</strong
        >: {{ t('replicationTo') }}
      </span>
    </CTooltip>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replication-direction-cell {
    display: flex;
    align-items: center;
    gap: utils.unit(2);

    > * {
      flex-shrink: 0;
    }
  }
</style>
