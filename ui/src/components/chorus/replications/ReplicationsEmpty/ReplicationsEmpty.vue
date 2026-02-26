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
  import { CResult, CIcon, CButton } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { useRouter } from 'vue-router';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { IconName } from '@/utils/types/icon';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import { RouteName } from '@/utils/types/router';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { isFiltered } = storeToRefs(useChorusReplicationsStore());

  const router = useRouter();

  const { clearFilters } = useChorusReplicationsStore();

  function handleAction() {
    if (!isFiltered.value) {
      router.push({ name: RouteName.CHORUS_ADD_REPLICATION });

      return;
    }

    clearFilters();
  }
</script>

<template>
  <CResult
    class="replications-empty"
    type="empty"
    size="large"
    :icon-name="IconName.BASE_SWAP_HORIZONTAL"
    :max-width="600"
  >
    <template #title>
      {{ t(isFiltered ? 'filterNoResultsTitle' : 'noResultsTitle') }}
    </template>

    <p>{{ t(isFiltered ? 'filterNoResultsText' : 'noResultsText') }}</p>

    <template #actions>
      <CButton
        type="primary"
        size="large"
        @click="handleAction"
      >
        <template #icon>
          <CIcon
            :is-inline="true"
            :name="isFiltered ? 'close' : IconName.BASE_ADD"
          />
        </template>

        {{ t(isFiltered ? 'filterNoResultsAction' : 'addReplicationAction') }}
      </CButton>
    </template>
  </CResult>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-empty {
    p {
      white-space: pre-line;
    }
  }
</style>
