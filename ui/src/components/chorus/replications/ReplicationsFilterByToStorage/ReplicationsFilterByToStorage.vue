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
  import { computed, watch } from 'vue';
  import { CResult, CSelect } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { replications, filterToStorages, page } = storeToRefs(
    useChorusReplicationsStore(),
  );

  const toOptions = computed<
    {
      label: string;
      value: string;
    }[]
  >(() => {
    const sortedUniqueTo = [
      ...new Set(replications.value.map((replication) => replication.to)),
    ].sort();

    return sortedUniqueTo.map((to) => ({ label: to, value: to }));
  });

  function sanitizeFilterTo() {
    filterToStorages.value = filterToStorages.value.filter((filterToOption) =>
      toOptions.value.map((to) => to.label).includes(filterToOption),
    );
  }

  watch(toOptions, sanitizeFilterTo);
</script>

<template>
  <CSelect
    v-model:value="filterToStorages"
    class="replications-filter-by-to-storage"
    multiple
    filterable
    clearable
    :placeholder="t('filterByToPlaceholder')"
    :options="toOptions"
    :max-tag-count="1"
    @update:value="page = 1"
  >
    <template #empty>
      <CResult
        type="empty"
        size="tiny"
      >
        <template v-if="toOptions.length">
          {{ t('filterByToNoResult') }}
        </template>
      </CResult>
    </template>
  </CSelect>
</template>
