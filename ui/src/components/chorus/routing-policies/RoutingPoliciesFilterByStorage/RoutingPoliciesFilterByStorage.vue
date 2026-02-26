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
  import { storeToRefs } from 'pinia';
  import { computed, watch } from 'vue';
  import { CResult, CSelect } from '@clyso/clyso-ui-kit';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import { useChorusRoutingPoliciesStore } from '@/stores/chorusRoutingPoliciesStore';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const { routingPolicies, filterStorages, page } = storeToRefs(
    useChorusRoutingPoliciesStore(),
  );

  const storageOptions = computed<
    {
      label: string;
      value: string;
    }[]
  >(() => {
    const sortedUniqueStorage = [
      ...new Set(
        routingPolicies.value.map((routingPolicy) => routingPolicy.toStorage),
      ),
    ].sort();

    return sortedUniqueStorage.map((storage) => ({
      label: storage,
      value: storage,
    }));
  });

  function sanitizeFilterStorages() {
    filterStorages.value = filterStorages.value.filter((filterStorage) =>
      storageOptions.value
        .map((storage) => storage.label)
        .includes(filterStorage),
    );
  }

  watch(storageOptions, sanitizeFilterStorages);
</script>

<template>
  <CSelect
    v-model:value="filterStorages"
    class="routing-policies-filter-by-storage"
    multiple
    filterable
    clearable
    :placeholder="t('filterByStoragePlaceholder')"
    :options="storageOptions"
    :max-tag-count="1"
    @update:value="page = 1"
  >
    <template #empty>
      <CResult
        type="empty"
        size="tiny"
      >
        <template v-if="storageOptions.length">
          {{ t('filterByToNoResult') }}
        </template>
      </CResult>
    </template>
  </CSelect>
</template>
