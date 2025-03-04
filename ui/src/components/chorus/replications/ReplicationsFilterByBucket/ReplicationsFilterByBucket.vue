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
  import { computed, ref, watch } from 'vue';
  import { CAutoComplete } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import { GeneralHelper } from '@/utils/helpers/GeneralHelper';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const searchString = ref('');
  const select = ref<InstanceType<typeof CAutoComplete> | null>(null);

  const { replications, filterBucket, page } = storeToRefs(
    useChorusReplicationsStore(),
  );

  const bucketOptions = computed<string[]>(() => {
    const uniqueBucketOptions = [
      ...new Set(replications.value.map((replication) => replication.bucket)),
    ];

    return uniqueBucketOptions
      .filter((bucket) =>
        bucket
          .trim()
          .toLowerCase()
          .includes(searchString.value.trim().toLowerCase()),
      )
      .sort();
  });

  const handleValueUpdate = GeneralHelper.debounce(() => {
    page.value = 1;
    filterBucket.value = searchString.value;
  }, 1000);

  watch(filterBucket, () => {
    if (filterBucket.value === '') {
      searchString.value = '';
    }
  });
</script>

<template>
  <CAutoComplete
    ref="select"
    v-model:value="searchString"
    class="replications-filter-by-bucket"
    :input-props="{
      autocomplete: 'disabled',
    }"
    :options="bucketOptions"
    :placeholder="t('filterByBucketPlaceholder')"
    clearable
    blur-after-select
    @update:value="handleValueUpdate"
  />
</template>
