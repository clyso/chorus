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
  import { CSelect } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import { computed } from 'vue';
  import i18nReplications from '../i18nReplications';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import { ReplicationType } from '@/utils/types/chorus';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { filterType, page } = storeToRefs(useChorusReplicationsStore());

  const typeOptions = computed<
    {
      label: string;
      value: ReplicationType;
    }[]
  >(() => [
    {
      value: ReplicationType.BUCKET,
      label: t('filterTypeBucket'),
    },
    {
      value: ReplicationType.USER,
      label: t('filterTypeUser'),
    },
  ]);
</script>

<template>
  <CSelect
    v-model:value="filterType"
    class="replications-filter-by-type"
    clearable
    :placeholder="t('filterByTypePlaceholder')"
    :options="typeOptions"
    :max-tag-count="1"
    @update:value="page = 1"
  />
</template>
