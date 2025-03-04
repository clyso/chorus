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
  import { storeToRefs } from 'pinia';
  import { CDateRangePicker } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';

  const { filterCreatedAtRange, page } = storeToRefs(
    useChorusReplicationsStore(),
  );

  const { t } = useI18n({
    messages: i18nReplications,
  });
</script>

<template>
  <CDateRangePicker
    v-model:value="filterCreatedAtRange"
    class="replications-filter-by-created-at"
    format="d MMM yyyy"
    :start-placeholder="t('filterByCreatedAtStartPlaceholder')"
    :end-placeholder="t('filterByCreatedAtEndPlaceholder')"
    clearable
    @update:value="page = 1"
  />
</template>
