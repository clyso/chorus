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
  import { storeToRefs } from 'pinia';
  import { useI18n } from 'vue-i18n';
  import { useChorusDiffReportEntriesStore } from '@/stores/chorusDiffReportEntriesStore';
  import i18nDiffReportDetail from '@/components/chorus/diff-report-detail/i18nDiffReportDetail';
  import DiffReportDetailEntriesList from '@/components/chorus/diff-report-detail/DiffReportDetailEntriesList/DiffReportDetailEntriesList.vue';
  import DiffReportDetailEntriesFilters from '@/components/chorus/diff-report-detail/DiffReportDetailEntriesFilters/DiffReportDetailEntriesFilters.vue';

  const { t } = useI18n({ messages: i18nDiffReportDetail });
  const entriesStore = useChorusDiffReportEntriesStore();
  const { inconsistentObjectsCount, hasError, isLoading } =
    storeToRefs(entriesStore);
</script>

<template>
  <div class="diff-report-detail-entries">
    <h5>
      {{ t('entriesTitle') }} ({{
        hasError || isLoading ? '-' : inconsistentObjectsCount
      }})
    </h5>

    <DiffReportDetailEntriesFilters
      class="diff-report-detail-entries__filters"
    />

    <DiffReportDetailEntriesList class="diff-report-detail-entries__entries" />
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-report-detail-entries {
    h5 {
      margin-bottom: utils.unit(3);
    }

    &__filters {
      margin-bottom: utils.unit(6);
    }
  }
</style>
