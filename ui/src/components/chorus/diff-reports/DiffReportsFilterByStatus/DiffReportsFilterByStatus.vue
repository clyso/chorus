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
  import { computed } from 'vue';
  import { CSelect } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import i18nDiffReports from '@/components/chorus/diff-reports/i18nDiffReports';
  import { useChorusDiffReportsStore } from '@/stores/chorusDiffReportsStore';
  import { DiffReportStatusFilter } from '@/utils/types/chorus';

  const { t } = useI18n({
    messages: i18nDiffReports,
  });

  const { filterStatuses, page } = storeToRefs(useChorusDiffReportsStore());

  const statusOptions = computed<
    {
      label: string;
      value: DiffReportStatusFilter;
    }[]
  >(() => [
    {
      value: DiffReportStatusFilter.CHECKING,
      label: t('filterStatusChecking'),
    },
    {
      value: DiffReportStatusFilter.CONSISTENT,
      label: t('filterStatusConsistent'),
    },
    {
      value: DiffReportStatusFilter.INCONSISTENT,
      label: t('filterStatusInconsistent'),
    },
  ]);
</script>

<template>
  <CSelect
    v-model:value="filterStatuses"
    clearable
    multiple
    :placeholder="t('filterByStatusPlaceholder')"
    :options="statusOptions"
    :max-tag-count="1"
    @update:value="page = 1"
  />
</template>
