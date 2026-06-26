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
  import type {
    DataTableBaseColumn,
    DataTableSelectionColumn,
    DataTableSortState,
  } from '@clyso/clyso-ui-kit';
  import { CDataTable } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import { storeToRefs } from 'pinia';
  import { useI18n } from 'vue-i18n';
  import i18nDiffReports from '@/components/chorus/diff-reports/i18nDiffReports';
  import type { DiffReport, DiffReportLocation } from '@/utils/types/chorus';
  import type { AddId } from '@/utils/types/helper';
  import { useChorusDiffReportsStore } from '@/stores/chorusDiffReportsStore';
  import { DiffReportsHelper } from '@/utils/helpers/DiffReportsHelper';
  import DiffReportsEmpty from '@/components/chorus/diff-reports/DiffReportsEmpty/DiffReportsEmpty.vue';
  import DiffReportsBucketCell from '@/components/chorus/diff-reports/DiffReportsBucketCell/DiffReportsBucketCell.vue';
  import DiffReportsDirectionCell from '@/components/chorus/diff-reports/DiffReportsDirectionCell/DiffReportsDirectionCell.vue';
  import DiffReportsProgressCell from '@/components/chorus/diff-reports/DiffReportsProgressCell/DiffReportsProgressCell.vue';
  import DiffReportsStatusCell from '@/components/chorus/diff-reports/DiffReportsStatusCell/DiffReportsStatusCell.vue';
  import DiffReportsConfigsCell from '@/components/chorus/diff-reports/DiffReportsConfigsCell/DiffReportsConfigsCell.vue';
  import ChorusListError from '@/components/chorus/common/ChorusListError/ChorusListError.vue';
  import DiffReportsActionsCell from '@/components/chorus/diff-reports/DiffReportsActionsCell/DiffReportsActionsCell.vue';

  const { t } = useI18n({
    messages: i18nDiffReports,
  });

  const {
    isLoading,
    hasError,
    hasNoData,
    computedReports,
    sorter,
    page,
    pageSize,
    pagination,
    selectedReportIds,
  } = storeToRefs(useChorusDiffReportsStore());

  const { initDiffReportPage } = useChorusDiffReportsStore();

  const columns = computed<
    (DataTableBaseColumn<AddId<DiffReport>> | DataTableSelectionColumn)[]
  >(() => [
    {
      type: 'selection',
    },
    {
      title: t('columnDirection'),
      key: 'direction',
    },
    {
      title: t('columnBucket'),
      key: 'bucket',
    },
    {
      title: t('columnProgress'),
      key: 'progress',
    },
    {
      title: t('columnStatus'),
      key: 'status',
      sorter: true,
    },
    {
      title: t('columnConfigs'),
      key: 'configs',
    },
    {
      title: t('columnActions'),
      key: 'actions',
    },
  ]);

  function getLocations(
    report: DiffReport,
  ): [DiffReportLocation, DiffReportLocation] {
    return report.locations as [DiffReportLocation, DiffReportLocation];
  }

  const handleSortingChange = (newSorter: DataTableSortState | null) => {
    if (hasNoData.value) {
      return;
    }

    sorter.value = newSorter;
    page.value = 1;
  };

  const handlePageChange = (newPage: number) => {
    page.value = newPage;
  };

  const handlePageSizeUpdate = (newPageSize: number) => {
    pageSize.value = newPageSize;
    page.value = 1;
  };

  const rowKey = (row: AddId<DiffReport>) => row.idStr;
</script>

<template>
  <div class="diff-reports-list">
    <div class="diff-reports-list__container">
      <CDataTable
        class="diff-reports-list__table"
        :columns="columns"
        :data="computedReports"
        max-height="1020px"
        virtual-scroll
        :is-controlled="true"
        :is-loading="isLoading"
        :has-error="hasError"
        :pagination="pagination"
        :bordered="false"
        :sorter="sorter"
        :row-key="rowKey"
        v-model:checked-row-keys="selectedReportIds"
        @update:sorter="handleSortingChange"
        @update:page="handlePageChange"
        @update:page-size="handlePageSizeUpdate"
        @retry="initDiffReportPage"
      >
        <template #direction="{ rowData }: { rowData: AddId<DiffReport> }">
          <span
            v-if="!DiffReportsHelper.isTwoLocationReport(rowData)"
            class="diff-reports-list__location-warning"
          >
            {{ t('cliCheckWarning') }}
          </span>
          <DiffReportsDirectionCell
            v-else
            :locations="getLocations(rowData)"
          />
        </template>

        <template #bucket="{ rowData }: { rowData: AddId<DiffReport> }">
          <DiffReportsBucketCell
            v-if="DiffReportsHelper.isTwoLocationReport(rowData)"
            :locations="getLocations(rowData)"
          />
        </template>

        <template #progress="{ rowData }: { rowData: AddId<DiffReport> }">
          <DiffReportsProgressCell
            v-if="DiffReportsHelper.isTwoLocationReport(rowData)"
            :report="rowData"
          />
        </template>

        <template #status="{ rowData }: { rowData: AddId<DiffReport> }">
          <DiffReportsStatusCell
            v-if="DiffReportsHelper.isTwoLocationReport(rowData)"
            :report="rowData"
          />
        </template>

        <template #configs="{ rowData }: { rowData: AddId<DiffReport> }">
          <DiffReportsConfigsCell
            v-if="DiffReportsHelper.isTwoLocationReport(rowData)"
            :report="rowData"
          />
        </template>

        <template #actions="{ rowData }: { rowData: AddId<DiffReport> }">
          <DiffReportsActionsCell
            v-if="DiffReportsHelper.isTwoLocationReport(rowData)"
            :report="rowData"
          />
        </template>

        <template #error>
          <ChorusListError
            class="diff-reports-list__result"
            :title="t('errorTitle')"
            :text="t('errorText')"
            :action-text="t('errorAction')"
            @retry="initDiffReportPage"
          />
        </template>

        <template #empty>
          <DiffReportsEmpty class="diff-reports-list__result" />
        </template>
      </CDataTable>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-reports-list {
    &__container {
      overflow-x: auto;
    }

    &__table {
      min-width: 850px;
    }

    &__result {
      min-height: 400px;
    }
  }
</style>
