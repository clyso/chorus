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
  import {
    CDataTable,
    type DataTableBaseColumn,
    type DataTableSelectionColumn,
    type DataTableSortState,
  } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { computed } from 'vue';
  import { useI18n } from 'vue-i18n';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import RoutingPolicyStatusCell from '../RoutingPolicyStatusCell/RoutingPolicyStatusCell.vue';
  import RoutingPolicyStorageCell from '../RoutingPolicyStorageCell/RoutingPolicyStorageCell.vue';
  import RoutingPolicyActionsCell from '../RoutingPolicyActionsCell/RoutingPolicyActionsCell.vue';
  import RoutingPolicyBucketCell from '../RoutingPolicyBucketCell/RoutingPolicyBucketCell.vue';
  import type { RoutingPolicy } from '@/utils/types/chorus';
  import { useChorusRoutingPoliciesStore } from '@/stores/chorusRoutingPoliciesStore';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const {
    isLoading,
    computedRoutingPolicies,
    selectedRoutingPolicyIds,
    hasError,
    sorter,
    page,
    pageSize,
    pagination,
  } = storeToRefs(useChorusRoutingPoliciesStore());

  const { initRoutingPoliciesPage } = useChorusRoutingPoliciesStore();

  const columns = computed<
    (DataTableBaseColumn<RoutingPolicy> | DataTableSelectionColumn)[]
  >(() => [
    {
      type: 'selection',
    },
    {
      title: t('columnUser'),
      key: 'user',
      sorter: true,
    },
    {
      title: t('columnBucket'),
      key: 'bucket',
      sorter: true,
    },
    {
      title: t('columnStorage'),
      key: 'storage',
      sorter: true,
    },
    {
      title: t('columnStatus'),
      key: 'isBlocked',
      sorter: true,
    },
    {
      title: t('columnActions'),
      key: 'actions',
    },
  ]);

  const handleSortingChange = (newSorter: DataTableSortState | null) => {
    if (!computedRoutingPolicies.value.length) {
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

  const rowKey = (row: RoutingPolicy) => row.id;
</script>

<template>
  <div class="routing-policies-list">
    <div class="routing-policies-list__container">
      <CDataTable
        v-model:checked-row-keys="selectedRoutingPolicyIds"
        class="routing-policies-list__table"
        :columns="columns"
        :data="computedRoutingPolicies"
        selectable
        max-height="1020px"
        virtual-scroll
        :is-controlled="true"
        :is-loading="isLoading"
        :has-error="hasError"
        :pagination="pagination"
        :bordered="false"
        :sorter="sorter"
        :row-key="rowKey"
        @update:sorter="handleSortingChange"
        @update:page="handlePageChange"
        @update:page-size="handlePageSizeUpdate"
        @retry="initRoutingPoliciesPage"
      >
        <template #user="{ rowData }">
          {{ rowData.user }}
        </template>

        <template #bucket="{ rowData }">
          <RoutingPolicyBucketCell :routing-policy="rowData" />
        </template>

        <template #storage="{ rowData }">
          <RoutingPolicyStorageCell :routing-policy="rowData" />
        </template>

        <template #isBlocked="{ rowData }">
          <RoutingPolicyStatusCell :routing-policy="rowData" />
        </template>

        <template #actions="{ rowData }">
          <RoutingPolicyActionsCell :routing-policy="rowData" />
        </template>
      </CDataTable>
    </div>
  </div>
</template>
