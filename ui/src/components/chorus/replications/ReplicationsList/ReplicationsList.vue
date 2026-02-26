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
  import { GeneralHelper } from '../../../../utils/helpers/GeneralHelper';
  import ReplicationBucketCell from '../ReplicationBucketCell/ReplicationBucketCell.vue';
  import type { ChorusReplication } from '@/utils/types/chorus';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';
  import type { AddId } from '@/utils/types/helper';
  import ReplicationsError from '@/components/chorus/replications/ReplicationsError/ReplicationsError.vue';
  import ReplicationStatusCell from '@/components/chorus/replications/ReplicationStatusCell/ReplicationStatusCell.vue';
  import ReplicationsEmpty from '@/components/chorus/replications/ReplicationsEmpty/ReplicationsEmpty.vue';
  import ReplicationDirectionCell from '@/components/chorus/replications/ReplicationDirectionCell/ReplicationDirectionCell.vue';
  import ReplicationActionsCell from '@/components/chorus/replications/ReplicationActionsCell/ReplicationActionsCell.vue';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const {
    isLoading,
    isSelectedProcessing,
    hasError,
    computedReplications,
    sorter,
    page,
    pageSize,
    pagination,
    selectedReplicationIds,
  } = storeToRefs(useChorusReplicationsStore());
  const { initReplicationsPage } = useChorusReplicationsStore();

  const columns = computed<
    (DataTableBaseColumn<AddId<ChorusReplication>> | DataTableSelectionColumn)[]
  >(() => [
    {
      type: 'selection',
    },
    {
      title: t('columnUser'),
      key: 'id.user',
      width: '15%',
      sorter: true,
    },
    {
      title: t('columnBucket'),
      key: 'bucket',
      width: '20%',
    },
    {
      title: t('columnCreatedAt'),
      key: 'createdAt',
      sorter: true,
    },
    {
      title: t('columnDirection'),
      key: 'direction',
    },
    {
      title: t('columnStatus'),
      key: 'status',
      width: '25%',
    },
    {
      title: t('columnActions'),
      key: 'actions',
    },
  ]);

  const handleSortingChange = (newSorter: DataTableSortState | null) => {
    if (!computedReplications.value.length) {
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

  const rowKey = (row: AddId<ChorusReplication>) => row.idStr;
</script>

<template>
  <div class="replications-list">
    <div class="replications-list__container">
      <CDataTable
        v-model:checked-row-keys="selectedReplicationIds"
        class="replications-list__table"
        :columns="columns"
        :data="computedReplications"
        selectable
        max-height="1020px"
        virtual-scroll
        :is-controlled="true"
        :is-loading="isLoading || isSelectedProcessing"
        :has-error="hasError"
        :pagination="pagination"
        :bordered="false"
        :sorter="sorter"
        :row-key="rowKey"
        @update:sorter="handleSortingChange"
        @update:page="handlePageChange"
        @update:page-size="handlePageSizeUpdate"
        @retry="initReplicationsPage"
      >
        <template #bucket="{ rowData }: { rowData: AddId<ChorusReplication> }">
          <ReplicationBucketCell :replication="rowData" />
        </template>

        <template #createdAt="{ rowData }">
          {{ GeneralHelper.formatDateTime(rowData.createdAt) }}
        </template>

        <template #direction="{ rowData }">
          <ReplicationDirectionCell
            :replication="rowData as AddId<ChorusReplication>"
          />
        </template>

        <template #status="{ rowData }">
          <ReplicationStatusCell
            :replication="rowData as AddId<ChorusReplication>"
          />
        </template>

        <template #actions="{ rowData }">
          <ReplicationActionsCell
            :replication="rowData as AddId<ChorusReplication>"
          />
        </template>

        <template #error>
          <ReplicationsError class="replications-list__result" />
        </template>

        <template #empty>
          <ReplicationsEmpty class="replications-list__result" />
        </template>
      </CDataTable>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-list {
    &__container {
      overflow-x: auto;
    }

    &__table {
      min-width: 1100px;
    }

    &__result {
      min-height: 400px;
    }
  }
</style>
