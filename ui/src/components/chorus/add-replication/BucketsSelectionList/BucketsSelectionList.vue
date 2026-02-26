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
  import {
    CCollapseTransition,
    CDataTable,
    CInput,
    CResult,
    CSwitch,
    CTag,
    type DataTableBaseColumn,
    type DataTableRowData,
    type DataTableSelectionColumn,
  } from '@clyso/clyso-ui-kit';
  import { computed, ref } from 'vue';
  import { useI18n } from 'vue-i18n';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';
  import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
  import { IconName } from '@/utils/types/icon';

  interface BucketRowData {
    name: string;
    isReplicated: boolean;
  }

  const {
    bucketsList,
    replicatedBucketsList,
    selectedBuckets,
    isBucketsListLoading,
    hasBucketsError,
    isReplicatedShown,
    selectedUser,
  } = storeToRefs(useChorusAddReplicationStore());
  const { initBucketsList } = useChorusAddReplicationStore();

  const { t } = useI18n({
    messages: i18nAddReplication,
  });

  const columns = ref<
    (DataTableBaseColumn<BucketRowData> | DataTableSelectionColumn)[]
  >([
    {
      type: 'selection',
      disabled(row: DataTableRowData) {
        return row.isReplicated;
      },
    },
    {
      title: t('columnName'),
      key: 'name',
      width: '50%',
      sorter: 'default',
    },
    {
      title: t('columnStatus'),
      key: 'status',
    },
  ]);

  const bucketsSearchString = ref<string>('');

  function getBucketRowList(
    nameList: string[],
    isReplicated: boolean,
  ): BucketRowData[] {
    return nameList
      .map((bucketName) => ({ name: bucketName, isReplicated }))
      .filter((bucketsRow) =>
        bucketsRow.name
          .toLowerCase()
          .includes(bucketsSearchString.value.toLowerCase()),
      );
  }

  const bucketsRows = computed<BucketRowData[]>(() => {
    const availableBucketsRows: BucketRowData[] = getBucketRowList(
      bucketsList.value,
      false,
    );

    if (!isReplicatedShown.value) {
      return GeneralHelper.orderBy(availableBucketsRows, 'name', 'asc');
    }

    const replicatedBucketsRows: BucketRowData[] = getBucketRowList(
      replicatedBucketsList.value,
      true,
    );

    return GeneralHelper.orderBy(
      [...availableBucketsRows, ...replicatedBucketsRows],
      'name',
      'asc',
    );
  });

  const rowKey = (row: BucketRowData) => row.name;
</script>

<template>
  <div class="buckets-selection-list">
    <div class="buckets-filters">
      <div class="buckets-filters__item">
        <CInput
          v-model:value="bucketsSearchString"
          size="small"
          clearable
          :placeholder="t('searchBuckets')"
        />
      </div>
      <div class="buckets-filters__item">
        <div
          tabindex="0"
          class="switch-block"
          @click="isReplicatedShown = !isReplicatedShown"
          @keydown.enter="isReplicatedShown = !isReplicatedShown"
        >
          <CSwitch
            :value="isReplicatedShown"
            size="small"
            tabindex="-1"
            @keydown.enter="isReplicatedShown = !isReplicatedShown"
          />
          <span>{{ t('showReplicatedBuckets') }}</span>
        </div>
      </div>
    </div>

    <CCollapseTransition
      class="tag-list"
      :show="selectedBuckets.length !== 0"
    >
      <CTag
        v-if="selectedBuckets.length !== 0"
        round
        closable
        type="info"
        class="tag-list__selected-tag"
        @close="selectedBuckets = []"
      >
        {{ t('selectedBuckets', { total: selectedBuckets.length }) }}
      </CTag>
    </CCollapseTransition>

    <div class="buckets-selection-list__container">
      <CDataTable
        v-model:checked-row-keys="selectedBuckets"
        class="buckets-list"
        :columns="columns"
        :data="bucketsRows"
        selectable
        virtual-scroll
        min-height="426px"
        max-height="426px"
        :row-key="rowKey"
        :is-loading="isBucketsListLoading"
        :has-error="hasBucketsError"
      >
        <template #name="{ rowData }">
          <span
            :class="{ 'buckets-list__name--disabled': rowData.isReplicated }"
            class="buckets-list__name"
          >
            {{ rowData.name }}
          </span>
        </template>

        <template #status="{ rowData }">
          <CTag
            class="buckets-list__tag"
            :class="{ 'buckets-list__tag--disabled': rowData.isReplicated }"
            :type="rowData.isReplicated ? 'default' : 'info'"
            round
            size="small"
          >
            {{
              t(rowData.isReplicated ? 'statusReplicated' : 'statusAvailable')
            }}
          </CTag>
        </template>

        <template #error>
          <CResult
            v-if="hasBucketsError"
            key="error"
            class="buckets-selection-list__error"
            type="error"
            :max-width="400"
            @positive-click="initBucketsList"
          >
            <template #title>
              {{ t('errorTitle') }}
            </template>

            <p>{{ t('initBucketsError') }}</p>

            <template #positive-text>
              {{ t('errorAction') }}
            </template>
          </CResult>
        </template>

        <template #empty>
          <CResult
            v-if="!bucketsSearchString"
            class="buckets-selection-list__empty"
            type="empty"
            :icon-name="IconName.BASE_CYLINDER"
            :max-width="600"
          >
            <template #title>
              {{ t('noResultsTitle') }}
            </template>

            <p>{{ t('noResultsText', { user: selectedUser }) }}</p>
          </CResult>
          <CResult
            v-else
            class="buckets-selection-list__empty-filter"
            type="empty"
            :max-width="600"
            :icon-name="IconName.BASE_CYLINDER"
            @positive-click="bucketsSearchString = ''"
          >
            <template #title>
              {{ t('filterNoResultsTitle') }}
            </template>

            <p>{{ t('filterNoResultsText') }}</p>

            <template #positive-text>
              {{ t('filterNoResultsAction') }}
            </template>
          </CResult>
        </template>
      </CDataTable>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .buckets-selection-list {
    &__container {
      overflow-x: auto;
    }
  }

  .buckets-filters {
    padding: utils.unit(3);
    border-radius: utils.$border-radius;
    background-color: var(--filters-card-color);
    border: 1px solid var(--border-color);
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
    gap: utils.unit(3) utils.unit(5);
    align-items: center;
    margin-bottom: utils.unit(4);

    @include utils.mobile {
      padding: 0;
      border-radius: 0;
      background-color: unset;
      border: 0;
    }
  }

  .switch-block {
    display: flex;
    align-items: center;
    gap: utils.unit(2);
    outline: none;
    border-radius: utils.$border-radius-small;
    cursor: pointer;

    &:focus-visible {
      outline: 1px solid var(--primary-color);
    }
  }

  .buckets-list {
    min-width: 500px;

    &__name {
      &--disabled {
        opacity: 0.4;
      }
    }

    &__tag {
      &--disabled {
        opacity: 0.4;
      }
    }
  }

  .tag-list {
    margin-bottom: utils.unit(3);
  }
</style>
