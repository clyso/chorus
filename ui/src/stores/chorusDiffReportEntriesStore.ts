/*
 * Copyright © 2026 Clyso GmbH
 *
 *  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  https://www.gnu.org/licenses/agpl-3.0.html
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import {
  type DataTablePaginationObject,
  type DataTableSortState,
} from '@clyso/clyso-ui-kit';
import { defineStore, storeToRefs } from 'pinia';
import { computed, reactive, toRefs, watch } from 'vue';
import { ChorusService } from '@/services/ChorusService';
import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
import type { DiffReportEntry } from '@/utils/types/chorus';
import { useChorusDiffReportDetailStore } from '@/stores/chorusDiffReportDetailStore';

interface ChorusDiffReportEntriesState {
  isLoading: boolean;
  hasError: boolean;
  entries: DiffReportEntry[];
  sorter: DataTableSortState | null;

  page: number;
  pageSize: number;

  filterObjectName: string;
}

const PAGE_SIZES = [10, 20, 30, 50, 100] as const;

function getInitialState(): ChorusDiffReportEntriesState {
  return {
    isLoading: false,
    hasError: false,
    entries: [],
    sorter: null,

    page: 1,
    pageSize: PAGE_SIZES[0],

    filterObjectName: '',
  };
}

export const useChorusDiffReportEntriesStore = defineStore(
  'chorusDiffReportEntries',
  () => {
    const state = reactive<ChorusDiffReportEntriesState>(getInitialState());

    const { report, locations } = storeToRefs(useChorusDiffReportDetailStore());

    watch(
      () => report.value && report.value.ready,
      (isReady) => {
        if (isReady) getReportEntries();
      },
    );

    const inconsistentObjectsCount = computed<number>(
      () => state.entries.length,
    );

    const filteredReportEntries = computed<DiffReportEntry[]>(() =>
      state.entries.filter((entry) => {
        return (
          !state.filterObjectName.length ||
          entry.object
            .toLowerCase()
            .includes(state.filterObjectName.toLowerCase())
        );
      }),
    );

    const isFiltered = computed<boolean>(
      () => state.filterObjectName.length !== 0,
    );

    const computedReportObjects = computed<DiffReportEntry[]>(() => {
      const sortedReportEntries = state.sorter
        ? GeneralHelper.orderBy(
            filteredReportEntries.value,
            [
              state.sorter.columnKey === 'size'
                ? (entry: DiffReportEntry) => Number(entry.size)
                : state.sorter.columnKey,
            ],
            [state.sorter.order === 'ascend' ? 'asc' : 'desc'],
          )
        : filteredReportEntries.value;

      const start = (state.page - 1) * state.pageSize;
      const end = state.page * state.pageSize;

      return sortedReportEntries.slice(start, end);
    });

    const pagination = computed<DataTablePaginationObject>(() => ({
      page: state.page,
      pageSize: state.pageSize,
      showSizePicker: true,
      pageSizes: [...PAGE_SIZES],
      pageCount: Math.ceil(filteredReportEntries.value.length / state.pageSize),
      itemCount: filteredReportEntries.value.length,
      prefix({ itemCount }) {
        if (state.isLoading || state.hasError) return '';

        if (isFiltered.value) {
          return `Filtered: ${filteredReportEntries.value.length} / Total: ${state.entries.length}`;
        }

        return `Total: ${itemCount}`;
      },
    }));

    async function getReportEntries() {
      state.entries = [];
      state.sorter = null;
      state.filterObjectName = '';

      if (locations.value.length !== 2) {
        return;
      }

      state.isLoading = true;

      try {
        const res = await ChorusService.getDiffReportEntries({
          locations: locations.value,
        });

        state.entries = res.entries;
        state.hasError = false;
      } catch {
        state.hasError = true;
      } finally {
        state.isLoading = false;
      }
    }

    function $reset() {
      Object.assign(state, getInitialState());
    }

    return {
      ...toRefs(state),
      computedReportObjects,
      inconsistentObjectsCount,
      pagination,
      getReportEntries,
      $reset,
    };
  },
);
