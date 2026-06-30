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

import { defineStore } from 'pinia';
import { computed, reactive, toRefs } from 'vue';
import type {
  DataTablePaginationObject,
  DataTableSortState,
} from '@clyso/clyso-ui-kit';
import type { DiffReport } from '@/utils/types/chorus';
import { DiffReportStatusFilter } from '@/utils/types/chorus';
import type { AddId } from '@/utils/types/helper';
import { ChorusService } from '@/services/ChorusService';
import { DiffReportsHelper } from '@/utils/helpers/DiffReportsHelper';

interface ChorusDiffReportsState {
  isLoading: boolean;
  hasError: boolean;
  reports: AddId<DiffReport>[];
  sorter: DataTableSortState | null;
  page: number;
  pageSize: number;
  pollingRequest: Promise<unknown> | null;
  pollingTimeout: number | null;

  selectedReportIds: string[];
  isDeleteSelectedProcessing: boolean;
  isRestartSelectedProcessing: boolean;

  filterDirections: string[];
  filterBuckets: string[];
  filterStatuses: DiffReportStatusFilter[];
}

const PAGE_SIZES = [10, 20, 30, 50, 100] as const;
const POLL_INTERVAL_MS = 5000;

function getInitialState(): ChorusDiffReportsState {
  return {
    isLoading: false,
    hasError: false,
    reports: [],
    sorter: null,
    page: 1,
    pageSize: PAGE_SIZES[0],
    pollingRequest: null,
    pollingTimeout: null,

    selectedReportIds: [],
    isDeleteSelectedProcessing: false,
    isRestartSelectedProcessing: false,

    filterDirections: [],
    filterBuckets: [],
    filterStatuses: [],
  };
}

export const useChorusDiffReportsStore = defineStore('chorusDiffReport', () => {
  const state = reactive<ChorusDiffReportsState>(getInitialState());

  const hasNoData = computed<boolean>(() => state.reports.length === 0);

  const filteredReports = computed<AddId<DiffReport>[]>(() =>
    state.reports.filter((report) => {
      const isDirectionMatched =
        !state.filterDirections.length ||
        state.filterDirections.includes(
          DiffReportsHelper.getDirectionPair(report),
        );
      const isBucketMatched =
        !state.filterBuckets.length ||
        state.filterBuckets.includes(DiffReportsHelper.getBucketPair(report));
      const isStatusFilterMatched =
        !state.filterStatuses.length ||
        state.filterStatuses.some((status) =>
          DiffReportsHelper.isDiffReportStatusMatched(report, status),
        );

      return isDirectionMatched && isBucketMatched && isStatusFilterMatched;
    }),
  );

  const isFiltered = computed<boolean>(
    () =>
      state.filterDirections.length !== 0 ||
      state.filterBuckets.length !== 0 ||
      state.filterStatuses.length !== 0,
  );

  function clearFilters() {
    state.filterDirections = [];
    state.filterBuckets = [];
    state.filterStatuses = [];
  }

  function getSortedReportsByStatus(sorter: DataTableSortState) {
    const direction = sorter.order === 'ascend' ? 1 : -1;

    return [...filteredReports.value].sort(
      (diffReportA, diffReportB) =>
        (DiffReportsHelper.getStatusSortOrder(diffReportA) -
          DiffReportsHelper.getStatusSortOrder(diffReportB)) *
        direction,
    );
  }

  const computedReports = computed<AddId<DiffReport>[]>(() => {
    const sortedReports =
      state.sorter && state.sorter.columnKey === 'status'
        ? getSortedReportsByStatus(state.sorter)
        : filteredReports.value;

    const start = (state.page - 1) * state.pageSize;
    const end = state.page * state.pageSize;

    return sortedReports.slice(start, end);
  });

  const pagination = computed<DataTablePaginationObject>(() => ({
    page: state.page,
    pageSize: state.pageSize,
    showSizePicker: true,
    pageSizes: [...PAGE_SIZES],
    pageCount: Math.ceil(filteredReports.value.length / state.pageSize),
    itemCount: filteredReports.value.length,
    prefix({ itemCount }) {
      if (state.isLoading || state.hasError) {
        return '';
      }

      if (isFiltered.value) {
        return `Filtered: ${filteredReports.value.length} / Total: ${state.reports.length}`;
      }

      return `Total: ${itemCount}`;
    },
  }));

  const selectedReportsCount = computed(() => state.selectedReportIds.length);

  const isAnyReportsSelected = computed(
    () => state.selectedReportIds.length !== 0,
  );

  const selectedReports = computed<AddId<DiffReport>[]>(() =>
    state.reports.filter((report) =>
      state.selectedReportIds.includes(report.idStr as string),
    ),
  );

  async function deleteDiffReports(reports: AddId<DiffReport>[]) {
    state.isDeleteSelectedProcessing = true;

    await stopPolling();

    const successList: AddId<DiffReport>[] = [];
    const errorList: AddId<DiffReport>[] = [];

    await Promise.all(
      reports.map(async (report) => {
        try {
          await ChorusService.deleteDiffReport({
            locations: report.locations,
          });
          successList.push(report);
        } catch {
          errorList.push(report);
        }
      }),
    );

    if (successList.length > 0) {
      state.reports = state.reports.filter(
        (report) => !successList.some((item) => item.idStr === report.idStr),
      );

      state.selectedReportIds = state.selectedReportIds.filter(
        (id) => !successList.some((item) => item.idStr === id),
      );

      const pageCount = pagination.value.pageCount ?? 1;

      if (state.page > pageCount) {
        state.page = pageCount;
      }
    }

    startPolling();
    state.isDeleteSelectedProcessing = false;

    return { successList, errorList };
  }

  async function restartDiffReports(reports: AddId<DiffReport>[]) {
    state.isRestartSelectedProcessing = true;

    await stopPolling();

    const successList: AddId<DiffReport>[] = [];
    const errorList: AddId<DiffReport>[] = [];

    await Promise.all(
      reports.map(async (report) => {
        try {
          await ChorusService.restartDiffReport({
            locations: report.locations,
          });
          successList.push(report);
        } catch {
          errorList.push(report);
        }
      }),
    );

    startPolling();
    state.isRestartSelectedProcessing = false;

    return { successList, errorList };
  }

  async function getDiffReports() {
    const res = await ChorusService.getDiffReports();

    state.reports = (res.checks ?? []).map((report) => ({
      ...report,
      idStr: DiffReportsHelper.getDiffReportId(report.locations),
    }));
  }

  async function startPolling() {
    try {
      await stopPolling();

      state.pollingRequest = getDiffReports();

      await state.pollingRequest;
    } finally {
      state.pollingRequest = null;
      state.pollingTimeout = window.setTimeout(startPolling, POLL_INTERVAL_MS);
    }
  }

  async function stopPolling() {
    let error: Error | null = null;

    if (state.pollingRequest) {
      try {
        await state.pollingRequest;
      } catch (e) {
        error = e as Error;
      } finally {
        state.pollingRequest = null;
      }
    }

    if (!state.pollingTimeout) {
      return;
    }

    clearTimeout(state.pollingTimeout);
    state.pollingTimeout = null;

    if (error) {
      throw error;
    }
  }

  async function initDiffReportPage() {
    state.isLoading = true;

    try {
      await startPolling();

      state.hasError = false;
    } catch {
      state.hasError = true;
      await stopPolling();
    } finally {
      state.isLoading = false;
    }
  }

  async function $reset() {
    try {
      await stopPolling();
    } finally {
      Object.assign(state, getInitialState());
    }
  }

  return {
    ...toRefs(state),
    hasNoData,
    isFiltered,
    pagination,
    computedReports,
    selectedReportsCount,
    isAnyReportsSelected,
    selectedReports,
    clearFilters,
    deleteDiffReports,
    restartDiffReports,
    initDiffReportPage,
    $reset,
  };
});
