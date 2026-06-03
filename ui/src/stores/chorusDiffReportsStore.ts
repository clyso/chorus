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
import type { AddId } from '@/utils/types/helper';
import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
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
  };
}

export const useChorusDiffReportsStore = defineStore('chorusDiffReport', () => {
  const state = reactive<ChorusDiffReportsState>(getInitialState());

  const hasNoData = computed<boolean>(() => state.reports.length === 0);

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
    const sortedReports = state.sorter
      ? GeneralHelper.orderBy(
          state.reports,
          [state.sorter.columnKey],
          [state.sorter.order === 'ascend' ? 'asc' : 'desc'],
        )
      : state.reports;

    const start = (state.page - 1) * state.pageSize;
    const end = state.page * state.pageSize;

    return sortedReports.slice(start, end);
  });

  const pagination = computed<DataTablePaginationObject>(() => ({
    page: state.page,
    pageSize: state.pageSize,
    showSizePicker: true,
    pageSizes: [...PAGE_SIZES],
    pageCount: Math.ceil(state.reports.length / state.pageSize),
    itemCount: state.reports.length,
    prefix({ itemCount }) {
      if (state.isLoading || state.hasError) {
        return '';
      }

      return `Total: ${itemCount}`;
    },
  }));

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
    pagination,
    computedReports,
    initDiffReportPage,
    $reset,
  };
});
