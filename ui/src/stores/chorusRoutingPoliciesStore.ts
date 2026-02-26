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

import { type DataTablePaginationObject } from '@clyso/clyso-ui-kit';
import type { DataTableSortState } from 'naive-ui';
import { defineStore } from 'pinia';
import { computed, reactive, toRefs } from 'vue';
import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
import {
  RoutingPolicyStatusFilter,
  RoutingPolicyTypes,
  type RoutingPolicyListRequest,
  type RoutingPolicy,
} from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';

interface ChorusRoutingPoliciesState {
  isLoading: boolean;
  hasError: boolean;
  routingPolicies: RoutingPolicy[];
  sorter: DataTableSortState | null;
  page: number;
  pageSize: number;
  pollingRequest: Promise<unknown> | null;
  pollingTimeout: number | null;

  selectedRoutingPolicyIds: string[];

  routingPoliciesRequestOptions: RoutingPolicyListRequest | null;

  filterUsers: string[];
  filterBucket: string;
  filterStorages: string[];
  filterStatus: RoutingPolicyStatusFilter | null;
}

const PAGE_SIZES = [10, 20, 30, 50, 100] as const;

function getInitialState(): ChorusRoutingPoliciesState {
  return {
    isLoading: false,
    hasError: false,
    routingPolicies: [],
    sorter: null,
    page: 1,
    pageSize: PAGE_SIZES[0],
    pollingRequest: null,
    pollingTimeout: null,
    selectedRoutingPolicyIds: [],

    routingPoliciesRequestOptions: null,

    filterUsers: [],
    filterBucket: '',
    filterStorages: [],
    filterStatus: null,
  };
}

export const useChorusRoutingPoliciesStore = defineStore(
  'chorusRoutingPolicies',
  () => {
    const state = reactive<ChorusRoutingPoliciesState>(getInitialState());

    const filteredRoutingPolicies = computed<RoutingPolicy[]>(() =>
      state.routingPolicies.filter((routingPolicy) => {
        const isUserMatched =
          !state.filterUsers.length ||
          state.filterUsers.includes(routingPolicy.user);
        const isStorageMatched =
          !state.filterStorages.length ||
          state.filterStorages.includes(routingPolicy.toStorage);
        const isStatusMatched =
          state.filterStatus === null ||
          (state.filterStatus === RoutingPolicyStatusFilter.BLOCKED) ===
            routingPolicy.isBlocked;
        const isBucketMatched =
          !state.filterBucket ||
          !routingPolicy.bucket ||
          routingPolicy.bucket
            .toLocaleLowerCase()
            .trim()
            .includes(state.filterBucket?.toLowerCase().trim());

        return (
          isUserMatched &&
          isStorageMatched &&
          isStatusMatched &&
          isBucketMatched
        );
      }),
    );

    const computedRoutingPolicies = computed<RoutingPolicy[]>(() => {
      const pageRoutingPolicies = state.sorter
        ? GeneralHelper.orderBy(
            filteredRoutingPolicies.value,
            [state.sorter.columnKey, 'id'],
            [state.sorter.order === 'ascend' ? 'asc' : 'desc'],
          )
        : GeneralHelper.orderBy(filteredRoutingPolicies.value, ['id'], ['asc']);

      const start = (state.page - 1) * state.pageSize;
      const end = state.page * state.pageSize;

      return pageRoutingPolicies.slice(start, end);
    });

    const hasNoData = computed<boolean>(
      () => state.routingPolicies.length === 0,
    );

    const isFiltered = computed<boolean>(
      () =>
        state.filterUsers.length !== 0 ||
        state.filterBucket !== '' ||
        state.filterStorages.length !== 0 ||
        state.filterStatus !== null,
    );

    function clearFilters() {
      state.filterUsers = [];
      state.filterBucket = '';
      state.filterStorages = [];
      state.filterStatus = null;
    }

    async function getRoutingPolicies() {
      state.routingPoliciesRequestOptions = {
        hideUserRoutings: false,
        hideBucketRoutings: false,
      };

      const res = await ChorusService.getRoutingPolicies(
        state.routingPoliciesRequestOptions,
      );

      const routingPolicies: RoutingPolicy[] = [
        ...(res.userRoutings ?? []).map((userPolicy) => ({
          ...userPolicy,
          bucket: '*',
          type: RoutingPolicyTypes.USER,
          id: `${userPolicy.toStorage}::${userPolicy.user}`,
        })),
        ...(res.bucketRoutings ?? []).map((bucketPolicy) => ({
          ...bucketPolicy,
          type: RoutingPolicyTypes.BUCKET,
          id: `${bucketPolicy.toStorage}::${bucketPolicy.user}::${bucketPolicy.bucket}`,
        })),
      ];

      state.routingPolicies = routingPolicies;
    }

    async function startRoutingPoliciesPolling() {
      try {
        await stopRoutingPoliciesPolling();

        state.pollingRequest = getRoutingPolicies();

        await state.pollingRequest;
      } finally {
        state.pollingRequest = null;
        state.pollingTimeout = window.setTimeout(
          startRoutingPoliciesPolling,
          5000,
        );
      }
    }

    async function stopRoutingPoliciesPolling() {
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

    async function initRoutingPoliciesPage() {
      state.isLoading = true;

      try {
        await startRoutingPoliciesPolling();

        state.hasError = false;
      } catch {
        state.hasError = true;
        await stopRoutingPoliciesPolling();
      } finally {
        state.isLoading = false;
      }
    }

    const pagination = computed<DataTablePaginationObject>(() => ({
      page: state.page,
      pageSize: state.pageSize,
      showSizePicker: true,
      pageSizes: [...PAGE_SIZES],
      pageCount: Math.ceil(
        filteredRoutingPolicies.value.length / state.pageSize,
      ),
      itemCount: filteredRoutingPolicies.value.length,
      prefix({ itemCount }) {
        if (state.isLoading || state.hasError) {
          return '';
        }

        if (isFiltered.value) {
          return `Filtered: ${filteredRoutingPolicies.value.length} / Total: ${state.routingPolicies.length}`;
        }

        return `Total: ${itemCount}`;
      },
    }));

    const selectedRoutingPoliciesCount = computed(
      () => state.selectedRoutingPolicyIds.length,
    );

    const isAnyRoutingPolicySelected = computed(
      () => state.selectedRoutingPolicyIds.length !== 0,
    );

    const selectedRoutingPolicies = computed<RoutingPolicy[]>(() =>
      state.routingPolicies.filter((routingPolicy) =>
        state.selectedRoutingPolicyIds.includes(routingPolicy.id),
      ),
    );

    async function $reset() {
      try {
        await stopRoutingPoliciesPolling();
      } finally {
        Object.assign(state, getInitialState());
      }
    }

    return {
      ...toRefs(state),
      hasNoData,
      pagination,
      computedRoutingPolicies,
      initRoutingPoliciesPage,
      selectedRoutingPoliciesCount,
      isAnyRoutingPolicySelected,
      selectedRoutingPolicies,
      isFiltered,
      clearFilters,
      $reset,
    };
  },
);
