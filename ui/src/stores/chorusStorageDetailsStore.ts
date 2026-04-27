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
import { useRouter } from 'vue-router';
import type {
  DataTablePaginationObject,
  DataTableSortState,
} from '@clyso/clyso-ui-kit';
import type { ChorusCredential, ChorusStorage } from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import { RouteName } from '@/utils/types/router';
import { GeneralHelper } from '@/utils/helpers/GeneralHelper';

const CREDENTIALS_PAGE_SIZES = [10, 20, 30, 50, 100] as const;

interface ChorusStorageDetailsState {
  isLoading: boolean;
  hasError: boolean;
  storage: ChorusStorage | null;

  credentialsList: ChorusCredential[];
  credentialsSorter: DataTableSortState | null;
  credentialsPage: number;
  credentialsPageSize: number;
  credentialsFilterAlias: string[];
}

function getInitialState(): ChorusStorageDetailsState {
  return {
    isLoading: false,
    hasError: false,
    storage: null,

    credentialsList: [],
    credentialsSorter: null,
    credentialsPage: 1,
    credentialsPageSize: CREDENTIALS_PAGE_SIZES[0],
    credentialsFilterAlias: [],
  };
}

export const useChorusStorageDetailsStore = defineStore(
  'chorusStorageDetails',
  () => {
    const state = reactive<ChorusStorageDetailsState>(getInitialState());
    const router = useRouter();

    async function initStorageDetails(storageName: string) {
      state.isLoading = true;
      state.hasError = false;

      try {
        const { storages: storagesValue } = await ChorusService.getStorages();

        state.storage =
          storagesValue.find(({ name }) => storageName === name) ?? null;

        if (state.storage === null) {
          router.push({ name: RouteName.CHORUS_STORAGES });
        } else {
          state.credentialsList = state.storage.credentials;
        }
      } catch {
        state.hasError = true;
      } finally {
        state.isLoading = false;
      }
    }

    const filteredCredentials = computed<ChorusCredential[]>(() =>
      state.credentialsList.filter((credential) => {
        return (
          !state.credentialsFilterAlias.length ||
          state.credentialsFilterAlias.includes(credential.alias)
        );
      }),
    );

    const isCredentialsFiltered = computed<boolean>(
      () => state.credentialsFilterAlias.length !== 0,
    );

    const computedCredentials = computed<ChorusCredential[]>(() => {
      const sorted = state.credentialsSorter
        ? GeneralHelper.orderBy(
            filteredCredentials.value,
            [state.credentialsSorter.columnKey],
            [state.credentialsSorter.order === 'ascend' ? 'asc' : 'desc'],
          )
        : filteredCredentials.value;

      const start = (state.credentialsPage - 1) * state.credentialsPageSize;
      const end = state.credentialsPage * state.credentialsPageSize;

      return sorted.slice(start, end);
    });

    const credentialsPagination = computed<DataTablePaginationObject>(() => ({
      page: state.credentialsPage,
      pageSize: state.credentialsPageSize,
      showSizePicker: true,
      pageSizes: [...CREDENTIALS_PAGE_SIZES],
      pageCount: Math.ceil(
        filteredCredentials.value.length / state.credentialsPageSize,
      ),
      itemCount: filteredCredentials.value.length,
      prefix({ itemCount }) {
        if (state.isLoading || state.hasError) {
          return '';
        }

        if (isCredentialsFiltered.value) {
          return `Filtered: ${filteredCredentials.value.length} / Total: ${state.credentialsList.length}`;
        }

        return `Total: ${itemCount}`;
      },
    }));

    async function $reset() {
      Object.assign(state, getInitialState());
    }

    return {
      ...toRefs(state),
      initStorageDetails,
      computedCredentials,
      credentialsPagination,
      $reset,
    };
  },
);
