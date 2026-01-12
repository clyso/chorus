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
import { reactive, toRefs } from 'vue';
import { useRouter } from 'vue-router';
import type { ChorusStorage } from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import { RouteName } from '@/utils/types/router';

interface ChorusStorageDetailsState {
  isLoading: boolean;
  hasError: boolean;
  storage: ChorusStorage | null;
}

function getInitialState(): ChorusStorageDetailsState {
  return {
    isLoading: false,
    hasError: false,
    storage: null,
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
        }
      } catch {
        state.hasError = true;
      } finally {
        state.isLoading = false;
      }
    }

    async function $reset() {
      Object.assign(state, getInitialState());
    }

    return {
      ...toRefs(state),
      initStorageDetails,
      $reset,
    };
  },
);
