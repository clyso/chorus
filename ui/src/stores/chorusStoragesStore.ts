/*
 * Copyright © 2025 Clyso GmbH
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
import type { ChorusStorage } from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';

interface ChorusStoragesState {
  isLoading: boolean;
  hasError: boolean;
  storages: ChorusStorage[];
}

function getInitialState(): ChorusStoragesState {
  return {
    isLoading: false,
    hasError: false,
    storages: [],
  };
}

export const useChorusStoragesStore = defineStore('chorusStorages', () => {
  const state = reactive<ChorusStoragesState>(getInitialState());

  const mainStorage = computed<ChorusStorage | undefined>(() =>
    state.storages.find((storage) => storage.isMain),
  );
  const followerStorages = computed<ChorusStorage[]>(() =>
    state.storages.filter((storage) => !storage.isMain),
  );

  async function initStorages() {
    state.isLoading = true;
    state.hasError = false;

    try {
      const { storages: storagesValue } = await ChorusService.getStorages();

      state.storages = storagesValue;
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
    mainStorage,
    followerStorages,
    initStorages,
    $reset,
  };
});
