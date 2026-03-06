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

import useVuelidate from '@vuelidate/core';
import { defineStore } from 'pinia';
import { computed, reactive, toRefs } from 'vue';
import type { ChorusStorage } from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';

interface ChorusAddRoutingPolicyState {
  isLoading: boolean;
  hasError: boolean;
  storages: ChorusStorage[];
  selectedToStorage: ChorusStorage | null;
  selectedUser: string | null;
  bucketName: string | null;
  isForAllBuckets: boolean;
  isBlocked: boolean;
}

function getInitialState(): ChorusAddRoutingPolicyState {
  return {
    isLoading: false,
    hasError: false,
    storages: [],
    selectedToStorage: null,
    selectedUser: null,
    bucketName: null,
    isForAllBuckets: false,
    isBlocked: false,
  };
}

export const useChorusAddRoutingPolicyStore = defineStore(
  'chorusAddRoutingPolicy',
  () => {
    const state = reactive<ChorusAddRoutingPolicyState>(getInitialState());

    const users = computed<string[]>(() => {
      const usersSet = new Set<string>();

      state.storages.forEach((storage) => {
        storage.credentials.forEach((credential) => {
          usersSet.add(credential.alias);
        });
      });

      return Array.from(usersSet).sort();
    });

    async function initAddRoutingPolicyPage() {
      state.isLoading = true;
      state.hasError = false;

      try {
        const res = await ChorusService.getStorages();

        state.storages = res.storages;
      } catch {
        state.hasError = true;
      } finally {
        state.isLoading = false;
      }
    }

    const validator = useVuelidate(
      {
        state: {
          selectedBucket: {
            required: () => {
              if (state.isForAllBuckets) {
                return true;
              }

              return state.bucketName !== null;
            },
          },
        },
      },
      {
        state,
      },
    );

    async function $reset() {
      Object.assign(state, getInitialState());
      validator.value.$reset();
    }

    return {
      ...toRefs(state),
      initAddRoutingPolicyPage,
      users,
      $reset,
    };
  },
);
