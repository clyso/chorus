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
import { StorageProvider, type ChorusStorage } from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import { RouteName } from '@/utils/types/router';

interface ChorusSetCredentialState {
  isLoading: boolean;
  isSubmitting: boolean;
  hasError: boolean;
  storage: ChorusStorage | null;
  isEditMode: boolean;

  user: string;

  accessKey: string;
  secretKey: string;

  username: string;
  password: string;
  domainName: string;
  tenantName: string;
}

function getInitialState(): ChorusSetCredentialState {
  return {
    isLoading: false,
    isSubmitting: false,
    hasError: false,
    storage: null,
    isEditMode: false,

    user: '',

    accessKey: '',
    secretKey: '',

    username: '',
    password: '',
    domainName: '',
    tenantName: '',
  };
}

export const useChorusSetCredentialStore = defineStore(
  'chorusSetCredential',
  () => {
    const state = reactive<ChorusSetCredentialState>(getInitialState());
    const router = useRouter();

    const isS3 = computed(() => state.storage?.provider === StorageProvider.S3);

    const isSwift = computed(
      () => state.storage?.provider === StorageProvider.SWIFT,
    );

    async function initSetCredentialPage(storageName: string, alias?: string) {
      state.isLoading = true;

      try {
        const { storages } = await ChorusService.getStorages();

        state.storage =
          storages.find(({ name }) => storageName === name) ?? null;

        if (!state.storage) {
          router.push({ name: RouteName.CHORUS_STORAGES });

          return;
        }

        if (alias) {
          state.isEditMode = true;
          state.user = alias;
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
      isS3,
      isSwift,
      initSetCredentialPage,
      $reset,
    };
  },
);
