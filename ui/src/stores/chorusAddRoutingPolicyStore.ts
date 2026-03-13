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
import { helpers } from '@vuelidate/validators';
import { useI18n } from 'vue-i18n';
import type {
  ChorusStorage,
  RoutingPolicyAddRequest,
  RoutingPolicyEditRequest,
} from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import {
  hasNoAdjacentPeriods,
  hasValidChars,
  hasValidPrefixSuffix,
  hasValidStartEnd,
  isNotIpAddress,
  isRequired,
  isValidLength,
} from '@/utils/validators/s3BucketNameValidator';
import { ErrorHelper } from '@/utils/helpers/ErrorHelper';
import i18nAddRoutingPolicy from '@/components/chorus/add-routing-policies/i18nAddRoutingPolicy';

interface ChorusAddRoutingPolicyState {
  isLoading: boolean;
  hasError: boolean;
  storages: ChorusStorage[];
  selectedToStorage: ChorusStorage | null;
  selectedUser: string | null;
  bucketName: string | null;
  isForAllBuckets: boolean;
  isBlocked: boolean;
  isConfirmDialogOpen: boolean;
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
    isConfirmDialogOpen: false,
  };
}

export const useChorusAddRoutingPolicyStore = defineStore(
  'chorusAddRoutingPolicy',
  () => {
    const state = reactive<ChorusAddRoutingPolicyState>(getInitialState());

    const { t } = useI18n({
      messages: i18nAddRoutingPolicy,
    });

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

    const validationRules = computed(() => ({
      selectedUser: {
        required: helpers.withMessage(
          'userSelectionRequired',
          (value: string | null) => !!value,
        ),
      },
      bucketName: {
        required: helpers.withMessage(
          'bucketNameRequired',
          (value: string | null) => isRequired(state.isForAllBuckets, value),
        ),
        validLength: helpers.withMessage(
          'bucketErrLength',
          (value: string | null) => isValidLength(state.isForAllBuckets, value),
        ),
        validChars: helpers.withMessage(
          'bucketErrChars',
          (value: string | null) => hasValidChars(state.isForAllBuckets, value),
        ),
        validStartEnd: helpers.withMessage(
          'bucketErrStartEnd',
          (value: string | null) =>
            hasValidStartEnd(state.isForAllBuckets, value),
        ),
        noAdjacentPeriods: helpers.withMessage(
          'bucketErrAdjacentPeriods',
          (value: string | null) =>
            hasNoAdjacentPeriods(state.isForAllBuckets, value),
        ),
        notIpAddress: helpers.withMessage(
          'bucketErrIpAddress',
          (value: string | null) =>
            isNotIpAddress(state.isForAllBuckets, value),
        ),
        validPrefixSuffix: helpers.withMessage(
          'bucketErrPrefixSuffix',
          (value: string | null) =>
            hasValidPrefixSuffix(state.isForAllBuckets, value),
        ),
      },
    }));

    const validator = useVuelidate(validationRules, state);

    async function addRoutingPolicy() {
      const {
        selectedToStorage,
        selectedUser,
        bucketName,
        isForAllBuckets,
        isBlocked,
      } = state;

      if (!selectedUser || (!bucketName && !isForAllBuckets)) {
        return;
      }

      const addPolicyRequestData: RoutingPolicyAddRequest = {
        user: selectedUser,
        bucket: isForAllBuckets ? null : bucketName,
        toStorage: selectedToStorage?.name ?? null,
      };

      try {
        await ChorusService.addRoutingPolicy(addPolicyRequestData);
      } catch (addError: unknown) {
        const addReason =
          ErrorHelper.getReason(addError) || t('addRoutingPolicyErrorUnknown');

        throw new Error(addReason);
      }

      if (isBlocked) {
        const editPolicyRequestData: RoutingPolicyEditRequest = {
          user: selectedUser,
          bucket: isForAllBuckets ? null : bucketName,
        };

        try {
          await ChorusService.blockRoutingPolicy(editPolicyRequestData);
        } catch (blockError: unknown) {
          await ChorusService.deleteRoutingPolicy(editPolicyRequestData);
          const blockReason =
            ErrorHelper.getReason(blockError) ||
            t('addRoutingPolicyBlockErrorUnknown');

          throw new Error(blockReason);
        }
      }
    }

    async function $reset() {
      Object.assign(state, getInitialState());
      validator.value.$reset();
    }

    return {
      ...toRefs(state),
      initAddRoutingPolicyPage,
      addRoutingPolicy,
      users,
      validator,
      $reset,
    };
  },
);
