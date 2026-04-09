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

    const isBlockOnly = computed(
      () => state.isBlocked && !state.selectedToStorage,
    );

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
          t('userSelectionRequired'),
          (value: string | null) => !!value,
        ),
      },
      selectedToStorage: {
        required: helpers.withMessage(
          t('storageSelectionRequired'),
          (value: ChorusStorage | null) => state.isBlocked || !!value,
        ),
      },
      bucketName: {
        required: helpers.withMessage(
          t('bucketNameRequired'),
          (value: string | null) => state.isForAllBuckets || isRequired(value),
        ),
        validLength: helpers.withMessage(
          t('bucketErrLength'),
          (value: string | null) =>
            state.isForAllBuckets || isValidLength(value),
        ),
        validChars: helpers.withMessage(
          t('bucketErrChars'),
          (value: string | null) =>
            state.isForAllBuckets || hasValidChars(value),
        ),
        validStartEnd: helpers.withMessage(
          t('bucketErrStartEnd'),
          (value: string | null) =>
            state.isForAllBuckets || hasValidStartEnd(value),
        ),
        noAdjacentPeriods: helpers.withMessage(
          t('bucketErrAdjacentPeriods'),
          (value: string | null) =>
            state.isForAllBuckets || hasNoAdjacentPeriods(value),
        ),
        notIpAddress: helpers.withMessage(
          t('bucketErrIpAddress'),
          (value: string | null) =>
            state.isForAllBuckets || isNotIpAddress(value),
        ),
        validPrefixSuffix: helpers.withMessage(
          t('bucketErrPrefixSuffix'),
          (value: string | null) =>
            state.isForAllBuckets || hasValidPrefixSuffix(value),
        ),
      },
    }));

    const validator = useVuelidate(validationRules, state);

    /*
     * There are three different situations possible when creating a routing
     * policy.
     *
     * 1. Routing policy only: it will be sent to '/routing/add'
     * (addRoutingPolicy).
     * Required properties: storage and user, optional: bucket.
     *
     * 2. Routing policy + blocked route: it will be sent to '/routing/add'
     * (addRoutingPolicy) for creation and to '/routing/block'
     * (blockRoutingPolicy) for blocking it.
     * Creation, required properties: storage and user, optional: bucket.
     * Block, required properties: user, optional: bucket.
     *
     * 3. Block only: it will be sent to '/routing/block' (blockRoutingPolicy).
     * Required properties: user, optional: bucket.
     */
    async function addRoutingPolicy() {
      const {
        selectedToStorage,
        selectedUser,
        bucketName,
        isForAllBuckets,
        isBlocked,
      } = state;

      // User selection required in all three cases
      if (!selectedUser) return;

      // Bucket selection is optional in all three cases
      const bucket = isForAllBuckets ? null : bucketName;

      // Use case 3: Blocked policy
      if (isBlockOnly.value) {
        return addBlock(selectedUser, bucket, false);
      }

      // Use case 1: Routing policy
      // Storage selection is required in case 1 + 2
      if (!selectedToStorage) return;

      const addPolicyRequestData: RoutingPolicyAddRequest = {
        user: selectedUser,
        bucket,
        toStorage: selectedToStorage.name,
      };

      try {
        await ChorusService.addRoutingPolicy(addPolicyRequestData);
      } catch (addError: unknown) {
        const addReason =
          ErrorHelper.getReason(addError) || t('addRoutingPolicyErrorUnknown');

        throw new Error(addReason);
      }

      // Use case 2: Routing policy + block
      if (isBlocked) {
        return addBlock(selectedUser, bucket, true);
      }
    }

    async function addBlock(
      user: string,
      bucket: string | null,
      hasCreationRollback: boolean,
    ) {
      const editPolicyRequestData: RoutingPolicyEditRequest = { user, bucket };

      try {
        await ChorusService.blockRoutingPolicy(editPolicyRequestData);
      } catch (blockError: unknown) {
        if (hasCreationRollback) {
          return handleCreationRollback(editPolicyRequestData);
        }

        const customBlockMsg = hasCreationRollback
          ? t('addBlockRollbackErrorUnknown')
          : t('addBlockErrorUnknown');

        throw new Error(ErrorHelper.getReason(blockError) || customBlockMsg);
      }
    }

    async function handleCreationRollback(
      editPolicyRequestData: RoutingPolicyEditRequest,
    ) {
      try {
        await ChorusService.deleteRoutingPolicy(editPolicyRequestData);
      } catch (deleteError: unknown) {
        throw new Error(
          ErrorHelper.getReason(deleteError) ||
            t('addRoutingPolicyRollbackErrorUnknown'),
        );
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
      isBlockOnly,
      validator,
      $reset,
    };
  },
);
