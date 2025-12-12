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
import { useI18n } from 'vue-i18n';
import type { Step } from '@clyso/clyso-ui-kit';
import useVuelidate from '@vuelidate/core';
import { useNotification } from '@clyso/clyso-ui-kit';
import { useRouter } from 'vue-router';
import {
  AddReplicationStepName,
  type ChorusBucketListRequest,
  type ChorusReplicationId,
  type ChorusStorage,
} from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';
import { RouteName } from '@/utils/types/router';
import { ErrorHelper } from '@/utils/helpers/ErrorHelper';

interface ChorusAddReplicationState {
  isLoading: boolean;
  hasError: boolean;
  storages: ChorusStorage[];
  isMainAsSourceAlertShown: boolean;
  selectedFromStorage: ChorusStorage | null;
  selectedToStorage: ChorusStorage | null;
  selectedUser: string | null;
  isForAllBuckets: boolean;
  isBucketsListLoading: boolean;
  hasBucketsError: boolean;
  isReplicatedShown: boolean;
  bucketsRequestOptions: ChorusBucketListRequest | null;
  bucketsList: string[];
  replicatedBucketsList: string[];
  selectedBuckets: string[];
  currentStep: AddReplicationStepName;
  isConfirmDialogOpen: boolean;
  isSubmitting: boolean;
}

function getInitialState(): ChorusAddReplicationState {
  return {
    isLoading: false,
    hasError: false,
    storages: [],
    isMainAsSourceAlertShown: false,
    selectedFromStorage: null,
    selectedToStorage: null,
    selectedUser: null,
    isForAllBuckets: false,
    isBucketsListLoading: false,
    hasBucketsError: false,
    isReplicatedShown: false,
    bucketsRequestOptions: null,
    bucketsList: [],
    replicatedBucketsList: [],
    selectedBuckets: [],
    currentStep: AddReplicationStepName.FROM_STORAGE,
    isConfirmDialogOpen: false,
    isSubmitting: false,
  };
}

export const useChorusAddReplicationStore = defineStore(
  'chorusAddReplication',
  () => {
    const state = reactive<ChorusAddReplicationState>(getInitialState());
    const router = useRouter();

    async function initAddReplicationPage() {
      state.isLoading = true;
      state.hasError = false;

      try {
        const { storages } = await ChorusService.getStorages();

        state.storages = storages;
        prepareForm();
      } catch {
        state.hasError = true;
      } finally {
        state.isLoading = false;
      }
    }

    const { t } = useI18n({
      messages: i18nAddReplication,
    });

    const steps = computed<Step[]>(() => [
      {
        title: t('step1Title'),
        description: t('step1Description'),
      },
      {
        title: t('step2Title'),
        description: t('step2Description'),
      },
      {
        title: t('step3Title'),
        description: t('step3Description'),
      },
      {
        title: t('step4Title'),
        description: t('step4Description'),
      },
    ]);
    const stepsCount = computed(() => steps.value.length);

    function prepareForm() {
      state.selectedFromStorage =
        state.storages.find((storage) => storage.isMain) ?? null;
      state.selectedToStorage =
        state.storages.find((storage) => !storage.isMain) ?? null;
      state.selectedUser =
        (state.selectedFromStorage?.credentials ?? [])[0]?.alias ?? null;
      setTimeout(() => {
        state.isMainAsSourceAlertShown = true;
      }, 500);
    }

    async function getBucketsList() {
      if (
        !state.selectedUser ||
        !state.selectedFromStorage ||
        !state.selectedToStorage
      ) {
        return;
      }

      state.bucketsRequestOptions = {
        user: state.selectedUser,
        fromStorage: state.selectedFromStorage.name,
        toStorage: state.selectedToStorage.name,
        showReplicated: true,
      };

      const { buckets, replicatedBuckets } =
        await ChorusService.getBucketsForReplication(
          state.bucketsRequestOptions,
        );

      state.bucketsList = buckets;
      state.replicatedBucketsList = replicatedBuckets;
    }

    async function initBucketsList() {
      state.isBucketsListLoading = true;
      state.selectedBuckets = [];
      state.bucketsList = [];
      state.replicatedBucketsList = [];

      try {
        await getBucketsList();
        state.hasBucketsError = false;
      } catch {
        state.hasBucketsError = true;
      } finally {
        state.isBucketsListLoading = false;
      }
    }

    const isBucketsAlreadyRequested = computed(
      () =>
        state.bucketsRequestOptions?.user === state.selectedUser &&
        state.bucketsRequestOptions?.fromStorage ===
          state.selectedFromStorage?.name &&
        state.bucketsRequestOptions?.toStorage ===
          state.selectedToStorage?.name,
    );

    const validator = useVuelidate(
      {
        state: {
          selectedBuckets: {
            required: () => {
              if (state.isForAllBuckets) {
                return true;
              }

              return state.selectedBuckets.length > 0;
            },
          },
        },
      },
      {
        state,
      },
    );

    async function createReplication() {
      const {
        selectedToStorage,
        selectedFromStorage,
        selectedUser,
        selectedBuckets,
        isForAllBuckets,
      } = state;

      if (
        !selectedToStorage ||
        !selectedFromStorage ||
        !selectedUser ||
        (!selectedBuckets.length && !isForAllBuckets)
      ) {
        return;
      }

      state.isSubmitting = true;

      const successList: ChorusReplicationId[] = [];
      const errorList: [ChorusReplicationId, string][] = [];

      const baseReplicationId: ChorusReplicationId = {
        user: selectedUser,
        fromStorage: selectedFromStorage.name,
        toStorage: selectedToStorage.name,
      };

      const replicationIds: ChorusReplicationId[] = isForAllBuckets
        ? [{ ...baseReplicationId }]
        : selectedBuckets.map((bucket) => ({
            ...baseReplicationId,
            fromBucket: bucket,
            toBucket: bucket,
          }));

      await Promise.all(
        replicationIds.map(async (replicationId) => {
          try {
            await ChorusService.addReplication({
              id: replicationId,
            });
            successList.push(replicationId);
          } catch (error: unknown) {
            const reason =
              ErrorHelper.getReason(error) ||
              t('createReplicationErrorUnknown');

            errorList.push([replicationId, reason]);
          }
        }),
      );

      router.push({ name: RouteName.CHORUS_REPLICATION });
      state.isSubmitting = false;

      if (errorList.length !== 0) {
        const content = errorList.map(([id, reason]) => {
          return `${id.user}/${id.fromStorage}->${id.toStorage}: ${reason}`;
        });

        showCreateError(content ? content.join(', ') : undefined);
      }

      if (successList.length !== 0) {
        showCreateSuccess();
      }
    }

    const { createNotification } = useNotification();

    function showCreateSuccess() {
      createNotification({
        type: 'success',
        title: `${t('successTitle')}`,
        content: t('createReplicationSuccess'),
        isClosable: true,
      });
    }

    function showCreateError(reason?: string) {
      const content = reason ? reason : t('createReplicationError');

      createNotification({
        type: 'error',
        title: `${t('errorTitle')}`,
        content: content,
        isClosable: true,
      });
    }

    async function $reset() {
      Object.assign(state, getInitialState());
      validator.value.$reset();
    }

    return {
      ...toRefs(state),
      steps,
      stepsCount,
      initBucketsList,
      getBucketsList,
      isBucketsAlreadyRequested,
      validator,
      createReplication,
      $reset,
      initAddReplicationPage,
    };
  },
);
