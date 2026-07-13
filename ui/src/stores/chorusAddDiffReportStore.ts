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

import type { Step } from '@clyso/clyso-ui-kit';
import { defineStore } from 'pinia';
import { computed, reactive, toRefs, watch } from 'vue';
import { useI18n } from 'vue-i18n';
import { helpers } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import {
  AddDiffReportStepName,
  type ChorusStorage,
} from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';
import {
  hasNoAdjacentPeriods,
  hasValidChars,
  hasValidPrefixSuffix,
  hasValidStartEnd,
  isNotIpAddress,
  isRequired,
  isValidLength,
} from '@/utils/validators/s3BucketNameValidator';

interface ChorusAddDiffReportState {
  isLoading: boolean;
  hasError: boolean;

  storages: ChorusStorage[];
  fromStorage: ChorusStorage | null;
  fromBucketName: string;
  toStorage: ChorusStorage | null;
  toBucketName: string;

  selectedUser: string | null;

  checkOnlyLastVersions: boolean;
  ignoreEtags: boolean;
  ignoreSizes: boolean;

  currentStep: AddDiffReportStepName;

  isConfirmDialogOpen: boolean;
  isSubmitting: boolean;
}

function getInitialState(): ChorusAddDiffReportState {
  return {
    isLoading: false,
    hasError: false,

    storages: [],
    fromStorage: null,
    fromBucketName: '',
    toStorage: null,
    toBucketName: '',

    selectedUser: null,

    checkOnlyLastVersions: true,
    ignoreEtags: false,
    ignoreSizes: false,

    currentStep: AddDiffReportStepName.FROM_STORAGE_BUCKET,

    isConfirmDialogOpen: false,
    isSubmitting: false,
  };
}

export const useChorusAddDiffReportStore = defineStore(
  'chorusAddDiffReport',
  () => {
    const state = reactive<ChorusAddDiffReportState>(getInitialState());
    const { t } = useI18n({
      messages: i18nAddDiffReport,
    });
    const hasEnoughStorages = computed(() => state.storages.length >= 2);

    // Filter for users available on both storages
    const users = computed<string[]>(() => {
      if (!state.fromStorage || !state.toStorage) return [];

      const fromStorageAliases = new Set(
        state.fromStorage.credentials.map((credential) => credential.alias),
      );

      return state.toStorage.credentials
        .map((credential) => credential.alias)
        .filter((alias) => fromStorageAliases.has(alias))
        .sort();
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

    function bucketNameValidationRules() {
      return {
        required: helpers.withMessage(t('bucketRequired'), isRequired),
        validLength: helpers.withMessage(t('bucketErrLength'), isValidLength),
        validChars: helpers.withMessage(t('bucketErrChars'), hasValidChars),
        validStartEnd: helpers.withMessage(
          t('bucketErrStartEnd'),
          hasValidStartEnd,
        ),
        noAdjacentPeriods: helpers.withMessage(
          t('bucketErrAdjacentPeriods'),
          hasNoAdjacentPeriods,
        ),
        notIpAddress: helpers.withMessage(
          t('bucketErrIpAddress'),
          isNotIpAddress,
        ),
        validPrefixSuffix: helpers.withMessage(
          t('bucketErrPrefixSuffix'),
          hasValidPrefixSuffix,
        ),
      };
    }

    const validationRules = computed(() => ({
      fromStorage: {
        required: helpers.withMessage(
          t('fromStorageRequired'),
          (value: ChorusStorage | null) => !!value,
        ),
      },
      fromBucketName: bucketNameValidationRules(),
      toStorage: {
        required: helpers.withMessage(
          t('toStorageRequired'),
          (value: ChorusStorage | null) => !!value,
        ),
      },
      toBucketName: bucketNameValidationRules(),
      selectedUser: {
        required: helpers.withMessage(
          t('userRequired'),
          (value: string | null) => !!value,
        ),
      },
    }));

    const validator = useVuelidate(validationRules, state);

    const stepValidationFields: Record<AddDiffReportStepName, string[]> = {
      [AddDiffReportStepName.FROM_STORAGE_BUCKET]: [
        'fromStorage',
        'fromBucketName',
      ],
      [AddDiffReportStepName.TO_STORAGE_BUCKET]: ['toStorage', 'toBucketName'],
      [AddDiffReportStepName.USER]: ['selectedUser'],
      [AddDiffReportStepName.SETTINGS]: [],
    };

    function validateCurrentStep(): boolean {
      const fields = stepValidationFields[state.currentStep];

      fields.forEach((field) => validator.value[field].$touch());

      return fields.every((field) => !validator.value[field].$error);
    }

    async function initAddDiffReportPage() {
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

    function findStorageToCompare(
      excludedStorage: ChorusStorage,
    ): ChorusStorage | null {
      return (
        state.storages.find(
          (storage) => storage.name !== excludedStorage.name,
        ) ?? null
      );
    }

    // Update toStorage in case of fromStorage updates
    watch(
      () => state.fromStorage,
      (newFromStorage) => {
        if (
          newFromStorage &&
          state.toStorage &&
          state.toStorage.name === newFromStorage.name
        ) {
          state.toStorage = findStorageToCompare(newFromStorage);
        }
      },
    );

    // Update selectedUser if fromStorage and/or toStorage have been updated
    watch(users, (newUsers) => {
      if (state.selectedUser && newUsers.includes(state.selectedUser)) return;

      state.selectedUser = newUsers[0] ?? null;
    });

    function prepareForm() {
      const mainStorage =
        state.storages.find((storage) => storage.isMain) ??
        state.storages[0] ??
        null;

      state.fromStorage = mainStorage;
      state.toStorage = mainStorage ? findStorageToCompare(mainStorage) : null;
      state.selectedUser = users.value[0] ?? null;
    }

    async function createDiffReport() {
      const { fromStorage, toStorage, selectedUser } = state;

      if (!fromStorage || !toStorage || !selectedUser) return;

      state.isSubmitting = true;

      try {
        await ChorusService.addDiffReport({
          locations: [
            { storage: fromStorage.name, bucket: state.fromBucketName },
            { storage: toStorage.name, bucket: state.toBucketName },
          ],
          user: selectedUser,
          checkOnlyLastVersions: state.checkOnlyLastVersions,
          ignoreEtags: state.ignoreEtags,
          ignoreSizes: state.ignoreSizes,
        });
      } finally {
        state.isSubmitting = false;
      }
    }

    function $reset() {
      Object.assign(state, getInitialState());
      validator.value.$reset();
    }

    return {
      ...toRefs(state),
      hasEnoughStorages,
      users,
      initAddDiffReportPage,
      validateCurrentStep,
      validator,
      steps,
      stepsCount,
      createDiffReport,
      $reset,
    };
  },
);
