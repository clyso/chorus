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
import { computed, reactive, toRefs } from 'vue';
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

  currentStep: AddDiffReportStepName;

  isSubmitting: boolean;
}

function getInitialState(): ChorusAddDiffReportState {
  return {
    isLoading: false,
    hasError: false,

    storages: [],
    fromStorage: null,
    fromBucketName: '',

    currentStep: AddDiffReportStepName.FROM_STORAGE_BUCKET,

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
    }));

    const validator = useVuelidate(validationRules, state);

    const stepValidationFields: Record<AddDiffReportStepName, string[]> = {
      [AddDiffReportStepName.FROM_STORAGE_BUCKET]: [
        'fromStorage',
        'fromBucketName',
      ],
      [AddDiffReportStepName.TO_STORAGE_BUCKET]: [],
      [AddDiffReportStepName.USER]: [],
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

    function prepareForm() {
      const mainStorage =
        state.storages.find((storage) => storage.isMain) ??
        state.storages[0] ??
        null;

      state.fromStorage = mainStorage;
    }

    function $reset() {
      Object.assign(state, getInitialState());
      validator.value.$reset();
    }

    return {
      ...toRefs(state),
      hasEnoughStorages,
      initAddDiffReportPage,
      validateCurrentStep,
      validator,
      steps,
      stepsCount,
      $reset,
    };
  },
);
