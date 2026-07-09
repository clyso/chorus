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
import {
  AddDiffReportStepName,
  type ChorusStorage,
} from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';

interface ChorusAddDiffReportState {
  isLoading: boolean;
  hasError: boolean;

  storages: ChorusStorage[];

  currentStep: AddDiffReportStepName;

  isSubmitting: boolean;
}

function getInitialState(): ChorusAddDiffReportState {
  return {
    isLoading: false,
    hasError: false,

    storages: [],

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

    function prepareForm() {}

    function $reset() {
      Object.assign(state, getInitialState());
    }

    return {
      ...toRefs(state),
      hasEnoughStorages,
      initAddDiffReportPage,
      steps,
      stepsCount,
      $reset,
    };
  },
);
