<!--
  - Copyright © 2026 Clyso GmbH
  -
  -  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
  -  you may not use this file except in compliance with the License.
  -  You may obtain a copy of the License at
  -
  -  https://www.gnu.org/licenses/agpl-3.0.html
  -
  -  Unless required by applicable law or agreed to in writing, software
  -  distributed under the License is distributed on an "AS IS" BASIS,
  -  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  -  See the License for the specific language governing permissions and
  -  limitations under the License.
  -->

<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import { computed } from 'vue';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';
  import { useChorusAddDiffReportStore } from '@/stores/chorusAddDiffReportStore';
  import ChorusWizard from '@/components/chorus/common/ChorusWizard/ChorusWizard.vue';

  const { t } = useI18n({
    messages: i18nAddDiffReport,
  });

  const store = useChorusAddDiffReportStore();
  const { currentStep, stepsCount, isLoading, isSubmitting } =
    storeToRefs(store);

  const isNextDisabled = computed<boolean>(
    () => isLoading.value || currentStep.value === stepsCount.value,
  );

  async function handleStepChange(step: number) {
    // Only check validation if going forward
    if (step > currentStep.value && !store.validateCurrentStep()) {
      return;
    }

    currentStep.value = step;
  }

  function handleSubmit() {}
</script>

<template>
  <ChorusWizard
    :current-step="currentStep"
    :steps-count="stepsCount"
    :is-submitting="isSubmitting"
    :is-next-disabled="isNextDisabled"
    :submit-label="t('actionAddDiffReport')"
    :next-label="t('nextAction')"
    @update:current-step="handleStepChange"
    @submit="handleSubmit"
  />
</template>
