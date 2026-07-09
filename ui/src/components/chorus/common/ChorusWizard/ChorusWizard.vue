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
  import { CButton, CIcon } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import { IconName } from '@/utils/types/icon';

  const props = defineProps<{
    currentStep: number;
    stepsCount: number;
    isSubmitting: boolean;
    isNextDisabled: boolean;
    submitLabel: string;
    nextLabel: string;
  }>();

  const emit = defineEmits<{
    'update:currentStep': [step: number];
    submit: [];
  }>();

  const isLastStep = computed<boolean>(
    () => props.currentStep === props.stepsCount,
  );
  const isFirstStep = computed<boolean>(() => props.currentStep === 1);

  function goNext() {
    if (props.isNextDisabled) {
      return;
    }

    emit('update:currentStep', props.currentStep + 1);
  }

  function goBack() {
    if (isFirstStep.value) {
      return;
    }

    emit('update:currentStep', props.currentStep - 1);
  }

  function handleSubmit() {
    emit('submit');
  }
</script>

<template>
  <div class="chorus-wizard-navigation">
    <CButton
      secondary
      :disabled="isFirstStep || isSubmitting"
      size="large"
      class="chorus-wizard-navigation__back"
      :loading="false"
      @click="goBack"
    >
      <template #icon>
        <CIcon
          :is-inline="true"
          :name="IconName.BASE_ARROW_BACK"
        />
      </template>
    </CButton>

    <CButton
      v-if="!isLastStep"
      type="primary"
      ghost
      size="large"
      :disabled="isNextDisabled"
      class="chorus-wizard-navigation__next"
      @click="goNext"
    >
      <template #icon>
        <CIcon
          :is-inline="true"
          :name="IconName.BASE_ARROW_FORWARD"
        />
      </template>
      {{ nextLabel }}
    </CButton>
    <CButton
      v-else
      type="primary"
      :disabled="isSubmitting"
      :loading="isSubmitting"
      size="large"
      class="chorus-wizard-navigation__submit"
      @click="handleSubmit"
    >
      {{ submitLabel }}
    </CButton>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .chorus-wizard-navigation {
    display: flex;
    gap: utils.unit(3);
  }
</style>
